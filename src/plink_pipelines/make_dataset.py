import argparse
import logging
import os
import warnings
from collections.abc import Generator, Sequence
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from pathlib import Path
from shutil import copyfile, rmtree
from typing import Literal

import luigi
import numba
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from aislib.misc_utils import ensure_path_exists, get_logger
from geno_reader import GenoReader  # type: ignore[unresolved-import]
from luigi.task import flatten
from luigi.util import inherits, requires
from numba import prange

from plink_pipelines.validation_functions import validate_cl_args

logger = get_logger(name=__name__)

luigi_logger = logging.getLogger("luigi")
luigi_logger.setLevel(logging.INFO)


class RenameOnFailureMixin:
    def on_failure(self, exception):
        targets = luigi.task.flatten(self.output())
        for target in targets:
            if target.exists() and isinstance(target, luigi.LocalTarget):
                target_path = Path(target.path)
                new_fname = target_path.stem + "_FAILED" + target_path.suffix
                target_path.rename(target_path.parent / new_fname)
        return luigi.Task.on_failure(self, exception)


class Config(luigi.Task, RenameOnFailureMixin):
    output_folder = luigi.Parameter()

    @property
    def input_name(self):
        bed_files = [
            i for i in Path(str(self.raw_data_path)).iterdir() if i.suffix == ".bed"
        ]
        if len(bed_files) != 1:
            raise ValueError(
                f"Expected one .bed file in {self.raw_data_path}, butfound {bed_files}."
            )
        return str(bed_files[0])

    @property
    def file_name(self):
        raise NotImplementedError

    @property
    def _sample_fname_part(self):
        return "full_inds/full_chrs"

    def output_target(self, fname):
        fname = Path(fname)
        fname_injected = fname.parent / self._sample_fname_part / fname.name

        output_path = Path(str(self.output_folder), fname_injected)

        return luigi.LocalTarget(str(output_path))

    def output(self):
        return self.output_target(self.file_name)


class ExternalRawData(luigi.ExternalTask):
    raw_data_path = luigi.Parameter()

    @property
    def input_name(self):
        bed_files = [
            i for i in Path(str(self.raw_data_path)).iterdir() if i.suffix == ".bed"
        ]
        if len(bed_files) != 1:
            raise ValueError(
                f"Expected one .bed file in {self.raw_data_path}, "
                f"but found {bed_files}."
            )
        return str(bed_files[0])

    def output(self):
        return luigi.LocalTarget(str(self.input_name))


@inherits(ExternalRawData)
class OneHotSNPs(Config):
    """
    Generates one hot encodings from a individuals x SNPs file.
    """

    output_folder = luigi.Parameter()
    output_format = luigi.Parameter()
    output_name = luigi.Parameter()
    read_chunk_size = luigi.IntParameter()
    process_chunk_size = luigi.IntParameter()
    file_name = "processed/encoded_outputs"

    def requires(self):
        return [self.clone(ExternalRawData)]

    def run(self):
        input_path = Path(self.input()[0].path)
        assert input_path.suffix == ".bed"

        output_path = Path(self.output().path)
        ensure_path_exists(output_path, is_folder=True)

        read_generator = get_sample_generator_from_bed(
            bed_path=input_path, read_chunk_size=int(self.read_chunk_size)
        )
        process_generator = rechunk_generator(
            chunk_generator=read_generator, new_chunk_size=int(self.process_chunk_size)
        )
        sample_id_one_hot_array_generator = _get_one_hot_encoded_generator(
            chunked_sample_generator=process_generator,
            output_format=self.output_format,
        )

        write_one_hot_outputs(
            id_array_generator=sample_id_one_hot_array_generator,
            output_folder=output_path,
            output_format=self.output_format,
            output_name=str(self.output_name),
            batch_size=int(self.process_chunk_size),
        )


def write_one_hot_outputs(
    id_array_generator: Generator[tuple[str, np.ndarray], None, None],
    output_folder: Path,
    output_format: Literal["disk", "parquet"],
    batch_size: int,
    output_name: str | None = None,
) -> None:
    if output_format == "disk":
        _write_one_hot_arrays_to_disk(
            id_array_generator=id_array_generator,
            output_folder=output_folder,
        )
    elif output_format == "parquet":
        assert output_name is not None
        write_one_hot_arrays_to_parquet(
            id_array_generator=id_array_generator,
            output_folder=output_folder,
            output_name=output_name,
            batch_size=batch_size,
        )
    else:
        raise ValueError(f"Unknown output format {output_format}")


def _save_array(output_folder: Path, id_array: tuple[str, np.ndarray]) -> Path:
    id_, array = id_array
    output_path = output_folder / f"{id_}.npy"
    np.save(str(output_path), array)
    return output_path


def _write_one_hot_arrays_to_disk(
    id_array_generator: Generator[tuple[str, np.ndarray], None, None],
    output_folder: Path,
    batch_size: int = 1000,
    max_workers: int = 16,
) -> None:
    cpu_count = os.cpu_count() or 1
    max_workers = min(cpu_count * 2, max_workers)

    save_fn = partial(_save_array, output_folder)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        batch = []
        for item in id_array_generator:
            batch.append(item)

            if len(batch) >= batch_size:
                futures = list(executor.map(save_fn, batch))
                _ = list(futures)
                batch = []

        if batch:
            futures = list(executor.map(save_fn, batch))
            _ = list(futures)


def write_one_hot_arrays_to_parquet(
    id_array_generator: Generator[tuple[str, np.ndarray], None, None],
    output_folder: Path,
    output_name: str,
    batch_size: int,
) -> int:
    output_folder.mkdir(parents=True, exist_ok=True)
    parquet_path = output_folder / f"{output_name}.parquet"

    try:
        first_id, first_array = next(id_array_generator)
    except StopIteration as e:
        raise ValueError("Generator is empty") from e

    schema = pa.schema(
        [
            pa.field("sample_id", pa.string()),
            pa.field("genotype_data", pa.list_(pa.int8())),
            pa.field("shape", pa.list_(pa.int32())),
        ]
    )

    batch_data = {
        "sample_id": [first_id],
        "genotype_data": [first_array.flatten().tolist()],
        "shape": [list(first_array.shape)],
    }

    sample_count = 1
    writer = None

    try:
        for id_, array in id_array_generator:
            if array.shape != first_array.shape:
                raise ValueError(
                    f"Array shape mismatch at ID {id_}. Expected "
                    f"{first_array.shape}, got {array.shape}"
                )

            batch_data["sample_id"].append(id_)
            batch_data["genotype_data"].append(array.flatten().tolist())
            batch_data["shape"].append(list(array.shape))
            sample_count += 1

            if len(batch_data["sample_id"]) >= batch_size:
                writer = _write_parquet_batch_streaming(
                    batch_data=batch_data,
                    schema=schema,
                    parquet_path=parquet_path,
                    writer=writer,
                )
                batch_data = {"sample_id": [], "genotype_data": [], "shape": []}

        if batch_data["sample_id"]:
            writer = _write_parquet_batch_streaming(
                batch_data=batch_data,
                schema=schema,
                parquet_path=parquet_path,
                writer=writer,
            )

    finally:
        if writer is not None:
            writer.close()

    logger.info(f"Successfully wrote {sample_count} samples to {parquet_path}")
    return sample_count


def _write_parquet_batch_streaming(
    batch_data: dict[str, list],
    schema: pa.Schema,
    parquet_path: Path,
    writer: pq.ParquetWriter | None,
) -> pq.ParquetWriter:
    table = pa.table(batch_data, schema=schema)

    if writer is None:
        writer = pq.ParquetWriter(
            where=parquet_path,
            schema=schema,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
        )

    writer.write_table(table)
    return writer


@numba.njit(parallel=True)
def parallel_one_hot(
    array_chunk: np.ndarray,
    mapping: np.ndarray,
    output: np.ndarray,
) -> None:
    """
    Note the inner range(4) loop is seems to be faster than e.g. using
    output[i, j] = mapping[array_chunk[i, j]].
    """
    n_samples, n_features = array_chunk.shape

    for i in prange(n_samples):  # type: ignore[not-iterable]
        for j in range(n_features):
            for k in range(4):
                output[i, k, j] = mapping[array_chunk[i, j], k]


def _get_one_hot_encoded_generator(
    chunked_sample_generator: Generator[tuple[Sequence[str], np.ndarray], None, None],
    output_format: Literal["disk", "parquet"],
) -> Generator[tuple[str, np.ndarray], None, None]:
    mapping = np.eye(4, dtype=np.int8)
    total_samples = 0

    for id_chunk, array_chunk in chunked_sample_generator:
        n_samples, n_features = array_chunk.shape

        one_hot_final = np.empty((n_samples, 4, n_features), dtype=np.int8, order="C")

        parallel_one_hot(array_chunk, mapping, one_hot_final)

        assert (one_hot_final[0].sum(0) == 1).all()
        assert one_hot_final.dtype == np.int8
        assert one_hot_final.flags["C_CONTIGUOUS"], "Array not C-contiguous!"

        total_samples += n_samples
        logger.info("Processed %s samples.", total_samples)

        yield from zip(id_chunk, one_hot_final, strict=False)


def rechunk_generator(
    chunk_generator: Generator[tuple[Sequence[str], np.ndarray], None, None],
    new_chunk_size: int,
) -> Generator[tuple[Sequence[str], np.ndarray], None, None]:
    for id_chunk, array_chunk in chunk_generator:
        num_samples_in_chunk = array_chunk.shape[0]
        for i in range(0, num_samples_in_chunk, new_chunk_size):
            end_index = min(i + new_chunk_size, num_samples_in_chunk)

            id_sub_chunk = id_chunk[i:end_index]
            array_sub_chunk = array_chunk[i:end_index]

            if array_sub_chunk.shape[0] > 0:
                yield id_sub_chunk, array_sub_chunk


def get_sample_generator_from_bed(
    bed_path: Path,
    read_chunk_size: int = 1024,
) -> Generator[tuple[np.ndarray, np.ndarray], None, None]:
    plink_suffix = bed_path.with_suffix("")

    reader = GenoReader(plink_suffix=str(plink_suffix), chunk_size=read_chunk_size)
    ids = []
    arrays = []
    for item in reader:
        ids.append(item.sample_id)
        arrays.append(item.genotypes_as_numpy())
        if len(ids) >= read_chunk_size:
            yield np.array(ids), np.array(arrays, dtype=np.int8)
            ids = []
            arrays = []

    if ids:
        yield np.array(ids), np.array(arrays, dtype=np.int8)


@inherits(OneHotSNPs)
class FinalizeParsing(luigi.Task):
    output_folder = luigi.Parameter()
    output_format = luigi.Parameter()
    output_name = luigi.Parameter()
    read_chunk_size = luigi.IntParameter()
    process_chunk_size = luigi.IntParameter()

    def requires(self):
        return [self.clone(OneHotSNPs), self.clone(ExternalRawData)]

    def run(self):
        plink_base_path = Path(self.input()[1].path).with_suffix("")
        snp_path = plink_base_path.with_suffix(".bim")

        output_path = Path(self.output()[0].path)
        ensure_path_exists(output_path, is_folder=True)

        copyfile(snp_path, output_path / "data_final.bim")

    def output(self):
        output_path = Path(
            str(self.output_folder),
            "processed/parsed_files/full_indiv/full_snps",
        )
        one_hot_outputs = self.input()
        return [luigi.LocalTarget(str(output_path)), one_hot_outputs]


@requires(FinalizeParsing)
class CleanupIntermediateTaskOutputs(luigi.Task):
    raw_data_path = luigi.Parameter()
    output_folder = luigi.Parameter()
    output_format = luigi.Parameter()
    read_chunk_size = luigi.IntParameter()
    process_chunk_size = luigi.IntParameter()

    @property
    def interim_folder(self):
        # since raw_data_path is pointing to data/raw
        base = Path(str(self.raw_data_path)).parent
        return base / "interim"

    def run(self):
        assert self.interim_folder.name == "interim"
        if self.interim_folder.exists():
            rmtree(self.interim_folder)

    def complete(self):
        inputs = flatten(self.input())
        if len(inputs) == 0:
            warnings.warn(
                f"Task {self!r} without outputs has no custom complete() method",
                stacklevel=2,
            )
            return False

        inputs_finished = all(output.exists() for output in inputs)
        interim_deleted = not self.interim_folder.exists()
        return inputs_finished and interim_deleted


class RunAll(luigi.WrapperTask):
    """
    Wrappertask to control which tasks to run and with what parameters depending
    on program CL arguments.

    TODO: Proper argument handling / reformatting.
    """

    config = luigi.DictParameter()

    def requires(self):
        yield CleanupIntermediateTaskOutputs(**self.config)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--raw_data_path",
        type=str,
        default="",
        help="Path to raw data folder to be processed (containing data.bed, data.fam, "
        "data.bim)",
    )
    parser.add_argument(
        "--output_folder",
        type=str,
        default="data",
        help="Folder to save the processed data in.",
    )

    parser.add_argument(
        "--output_format",
        type=str,
        default="disk",
        choices=["disk", "parquet"],
        help="What format to save the data in.",
    )

    parser.add_argument(
        "--output_name",
        type=str,
        default="genotype",
        help="Name used for parquet dataset.",
    )

    parser.add_argument(
        "--read_chunk_size",
        type=int,
        default=8192,
        help="How many individuals to read from disk at a time. "
        "Larger values improve I/O efficiency.",
    )

    parser.add_argument(
        "--process_chunk_size",
        type=int,
        default=1024,
        help="How many individuals to process for one-hot encoding at a time. "
        "Smaller values reduce memory usage. Must be <= read_chunk_size.",
    )

    return parser


def get_cl_args():
    parser = get_parser()
    cl_args = parser.parse_args()
    validate_cl_args(cl_args)

    return cl_args


def main():
    cl_args = get_cl_args()
    luigi.build([RunAll(vars(cl_args))], local_scheduler=True)


if __name__ == "__main__":
    main()
