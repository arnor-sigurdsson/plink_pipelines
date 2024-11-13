import argparse
import logging
import os
import subprocess
import warnings
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from pathlib import Path
from shutil import copyfile, rmtree, which
from typing import Generator, Literal, Optional, Sequence, Tuple

import deeplake
import luigi
import numpy as np
from aislib.misc_utils import ensure_path_exists, get_logger
from bed_reader import open_bed
from luigi.task import flatten
from luigi.util import inherits, requires

from plink_pipelines.validation_functions import validate_cl_args

logger = get_logger(name=__name__)

luigi_logger = logging.getLogger("luigi")
luigi_logger.setLevel(logging.INFO)


class RenameOnFailureMixin(object):
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
    indiv_sample_size = luigi.IntParameter()
    chr_sample = luigi.Parameter()

    @property
    def input_name(self):
        bed_files = [
            i for i in Path(str(self.raw_data_path)).iterdir() if i.suffix == ".bed"
        ]
        if len(bed_files) != 1:
            raise ValueError(
                f"Expected one .bed file in {self.raw_data_path}, but"
                f"found {bed_files}."
            )
        return str(bed_files[0])

    @property
    def file_name(self):
        raise NotImplementedError

    @property
    def _sample_fname_part(self):
        indiv_part = self.indiv_sample_size if self.indiv_sample_size else "full"
        snp_part = self.chr_sample if self.chr_sample else "full"

        return f"{indiv_part}_inds" + f"/{snp_part}_chrs"

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
                f"Expected one .bed file in {self.raw_data_path}, but"
                f"found {bed_files}."
            )
        return str(bed_files[0])

    def output(self):
        return luigi.LocalTarget(str(self.input_name))


@requires(ExternalRawData)
class PlinkExtractAlleles(Config):
    raw_data_path = luigi.Parameter()
    extract_snp_file = luigi.Parameter()

    file_name = "interim/0_filtering_files/0_data_extracted/data_plink_extracted.bed"

    def run(self):
        input_path = Path(self.input().path)
        output_path = Path(self.output().path)
        ensure_path_exists(output_path)

        plink_input = input_path.parent / input_path.stem
        plink_output = output_path.parent / output_path.stem

        cmd = [
            "plink",
            "--bfile",
            plink_input,
            "--extract",
            self.extract_snp_file,
            "--make-bed",
            "--out",
            plink_output,
        ]

        subprocess.call(cmd)


@inherits(ExternalRawData, PlinkExtractAlleles)
class PlinkQC(Config):
    raw_data_path = luigi.Parameter()
    autosome_only = luigi.BoolParameter()
    extract_snp_file = luigi.Parameter()

    file_name = "interim/0_filtering_files/1_data_QC/data_plink_QC.bed"

    def requires(self):
        if self.extract_snp_file:
            return self.clone(PlinkExtractAlleles)
        return self.clone(ExternalRawData)

    def run(self):
        _validate_plink_exists_in_path()

        input_path = Path(self.input().path)
        output_path = Path(self.output().path)
        ensure_path_exists(output_path)

        plink_input = input_path.parent / input_path.stem
        plink_output = output_path.parent / output_path.stem
        cmd = [
            "plink",
            "--bfile",
            plink_input,
            "--maf",
            str(0.001),
            "--geno",
            str(0.03),
            "--mind",
            str(0.1),
            "--make-bed",
            "--out",
            plink_output,
        ]

        if self.indiv_sample_size:
            cmd += ["--thin-indiv-count", str(self.indiv_sample_size)]

        if self.chr_sample:
            cmd += ["--chr", str(self.chr_sample)]

        if self.autosome_only:
            cmd += ["--autosome"]

        subprocess.call(cmd)


def _validate_plink_exists_in_path():
    if which("plink") is None:
        raise RuntimeError(
            "plink is not installed or is not in the path. "
            "Please install plink and try again."
        )


@inherits(ExternalRawData, PlinkExtractAlleles, PlinkQC)
class OneHotSNPs(Config):
    """
    Generates one hot encodings from a individuals x SNPs file.
    """

    output_folder = luigi.Parameter()
    output_format = luigi.Parameter()
    output_name = luigi.Parameter()
    qc = luigi.BoolParameter()
    extract_snp_file = luigi.Parameter()
    array_chunk_size = luigi.IntParameter()
    file_name = "processed/encoded_outputs"

    def requires(self):
        """
        Here we can return any of:

            - [raw.bed]             # no QC, no extract
            - [extract.bed]         # only extract
            - [qc.bed]              # only QC
            - [qc.bed, extract.bed] # both, but qc.bed is the input as it happens after
        """
        base = []

        if self.qc:
            base += [self.clone(PlinkQC)]
        if self.extract_snp_file:
            base += [self.clone(PlinkExtractAlleles)]

        if not base:
            return [self.clone(ExternalRawData)]
        return base

    def run(self):
        input_path = Path(self.input()[-1].path)
        assert input_path.suffix == ".bed"

        output_path = Path(self.output().path)
        ensure_path_exists(output_path, is_folder=True)

        chunk_generator = get_sample_generator_from_bed(
            bed_path=input_path, chunk_size=int(self.array_chunk_size)
        )
        sample_id_one_hot_array_generator = _get_one_hot_encoded_generator(
            chunked_sample_generator=chunk_generator
        )

        write_one_hot_outputs(
            id_array_generator=sample_id_one_hot_array_generator,
            output_folder=output_path,
            output_format=str(self.output_format),
            output_name=str(self.output_name),
            batch_size=int(self.array_chunk_size),
        )


def write_one_hot_outputs(
    id_array_generator: Generator[Tuple[str, np.ndarray], None, None],
    output_folder: Path,
    output_format: Literal["disk", "deeplake"],
    batch_size: int,
    output_name: Optional[str] = None,
) -> None:
    if output_format == "disk":
        _write_one_hot_arrays_to_disk(
            id_array_generator=id_array_generator,
            output_folder=output_folder,
        )
    elif output_format == "deeplake":
        assert output_name is not None
        _write_one_hot_arrays_to_deeplake_ds(
            id_array_generator=id_array_generator,
            output_folder=output_folder,
            output_name=output_name,
            batch_size=batch_size,
        )
    else:
        raise ValueError(f"Unknown output format {output_format}")


def _save_array(output_folder: Path, id_array: Tuple[str, np.ndarray]) -> Path:
    id_, array = id_array
    output_path = output_folder / f"{id_}.npy"
    np.save(str(output_path), array)
    return output_path


def _write_one_hot_arrays_to_disk(
    id_array_generator: Generator[Tuple[str, np.ndarray], None, None],
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
                _ = [f for f in futures]
                batch = []

        if batch:
            futures = list(executor.map(save_fn, batch))
            _ = [f for f in futures]


def _write_one_hot_arrays_to_deeplake_ds(
    id_array_generator: Generator[Tuple[str, np.ndarray], None, None],
    output_folder: Path,
    output_name: str,
    batch_size: int,
    commit_frequency: int = 1024,
) -> int:
    ds_path = str(output_folder / output_name)

    try:
        first_id, first_array = next(id_array_generator)
    except StopIteration:
        raise ValueError("Generator is empty")

    array_shape = list(first_array.shape)

    if deeplake.exists(ds_path):
        ds = deeplake.open(ds_path)
        columns = {col.name for col in ds.schema.columns}
        if "ID" not in columns:
            raise ValueError(
                f"Existing dataset at {ds_path} missing required 'ID' column"
            )
    else:
        ds = deeplake.create(ds_path)
        ds.add_column("ID", dtype=deeplake.types.Text())
        array_schema = deeplake.types.Array(dtype="bool", shape=array_shape)
        ds.add_column(output_name, dtype=array_schema)
        ds.commit()

    assert first_array.flags["C_CONTIGUOUS"], "Array not C-contiguous!"

    ds.append({"ID": [first_id], output_name: [first_array]})
    ds.commit()

    try:
        batch = {"ID": [], output_name: []}
        sample_count = 1

        for id_, array in id_array_generator:
            if list(array.shape) != array_shape:
                raise ValueError(
                    f"Array shape mismatch at ID {id_}. Expected {array_shape}, "
                    f"got {list(array.shape)}"
                )

            assert array.flags["C_CONTIGUOUS"], f"Array for {id_} not C-contiguous!"

            batch["ID"].append(id_)
            batch[output_name].append(array)
            sample_count += 1

            if len(batch["ID"]) >= batch_size:
                ds.append(batch)
                batch = {"ID": [], output_name: []}

            if sample_count % commit_frequency == 0:
                ds.commit(f"Processed {sample_count} samples")

        if batch["ID"]:
            ds.append(batch)

    except Exception as e:
        ds.rollback()
        raise RuntimeError(f"Error processing samples: {str(e)}") from e

    ds.commit(f"Completed processing {sample_count} samples")
    return sample_count


def parallel_one_hot(array_chunk: np.ndarray, mapping: np.ndarray) -> np.ndarray:
    """
    TODO:
        Decorate with '@numba.njit(parallel=True)' and change sample loop
        to numba prange once numba supports numpy>=2.1.3 (once numba version 0.61 is
        released).
    """
    n_samples, n_features = array_chunk.shape
    result = np.empty((n_samples, n_features, 4), dtype=np.int8)
    for i in range(n_samples):  # TODO: change to prange
        for j in range(n_features):
            result[i, j] = mapping[array_chunk[i, j]]
    return result


def _get_one_hot_encoded_generator(
    chunked_sample_generator: Generator[Tuple[Sequence[str], np.ndarray], None, None]
) -> Generator[Tuple[str, np.ndarray], None, None]:
    """
    IMPORTANT NOTE ON MEMORY LAYOUT in DeepLake V4:
    This function ensures proper memory layout for storage in DeepLake. The PyTorch
    transpose operation creates arrays in Fortran order (column-major) memory layout:
        - Original data: C_CONTIGUOUS=True, F_CONTIGUOUS=False
        - After transpose: C_CONTIGUOUS=False, F_CONTIGUOUS=True

    DeepLake assumes C-order (row-major) when storing/loading arrays. If we store
    a Fortran-ordered array, Deep Lake will read the memory in the wrong order,
    corrupting the data. Consider this example:

    Fortran-ordered memory of a one-hot array:
        Memory: [1,0,0,0, 0,1,0,0, 0,0,1,0, 0,0,0,1]
        Intended shape (4x4):    When Deep Lake reads in C-order:
        1 0 0 0                  1 1 0 0
        0 1 0 0      -->         0 0 0 0
        0 0 1 0                  0 0 0 0
        0 0 0 1                  0 0 1 1

    To prevent this, we use np.ascontiguousarray() to ensure C-ordered memory
    layout before yielding the arrays for storage.
    """
    mapping = np.eye(4, dtype=np.int8)

    for id_chunk, array_chunk in chunked_sample_generator:
        one_hot = mapping[array_chunk]

        one_hot_transposed = np.transpose(one_hot, (0, 2, 1))

        one_hot_final = np.ascontiguousarray(one_hot_transposed)

        assert (one_hot_final[0].sum(0) == 1).all()
        assert one_hot_final.dtype == np.int8

        for id_, array in zip(id_chunk, one_hot_final):
            yield id_, array


def get_sample_generator_from_bed(
    bed_path: Path,
    chunk_size: int = 1000,
) -> Generator[Tuple[np.ndarray, np.ndarray], None, None]:
    """
    Note the indexing is a bit weird below, as if we only index the first dimension
    (which should be individuals), it actually indexes SNPs. So we need to
    explicitly index both dimensions.
    """
    with open_bed(location=bed_path) as bed_handle:
        n_samples = bed_handle.iid_count

        for index in range(0, n_samples, chunk_size):
            samples_idx_start = index
            samples_idx_end = index + chunk_size
            ids = bed_handle.iid[samples_idx_start:samples_idx_end]
            arrays = bed_handle.read(
                index=np.s_[samples_idx_start:samples_idx_end, :],
                dtype=np.int8,
            )
            arrays[arrays == -127] = 3  # NA is encoded as -127
            yield ids, arrays
            logger.info("Processed %s samples.", index + chunk_size)


@inherits(OneHotSNPs)
class FinalizeParsing(luigi.Task):
    output_folder = luigi.Parameter()
    output_format = luigi.Parameter()
    output_name = luigi.Parameter()
    qc = luigi.BoolParameter()
    array_chunk_size = luigi.IntParameter()
    extract_snp_file = luigi.Parameter()

    def requires(self):
        base = [self.clone(OneHotSNPs)]

        last_plink_task = [self.clone(ExternalRawData)]
        if self.extract_snp_file:
            last_plink_task = [self.clone(PlinkExtractAlleles)]
        if self.qc:
            last_plink_task = [self.clone(PlinkQC)]

        base += last_plink_task
        assert len(base) == 2

        return base

    def run(self):
        plink_base_path = Path(self.input()[1].path).with_suffix("")
        snp_path = plink_base_path.with_suffix(".bim")

        output_path = Path(self.output()[0].path)
        ensure_path_exists(output_path, is_folder=True)

        copyfile(snp_path, output_path / "data_final.bim")

    def output(self):
        indiv_sample_size = self.indiv_sample_size or "full"
        chr_sample = self.chr_sample or "full"
        output_path = Path(
            str(self.output_folder),
            f"processed/parsed_files/{indiv_sample_size}_indiv/{chr_sample}_snps",
        )
        one_hot_outputs = self.input()
        return [luigi.LocalTarget(str(output_path)), one_hot_outputs]


@requires(FinalizeParsing)
class CleanupIntermediateTaskOutputs(luigi.Task):
    raw_data_path = luigi.Parameter()
    output_folder = luigi.Parameter()
    output_format = luigi.Parameter()
    qc = luigi.BoolParameter()
    array_chunk_size = luigi.IntParameter()
    indiv_sample_size = luigi.IntParameter()
    chr_sample = luigi.Parameter()
    autosome_only = luigi.BoolParameter()
    extract_snp_file = luigi.Parameter()

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
                "Task %r without outputs has no custom complete() method" % self,
                stacklevel=2,
            )
            return False

        inputs_finished = all(map(lambda output: output.exists(), inputs))
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
        choices=["disk", "deeplake"],
        help="What format to save the data in.",
    )

    parser.add_argument(
        "--output_name",
        type=str,
        default="genotype",
        help="Name used for deeplake dataset.",
    )

    parser.add_argument(
        "--do_qc",
        action="store_true",
        dest="qc",
        help="Whether to do basic QC on plink data (--maf 0.001, --geno 0.03, "
        "--mind 0.1). Default: False.",
    )
    parser.add_argument(
        "--no_qc", dest="qc", action="store_false", help="Skip QC on plink data."
    )
    parser.set_defaults(qc=False)

    parser.add_argument(
        "--array_chunk_size",
        type=int,
        default=1000,
        help="How many individuals to process at a time. "
        "Useful to avoid running out of memory.",
    )

    parser.add_argument(
        "--indiv_sample_size",
        type=int,
        default=None,
        help="How many individuals to randomly sample."
        " Only applicable if do_qc is set.",
    )

    parser.add_argument(
        "--chr_sample",
        type=str,
        default="",
        help="Which chromosomes to sample, follows plink notation."
        " Only applicable if do_qc is set.",
    )

    parser.add_argument(
        "--autosome_only",
        action="store_true",
        help="Whether to only use autosomes. " "Only applicable if do_qc is set.",
    )

    parser.add_argument(
        "--extract_snp_file",
        type=str,
        default="",
        help=".bim file to use if generating only the "
        "intersection between the data and the "
        "specified .bim file.",
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
