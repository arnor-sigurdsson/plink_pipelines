import argparse
import subprocess
import warnings
from pathlib import Path
from shutil import copyfile, rmtree
from typing import Generator, Tuple, Literal, Optional, Sequence

import deeplake
import luigi
import numpy as np
from aislib.misc_utils import ensure_path_exists, get_logger
from bed_reader import open_bed
from luigi.task import flatten
from luigi.util import requires, inherits

from plink_pipelines.validation_functions import validate_cl_args

logger = get_logger(name=__name__)


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
            bed_path=input_path, chunk_size=1000
        )
        sample_id_one_hot_array_generator = _get_one_hot_encoded_generator(
            chunked_sample_generator=chunk_generator
        )

        write_one_hot_outputs(
            id_array_generator=sample_id_one_hot_array_generator,
            output_folder=output_path,
            output_format=str(self.output_format),
            output_name=str(self.output_name),
        )


def write_one_hot_outputs(
    id_array_generator: Generator[Tuple[str, np.ndarray], None, None],
    output_folder: Path,
    output_format: Literal["disk", "deeplake"],
    output_name: Optional[str] = None,
) -> None:

    if output_format == "disk":
        _write_one_hot_arrays_to_disk(
            id_array_generator=id_array_generator, output_folder=output_folder
        )
    elif output_format == "deeplake":
        assert output_name is not None
        _write_one_hot_arrays_to_deeplake_ds(
            id_array_generator=id_array_generator,
            output_folder=output_folder,
            output_name=output_name,
        )
    else:
        raise ValueError(f"Unknown output format {output_format}")


def _write_one_hot_arrays_to_disk(
    id_array_generator: Generator[Tuple[str, np.ndarray], None, None],
    output_folder: Path,
):
    for id_, array in id_array_generator:
        output_path = output_folder / f"{id_}.npy"
        np.save(str(output_path), array)


def _write_one_hot_arrays_to_deeplake_ds(
    id_array_generator: Generator[Tuple[str, np.ndarray], None, None],
    output_folder: Path,
    output_name: str,
):
    ds_path = output_folder / output_name
    if deeplake.exists(ds_path):
        ds = deeplake.load(ds_path)
        assert "ID" in ds.tensors
    else:
        ds = deeplake.empty(ds_path)
        ds.create_tensor("ID", htype="text")

    ds.create_tensor(output_name, dtype="int8")
    with ds:
        for id_, array in id_array_generator:
            sample = {"ID": id_, output_name: array}
            ds.append(sample)


def _get_one_hot_encoded_generator(
    chunked_sample_generator: Generator[Tuple[Sequence[str], np.ndarray], None, None]
) -> Generator[Tuple[str, np.ndarray], None, None]:

    for id_chunk, array_chunk in chunked_sample_generator:
        one_hot_encoded = np.eye(4)[array_chunk]

        # convert (n_samples, n_snps, 4) -> (n_samples, 4, n_snps)
        one_hot_encoded = one_hot_encoded.transpose(0, 2, 1).astype(np.int8)
        assert (one_hot_encoded[0].sum(0) == 1).all()
        for id_, array in zip(id_chunk, one_hot_encoded):
            yield id_, array


def get_sample_generator_from_bed(
    bed_path: Path,
    chunk_size: int = 1000,
) -> Generator[Tuple[np.ndarray, np.ndarray], None, None]:
    with open_bed(bed_path) as bed_handle:
        n_samples = bed_handle.iid_count

        for index in range(0, n_samples, chunk_size):
            ids = bed_handle.iid[index : index + chunk_size]
            arrays = bed_handle.read(np.s_[index : index + chunk_size, :])
            arrays = arrays.astype(np.int8)
            yield ids, arrays
            logger.info("Processed %s samples.", index + chunk_size)


@inherits(OneHotSNPs)
class FinalizeParsing(luigi.Task):
    output_folder = luigi.Parameter()
    output_format = luigi.Parameter()
    output_name = luigi.Parameter()
    qc = luigi.BoolParameter()
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


def get_cl_args():
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
        "--indiv_sample_size",
        type=int,
        default=None,
        help="How many individuals to randomly sample.",
    )

    parser.add_argument(
        "--chr_sample",
        type=str,
        default="",
        help="Which chromosomes to sample, follows plink notation.",
    )

    parser.add_argument(
        "--autosome_only", action="store_true", help="Whether to only use autosomes."
    )

    parser.add_argument(
        "--extract_snp_file",
        type=str,
        default=None,
        help=".bim file to use if generating only the "
        "intersection between the data and the "
        "specified .bim file.",
    )

    cl_args = parser.parse_args()
    validate_cl_args(cl_args)

    return cl_args


def main():
    cl_args = get_cl_args()
    luigi.build([RunAll(vars(cl_args))], local_scheduler=True)


if __name__ == "__main__":
    main()
