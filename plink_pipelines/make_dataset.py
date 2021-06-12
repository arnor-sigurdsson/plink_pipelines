import argparse
import subprocess
import warnings
from pathlib import Path
from shutil import copyfile, rmtree

import luigi
import pandas as pd
from aislib import plink_utils
from aislib.misc_utils import ensure_path_exists
from luigi.task import flatten
from luigi.util import requires, inherits

from plink_pipelines.validation_functions import validate_cl_args


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
class PlinkRecode(Config):
    file_name = "interim/0_filtering_files/2_data_recoded/data_plink_processed.raw"
    extract_snp_file = luigi.Parameter()
    qc = luigi.BoolParameter()

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
        input_path = Path(self.input()[0].path)
        output_path = Path(self.output().path)
        ensure_path_exists(output_path)

        plink_input = input_path.parent / input_path.stem
        plink_output = output_path.parent / output_path.stem

        cmd = [
            "plink",
            "--bfile",
            plink_input,
            "--recodeA",
            "--make-bed",
            "--out",
            plink_output,
        ]

        if self.extract_snp_file:
            snp_series = pd.read_csv(self.extract_snp_file, sep=r"\s+", usecols=[1, 4])
            snp_series_path = output_path.parent / "snps_to_extract.txt"
            snp_series.to_csv(snp_series_path, index=False, header=False, sep="\t")
            cmd += ["--recode-allele", str(snp_series_path)]

        subprocess.call(cmd)


@requires(PlinkRecode)
class OneHotSNPs(Config):
    """
    Generates one hot encodings from a individuals x SNPs file.
    """

    output_folder = luigi.Parameter()
    file_name = "processed/encoded_outputs"

    def run(self):
        input_path = self.input().path
        output_path = Path(self.output().path)
        ensure_path_exists(output_path, is_folder=True)

        encoder = plink_utils.get_plink_raw_encoder()
        plink_utils.plink_raw_to_one_hot(input_path, output_path, encoder)


@inherits(OneHotSNPs, PlinkRecode)
class FinalizeParsing(luigi.Task):
    def requires(self):
        return self.clone(OneHotSNPs), self.clone(PlinkRecode)

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
        one_hot_outputs = self.input()[0]
        return [luigi.LocalTarget(str(output_path)), one_hot_outputs]


@requires(FinalizeParsing)
class CleanupIntermediateTaskOutputs(luigi.Task):

    raw_data_path = luigi.Parameter()
    output_folder = luigi.Parameter()
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
        """
        If the task has any outputs, return ``True`` if all outputs exist.
        Otherwise, return ``False``.

        However, you may freely override this method with custom logic.
        """

        # changed to input here, to make sure finalize is done
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
