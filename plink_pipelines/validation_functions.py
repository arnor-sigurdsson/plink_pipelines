from argparse import Namespace
from pathlib import Path


def validate_cl_args(cl_args: Namespace) -> None:

    if cl_args.extract_snp_file:
        snp_file_path = Path(cl_args.extract_snp_file)
        error_msg = (
            f"Expected .bim file for --extract_snp_file but got "
            f"{snp_file_path} instead."
        )
        if snp_file_path.suffix != ".bim":
            raise ValueError(error_msg)
