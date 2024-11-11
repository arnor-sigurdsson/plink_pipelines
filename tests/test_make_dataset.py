import subprocess
from pathlib import Path

import deeplake
import numpy as np
import pytest


def _get_test_cl_commands() -> list[str]:
    base = "plink_pipelines --raw_data_path tests/test_data/"

    commands = [
        base,
    ]

    extras = [
        " --output_format deeplake",
        " --array_chunk_size 100",
        " --do_qc --autosome_only",
    ]

    for extra in extras:
        assert extra.startswith(" ")
        commands.append(base + extra)

    return commands


@pytest.mark.parametrize("command", _get_test_cl_commands())
def test_run_plink_pipelines(command: str, tmp_path: Path) -> None:
    command_split = command.split()
    command_split.extend(["--output_folder", str(tmp_path)])

    subprocess.run(command_split, check=True)

    processed_path = tmp_path / "processed"
    full_inds_path = processed_path / "full_inds"
    full_chrs_path = full_inds_path / "full_chrs"
    encoded_outputs_path = full_chrs_path / "encoded_outputs"
    parsed_files_path = processed_path / "parsed_files"
    full_indiv_path = parsed_files_path / "full_indiv"
    full_snps_path = full_indiv_path / "full_snps"
    data_final_bim_path = full_snps_path / "data_final.bim"

    assert processed_path.exists()
    assert full_inds_path.exists()
    assert full_chrs_path.exists()
    assert encoded_outputs_path.exists()
    assert parsed_files_path.exists()
    assert full_indiv_path.exists()
    assert full_snps_path.exists()
    assert data_final_bim_path.exists()

    raw_data_folder = Path("tests/test_data/")
    if "--do_qc" not in command and "deeplake" not in command:
        validate_npy_files(
            path=encoded_outputs_path,
            raw_data_folder=raw_data_folder,
        )

    if "deeplake" in command:
        ds_path = tmp_path / "processed/full_inds/full_chrs/encoded_outputs/genotype"
        ds = deeplake.open_read_only(str(ds_path))

        fam_file = next(i for i in raw_data_folder.iterdir() if i.suffix == ".fam")
        bim_file = next(i for i in raw_data_folder.iterdir() if i.suffix == ".bim")

        expected_samples = _lines_in_file(file_path=fam_file)
        expected_snps = _lines_in_file(file_path=bim_file)
        expected_shape = (4, expected_snps)
        for row in ds:
            assert len(ds) == expected_samples
            assert row["genotype"].shape == expected_shape
            assert (row["genotype"].sum(0) == 1).all()


def _lines_in_file(file_path: Path) -> int:
    with open(file_path, "r") as f:
        return sum(1 for _ in f)


def validate_npy_files(path: Path, raw_data_folder: Path) -> None:
    file_count = 0

    fam_file = next(i for i in raw_data_folder.iterdir() if i.suffix == ".fam")
    bim_file = next(i for i in raw_data_folder.iterdir() if i.suffix == ".bim")

    expected_samples = _lines_in_file(file_path=fam_file)
    expected_snps = _lines_in_file(file_path=bim_file)
    expected_shape = (4, expected_snps)

    for file in path.iterdir():
        if file.suffix == ".npy":
            file_count += 1
            data = np.load(file=str(file))
            assert data.shape == expected_shape
            assert data.dtype == np.int8

    assert file_count == expected_samples
