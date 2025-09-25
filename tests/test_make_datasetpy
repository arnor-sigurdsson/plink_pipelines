import sys
from pathlib import Path
from unittest.mock import patch

import luigi
import numpy as np
import pyarrow.parquet as pq
import pytest

from plink_pipelines.make_dataset import RunAll


def _get_test_cl_commands() -> list[str]:
    base = "plink_pipelines --raw_data_path tests/test_data/"

    commands = [
        base,
    ]

    extras = [
        " --output_format parquet",
        " --output_format disk",
        " --array_chunk_size 100",
        " --array_chunk_size 100 --output_format parquet",
    ]

    for extra in extras:
        assert extra.startswith(" ")
        commands.append(base + extra)

    return commands


@pytest.mark.parametrize("command", _get_test_cl_commands())
def test_run_plink_pipelines_in_process(command: str, tmp_path: Path) -> None:
    luigi.task.Task.clear_instance_cache()

    command_split = command.split()[1:]  # Remove 'plink_pipelines' from start
    command_split.extend(["--output_folder", str(tmp_path)])

    with patch.object(sys, "argv", ["plink_pipelines"] + command_split):
        from plink_pipelines.make_dataset import get_cl_args

        cl_args = get_cl_args()

    success = luigi.build([RunAll(vars(cl_args))], local_scheduler=True)
    assert success, "Luigi pipeline failed"

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

    if "parquet" in command:
        validate_parquet_files(
            path=encoded_outputs_path,
            raw_data_folder=raw_data_folder,
        )
    else:
        validate_npy_files(
            path=encoded_outputs_path,
            raw_data_folder=raw_data_folder,
        )


def _lines_in_file(file_path: Path) -> int:
    with open(file_path) as f:
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


def validate_parquet_files(path: Path, raw_data_folder: Path) -> None:
    parquet_file = path / "genotype.parquet"
    assert parquet_file.exists(), f"Expected parquet file not found: {parquet_file}"

    fam_file = next(i for i in raw_data_folder.iterdir() if i.suffix == ".fam")
    bim_file = next(i for i in raw_data_folder.iterdir() if i.suffix == ".bim")

    expected_samples = _lines_in_file(file_path=fam_file)
    expected_snps = _lines_in_file(file_path=bim_file)
    expected_shape = (4, expected_snps)

    table = pq.read_table(parquet_file)
    df = table.to_pandas()

    assert len(df) == expected_samples, (
        f"Expected {expected_samples} rows, got {len(df)}"
    )

    expected_columns = {"sample_id", "genotype_data", "shape"}
    assert set(df.columns) == expected_columns, (
        f"Expected columns {expected_columns}, got {df.columns}"
    )

    for idx, row in df.iterrows():
        assert isinstance(row["sample_id"], str), (
            f"Row {idx}: sample_id should be string"
        )

        shape = row["shape"]
        assert list(shape) == list(expected_shape), (
            f"Row {idx}: expected shape {expected_shape}, got {shape}"
        )

        genotype_data = np.array(row["genotype_data"], dtype=np.int8).reshape(
            expected_shape
        )

        assert (genotype_data.sum(axis=0) == 1).all(), (
            f"Row {idx}: invalid one-hot encoding"
        )

        assert genotype_data.dtype == np.int8, f"Row {idx}: expected int8 dtype"
