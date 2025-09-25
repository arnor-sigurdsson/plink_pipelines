import sys
from pathlib import Path
from unittest.mock import patch

import luigi
import numpy as np
import pyarrow.parquet as pq
import pytest

from plink_pipelines.make_dataset import rechunk_generator
from plink_pipelines.make_dataset import RunAll


def _get_test_cl_commands() -> list[str]:
    base = "plink_pipelines --raw_data_path tests/test_data/"

    commands = [
        base,
    ]

    extras = [
        " --output_format parquet",
        " --output_format disk",
        " --read_chunk_size 200 --process_chunk_size 100",
        " --read_chunk_size 200 --process_chunk_size 100 --output_format parquet",
        " --process_chunk_size 50",
        " --read_chunk_size 1000",
    ]

    for extra in extras:
        assert extra.startswith(" ")
        commands.append(base + extra)

    return commands


@pytest.mark.parametrize("command", _get_test_cl_commands())
def test_run_plink_pipelines_in_process(command: str, tmp_path: Path) -> None:
    luigi.task.Task.clear_instance_cache()

    command_split = command.split()[1:]
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


def test_rechunk_generator_basic():
    def mock_chunk_generator():
        ids = [f"sample_{i}" for i in range(10)]
        array = np.arange(10 * 5).reshape(10, 5).astype(np.int8)
        yield ids, array

    rechunked = list(rechunk_generator(mock_chunk_generator(), new_chunk_size=3))

    assert len(rechunked) == 4

    chunk1_ids, chunk1_array = rechunked[0]
    assert len(chunk1_ids) == 3
    assert chunk1_array.shape == (3, 5)
    assert chunk1_ids[0] == "sample_0"
    assert chunk1_ids[2] == "sample_2"

    chunk2_ids, chunk2_array = rechunked[1]
    assert len(chunk2_ids) == 3
    assert chunk2_array.shape == (3, 5)
    assert chunk2_ids[0] == "sample_3"

    chunk4_ids, chunk4_array = rechunked[3]
    assert len(chunk4_ids) == 1
    assert chunk4_array.shape == (1, 5)
    assert chunk4_ids[0] == "sample_9"


def test_rechunk_generator_exact_multiple():
    def mock_chunk_generator():
        ids = [f"sample_{i}" for i in range(6)]
        array = np.arange(6 * 3).reshape(6, 3).astype(np.int8)
        yield ids, array

    rechunked = list(rechunk_generator(mock_chunk_generator(), new_chunk_size=3))

    assert len(rechunked) == 2

    chunk1_ids, chunk1_array = rechunked[0]
    assert len(chunk1_ids) == 3
    assert chunk1_array.shape == (3, 3)

    chunk2_ids, chunk2_array = rechunked[1]
    assert len(chunk2_ids) == 3
    assert chunk2_array.shape == (3, 3)


def test_rechunk_generator_larger_chunk_size():
    def mock_chunk_generator():
        ids = [f"sample_{i}" for i in range(3)]
        array = np.arange(3 * 4).reshape(3, 4).astype(np.int8)
        yield ids, array

    rechunked = list(rechunk_generator(mock_chunk_generator(), new_chunk_size=5))

    assert len(rechunked) == 1

    chunk1_ids, chunk1_array = rechunked[0]
    assert len(chunk1_ids) == 3
    assert chunk1_array.shape == (3, 4)


def test_rechunk_generator_empty_input():
    def empty_generator():
        return
        yield

    rechunked = list(rechunk_generator(empty_generator(), new_chunk_size=5))
    assert len(rechunked) == 0


def test_rechunk_generator_multiple_input_chunks():
    def multiple_chunks_generator():
        for chunk_idx in range(3):
            ids = [f"chunk{chunk_idx}_sample_{i}" for i in range(4)]
            array = np.full((4, 2), chunk_idx, dtype=np.int8)
            yield ids, array

    rechunked = list(rechunk_generator(multiple_chunks_generator(), new_chunk_size=3))

    assert len(rechunked) == 6

    chunk1_ids, chunk1_array = rechunked[0]
    assert len(chunk1_ids) == 3
    assert chunk1_array.shape == (3, 2)
    assert (chunk1_array == 0).all()
    assert chunk1_ids[0] == "chunk0_sample_0"

    chunk2_ids, chunk2_array = rechunked[1]
    assert len(chunk2_ids) == 1
    assert chunk2_array.shape == (1, 2)
    assert (chunk2_array == 0).all()
    assert chunk2_ids[0] == "chunk0_sample_3"

    chunk3_ids, chunk3_array = rechunked[2]
    assert len(chunk3_ids) == 3
    assert chunk3_array.shape == (3, 2)
    assert (chunk3_array == 1).all()
    assert chunk3_ids[0] == "chunk1_sample_0"

    chunk6_ids, chunk6_array = rechunked[5]
    assert len(chunk6_ids) == 1
    assert chunk6_array.shape == (1, 2)
    assert (chunk6_array == 2).all()
    assert chunk6_ids[0] == "chunk2_sample_3"
