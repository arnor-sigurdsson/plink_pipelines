import numpy as np
import pyarrow.parquet as pq
import pytest

from plink_pipelines.make_dataset import (
    _write_parquet_batch_streaming,
    write_one_hot_arrays_to_parquet,
    write_one_hot_outputs,
)


@pytest.fixture
def sample_data_generator():
    def generator():
        for i in range(5):
            sample_id = f"sample_{i}"
            array = np.zeros((4, 10), dtype=np.int8)
            for j in range(10):
                array[j % 4, j] = 1
            yield sample_id, array

    return generator


@pytest.fixture
def empty_generator():
    def generator():
        return
        yield  # unreachable

    return generator


def test_write_one_hot_arrays_to_parquet_basic(sample_data_generator, tmp_path):
    output_name = "test_genotype"
    batch_size = 2

    sample_count = write_one_hot_arrays_to_parquet(
        id_array_generator=sample_data_generator(),
        output_folder=tmp_path,
        output_name=output_name,
        batch_size=batch_size,
    )

    parquet_file = tmp_path / f"{output_name}.parquet"
    assert parquet_file.exists()

    assert sample_count == 5

    table = pq.read_table(parquet_file)
    df = table.to_pandas()

    assert len(df) == 5
    assert list(df.columns) == ["sample_id", "genotype_data", "shape"]

    first_row = df.iloc[0]
    assert first_row["sample_id"] == "sample_0"
    assert list(first_row["shape"]) == [4, 10]

    reconstructed = np.array(first_row["genotype_data"], dtype=np.int8).reshape(4, 10)
    assert reconstructed.dtype == np.int8
    assert (reconstructed.sum(axis=0) == 1).all()


def test_write_one_hot_arrays_to_parquet_empty_generator(empty_generator, tmp_path):
    output_name = "test_empty"
    batch_size = 10

    with pytest.raises(ValueError, match="Generator is empty"):
        write_one_hot_arrays_to_parquet(
            id_array_generator=empty_generator(),
            output_folder=tmp_path,
            output_name=output_name,
            batch_size=batch_size,
        )


def test_write_one_hot_arrays_to_parquet_shape_mismatch(tmp_path):
    def mismatched_generator():
        yield "sample_1", np.zeros((4, 10), dtype=np.int8)
        yield "sample_2", np.zeros((4, 5), dtype=np.int8)

    output_name = "test_mismatch"
    batch_size = 10

    with pytest.raises(ValueError, match="Array shape mismatch"):
        write_one_hot_arrays_to_parquet(
            id_array_generator=mismatched_generator(),
            output_folder=tmp_path,
            output_name=output_name,
            batch_size=batch_size,
        )


def test_write_parquet_batch_streaming(tmp_path):
    import pyarrow as pa

    batch_data = {
        "sample_id": ["sample_1", "sample_2"],
        "genotype_data": [
            np.zeros(40, dtype=np.int8).tolist(),
            np.ones(40, dtype=np.int8).tolist(),
        ],
        "shape": [[4, 10], [4, 10]],
    }

    schema = pa.schema(
        [
            pa.field("sample_id", pa.string()),
            pa.field("genotype_data", pa.list_(pa.int8())),
            pa.field("shape", pa.list_(pa.int32())),
        ]
    )

    parquet_path = tmp_path / "test_streaming.parquet"

    writer = _write_parquet_batch_streaming(
        batch_data=batch_data,
        schema=schema,
        parquet_path=parquet_path,
        writer=None,
    )
    assert writer is not None

    writer = _write_parquet_batch_streaming(
        batch_data=batch_data,
        schema=schema,
        parquet_path=parquet_path,
        writer=writer,
    )
    writer.close()

    assert parquet_path.exists()
    table = pq.read_table(parquet_path)
    df = table.to_pandas()

    assert len(df) == 4


def test_write_one_hot_outputs_parquet_format(sample_data_generator, tmp_path):
    output_name = "test_output"
    batch_size = 3

    write_one_hot_outputs(
        id_array_generator=sample_data_generator(),
        output_folder=tmp_path,
        output_format="parquet",
        batch_size=batch_size,
        output_name=output_name,
    )

    parquet_file = tmp_path / f"{output_name}.parquet"
    assert parquet_file.exists()

    table = pq.read_table(parquet_file)
    df = table.to_pandas()
    assert len(df) == 5


def test_write_one_hot_outputs_disk_format(sample_data_generator, tmp_path):
    write_one_hot_outputs(
        id_array_generator=sample_data_generator(),
        output_folder=tmp_path,
        output_format="disk",
        batch_size=3,
        output_name=None,
    )

    npy_files = list(tmp_path.glob("*.npy"))
    assert len(npy_files) == 5

    expected_files = {f"sample_{i}.npy" for i in range(5)}
    actual_files = {f.name for f in npy_files}
    assert actual_files == expected_files


def test_write_one_hot_outputs_parquet_missing_name(sample_data_generator, tmp_path):
    with pytest.raises(AssertionError):
        write_one_hot_outputs(
            id_array_generator=sample_data_generator(),
            output_folder=tmp_path,
            output_format="parquet",
            batch_size=10,
            output_name=None,
        )


def test_write_one_hot_outputs_invalid_format(sample_data_generator, tmp_path):
    with pytest.raises(ValueError, match="Unknown output format"):
        write_one_hot_outputs(
            id_array_generator=sample_data_generator(),
            output_folder=tmp_path,
            output_format="invalid_format",
            batch_size=10,
            output_name="test",
        )


def test_parquet_compression_and_options(sample_data_generator, tmp_path):
    output_name = "test_compression"

    write_one_hot_arrays_to_parquet(
        id_array_generator=sample_data_generator(),
        output_folder=tmp_path,
        output_name=output_name,
        batch_size=10,
    )

    parquet_file = tmp_path / f"{output_name}.parquet"

    parquet_file_obj = pq.ParquetFile(parquet_file)
    metadata = parquet_file_obj.metadata

    for i in range(metadata.num_row_groups):
        row_group = metadata.row_group(i)
        for j in range(row_group.num_columns):
            column = row_group.column(j)
            assert column.compression in ["SNAPPY", "UNCOMPRESSED"]
