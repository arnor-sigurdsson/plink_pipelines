import numpy as np
import pyarrow.parquet as pq

from plink_pipelines.make_dataset import (
    write_one_hot_arrays_to_parquet,
    write_one_hot_outputs,
)


def test_parquet_with_single_sample(tmp_path):
    def single_sample_generator():
        array = np.zeros((4, 5), dtype=np.int8)
        array[0, :] = 1
        yield "single_sample", array

    sample_count = write_one_hot_arrays_to_parquet(
        id_array_generator=single_sample_generator(),
        output_folder=tmp_path,
        output_name="single_test",
        batch_size=10,
    )

    assert sample_count == 1
    parquet_file = tmp_path / "single_test.parquet"
    assert parquet_file.exists()

    table = pq.read_table(parquet_file)
    df = table.to_pandas()
    assert len(df) == 1
    assert df.iloc[0]["sample_id"] == "single_sample"


def test_parquet_with_large_batch_size(tmp_path):
    def small_generator():
        for i in range(3):
            array = np.zeros((4, 5), dtype=np.int8)
            array[i % 4, :] = 1
            yield f"sample_{i}", array

    sample_count = write_one_hot_arrays_to_parquet(
        id_array_generator=small_generator(),
        output_folder=tmp_path,
        output_name="large_batch_test",
        batch_size=100,
    )

    assert sample_count == 3
    parquet_file = tmp_path / "large_batch_test.parquet"
    table = pq.read_table(parquet_file)
    df = table.to_pandas()
    assert len(df) == 3


def test_parquet_with_small_batch_size(tmp_path):
    def generator():
        for i in range(5):
            array = np.zeros((4, 3), dtype=np.int8)
            array[0, :] = 1
            yield f"sample_{i}", array

    sample_count = write_one_hot_arrays_to_parquet(
        id_array_generator=generator(),
        output_folder=tmp_path,
        output_name="small_batch_test",
        batch_size=1,
    )

    assert sample_count == 5
    parquet_file = tmp_path / "small_batch_test.parquet"
    table = pq.read_table(parquet_file)
    df = table.to_pandas()
    assert len(df) == 5


def test_parquet_with_special_characters_in_sample_id(tmp_path):
    def special_chars_generator():
        special_ids = ["sample-1", "sample_2", "sample.3", "sample@4", "sample#5"]
        for sample_id in special_ids:
            array = np.zeros((4, 2), dtype=np.int8)
            array[0, :] = 1
            yield sample_id, array

    sample_count = write_one_hot_arrays_to_parquet(
        id_array_generator=special_chars_generator(),
        output_folder=tmp_path,
        output_name="special_chars_test",
        batch_size=3,
    )

    assert sample_count == 5
    parquet_file = tmp_path / "special_chars_test.parquet"
    table = pq.read_table(parquet_file)
    df = table.to_pandas()

    expected_ids = ["sample-1", "sample_2", "sample.3", "sample@4", "sample#5"]
    actual_ids = df["sample_id"].tolist()
    assert actual_ids == expected_ids


def test_parquet_with_different_array_values(tmp_path):
    def varied_values_generator():
        for i in range(4):
            array = np.zeros((4, 3), dtype=np.int8)
            array[i, :] = 1
            yield f"sample_{i}", array

    sample_count = write_one_hot_arrays_to_parquet(
        id_array_generator=varied_values_generator(),
        output_folder=tmp_path,
        output_name="varied_values_test",
        batch_size=2,
    )

    assert sample_count == 4
    parquet_file = tmp_path / "varied_values_test.parquet"
    table = pq.read_table(parquet_file)
    df = table.to_pandas()

    for idx, row in df.iterrows():
        reconstructed = np.array(row["genotype_data"], dtype=np.int8).reshape(4, 3)
        assert (reconstructed.sum(axis=0) == 1).all()
        active_genotype = np.where(reconstructed.sum(axis=1) > 0)[0][0]
        assert active_genotype == idx


def test_parquet_file_size_reasonable(tmp_path):
    def large_generator():
        for i in range(100):
            array = np.zeros((4, 1000), dtype=np.int8)
            array[0, :] = 1  # All homozygous reference
            yield f"sample_{i:03d}", array

    sample_count = write_one_hot_arrays_to_parquet(
        id_array_generator=large_generator(),
        output_folder=tmp_path,
        output_name="large_test",
        batch_size=20,
    )

    assert sample_count == 100
    parquet_file = tmp_path / "large_test.parquet"
    assert parquet_file.exists()

    file_size = parquet_file.stat().st_size
    raw_data_size = 100 * 4 * 1000

    assert file_size < raw_data_size * 0.5


def test_parquet_schema_consistency(tmp_path):
    def generator():
        for i in range(3):
            array = np.zeros((4, 10), dtype=np.int8)
            array[i % 4, :] = 1
            yield f"sample_{i}", array

    write_one_hot_arrays_to_parquet(
        id_array_generator=generator(),
        output_folder=tmp_path,
        output_name="schema_test",
        batch_size=2,
    )

    parquet_file = tmp_path / "schema_test.parquet"

    table = pq.read_table(parquet_file)
    schema = table.schema

    assert len(schema) == 3

    field_names = [field.name for field in schema]
    assert "sample_id" in field_names
    assert "genotype_data" in field_names
    assert "shape" in field_names

    sample_id_field = next(f for f in schema if f.name == "sample_id")
    assert str(sample_id_field.type) == "string"

    genotype_field = next(f for f in schema if f.name == "genotype_data")
    assert "list" in str(genotype_field.type).lower()
    assert "int8" in str(genotype_field.type).lower()

    shape_field = next(f for f in schema if f.name == "shape")
    assert "list" in str(shape_field.type).lower()
    assert "int32" in str(shape_field.type).lower()


def test_output_folder_creation(tmp_path):
    non_existent_folder = tmp_path / "new_folder" / "subfolder"
    assert not non_existent_folder.exists()

    def generator():
        array = np.zeros((4, 5), dtype=np.int8)
        array[0, :] = 1
        yield "test_sample", array

    write_one_hot_outputs(
        id_array_generator=generator(),
        output_folder=non_existent_folder,
        output_format="parquet",
        batch_size=10,
        output_name="test_output",
    )

    assert non_existent_folder.exists()
    parquet_file = non_existent_folder / "test_output.parquet"
    assert parquet_file.exists()
