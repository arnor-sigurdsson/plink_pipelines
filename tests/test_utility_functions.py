from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np

from plink_pipelines.make_dataset import (
    _get_one_hot_encoded_generator,
    _save_array,
    _write_one_hot_arrays_to_disk,
    get_sample_generator_from_bed,
    parallel_one_hot,
)


def test_save_array(tmp_path):
    array = np.array([[1, 2], [3, 4]], dtype=np.int8)
    id_array = ("test_sample", array)

    result_path = _save_array(output_folder=tmp_path, id_array=id_array)

    expected_path = tmp_path / "test_sample.npy"
    assert result_path == expected_path
    assert expected_path.exists()

    loaded_array = np.load(str(expected_path))
    np.testing.assert_array_equal(loaded_array, array)


def test_parallel_one_hot():
    array_chunk = np.array([[0, 1, 2, 3], [1, 2, 3, 0]], dtype=np.int8)
    mapping = np.eye(4, dtype=np.int8)
    output = np.zeros((2, 4, 4), dtype=np.int8)

    parallel_one_hot(array_chunk, mapping, output)

    # Check first sample encodings
    assert output[0, 0, 0] == 1  # First sample, first SNP, genotype 0
    assert output[0, 1, 1] == 1  # First sample, second SNP, genotype 1
    assert output[0, 2, 2] == 1  # First sample, third SNP, genotype 2
    assert output[0, 3, 3] == 1  # First sample, fourth SNP, genotype 3

    # Check second sample encodings
    assert output[1, 1, 0] == 1  # Second sample, first SNP, genotype 1
    assert output[1, 2, 1] == 1  # Second sample, second SNP, genotype 2
    assert output[1, 3, 2] == 1  # Second sample, third SNP, genotype 3
    assert output[1, 0, 3] == 1  # Second sample, fourth SNP, genotype 0


def test_get_one_hot_encoded_generator():
    def chunked_generator():
        ids = np.array(["sample_1", "sample_2"])
        array = np.array([[0, 1], [2, 3]], dtype=np.int8)
        yield ids, array

    generator = _get_one_hot_encoded_generator(
        chunked_sample_generator=chunked_generator(), output_format="disk"
    )

    results = list(generator)
    assert len(results) == 2

    # Check first sample
    sample_id, one_hot_array = results[0]
    assert sample_id == "sample_1"
    assert one_hot_array.shape == (4, 2)
    assert one_hot_array.dtype == np.int8
    assert (one_hot_array.sum(axis=0) == 1).all()
    assert one_hot_array.flags["C_CONTIGUOUS"]

    # Check second sample
    sample_id, one_hot_array = results[1]
    assert sample_id == "sample_2"
    assert one_hot_array.shape == (4, 2)
    assert one_hot_array.dtype == np.int8
    assert (one_hot_array.sum(axis=0) == 1).all()


@patch("plink_pipelines.make_dataset.GenoReader")
def test_get_sample_generator_from_bed(mock_geno_reader_class):
    mock_reader = Mock()
    mock_geno_reader_class.return_value = mock_reader

    mock_item1 = Mock()
    mock_item1.sample_id = "sample_1"
    mock_item1.genotypes_as_numpy.return_value = np.array([0, 1, 2], dtype=np.int8)

    mock_item2 = Mock()
    mock_item2.sample_id = "sample_2"
    mock_item2.genotypes_as_numpy.return_value = np.array([1, 2, 3], dtype=np.int8)

    mock_reader.__iter__ = Mock(return_value=iter([mock_item1, mock_item2]))

    bed_path = Path("test_data.bed")
    generator = get_sample_generator_from_bed(bed_path=bed_path, chunk_size=2)

    results = list(generator)

    mock_geno_reader_class.assert_called_once_with(
        plink_suffix="test_data", chunk_size=2
    )

    assert len(results) == 1
    ids, arrays = results[0]

    np.testing.assert_array_equal(ids, ["sample_1", "sample_2"])
    expected_arrays = np.array([[0, 1, 2], [1, 2, 3]], dtype=np.int8)
    np.testing.assert_array_equal(arrays, expected_arrays)


@patch("plink_pipelines.make_dataset.GenoReader")
def test_get_sample_generator_from_bed_partial_chunk(mock_geno_reader_class):
    mock_reader = Mock()
    mock_geno_reader_class.return_value = mock_reader

    mock_item = Mock()
    mock_item.sample_id = "sample_1"
    mock_item.genotypes_as_numpy.return_value = np.array([0, 1], dtype=np.int8)

    mock_reader.__iter__ = Mock(return_value=iter([mock_item]))

    bed_path = Path("test_data.bed")
    generator = get_sample_generator_from_bed(bed_path=bed_path, chunk_size=5)

    results = list(generator)
    assert len(results) == 1

    ids, arrays = results[0]
    np.testing.assert_array_equal(ids, ["sample_1"])
    expected_arrays = np.array([[0, 1]], dtype=np.int8)
    np.testing.assert_array_equal(arrays, expected_arrays)


@patch("plink_pipelines.make_dataset.ProcessPoolExecutor")
@patch("plink_pipelines.make_dataset._save_array")
def test_write_one_hot_arrays_to_disk(mock_save_array, mock_executor, tmp_path):
    mock_executor_instance = Mock()
    mock_executor.return_value.__enter__.return_value = mock_executor_instance

    mocked_return = [tmp_path / f"sample_{i}.npy" for i in range(3)]
    mock_executor_instance.map.return_value = mocked_return

    mock_save_array.side_effect = lambda output_folder, id_array: (
        output_folder / f"{id_array[0]}.npy"
    )

    def test_generator():
        for i in range(3):
            sample_id = f"sample_{i}"
            array = np.zeros((4, 5), dtype=np.int8)
            yield sample_id, array

    _write_one_hot_arrays_to_disk(
        id_array_generator=test_generator(), output_folder=tmp_path
    )

    mock_executor.assert_called_once()
    mock_executor_instance.map.assert_called_once()
