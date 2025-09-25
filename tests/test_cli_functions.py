from argparse import Namespace
from unittest.mock import Mock, patch

import pytest

from plink_pipelines.make_dataset import get_cl_args, get_parser, main
from plink_pipelines.validation_functions import validate_cl_args


def test_get_parser():
    parser = get_parser()

    args = parser.parse_args([])
    assert args.raw_data_path == ""
    assert args.output_folder == "data"
    assert args.output_format == "disk"
    assert args.output_name == "genotype"
    assert args.read_chunk_size == 8192
    assert args.process_chunk_size == 1024

    args = parser.parse_args(
        [
            "--raw_data_path",
            "test/path",
            "--output_folder",
            "output",
            "--output_format",
            "parquet",
            "--output_name",
            "test_genotype",
            "--read_chunk_size",
            "2000",
            "--process_chunk_size",
            "500",
        ]
    )

    assert args.raw_data_path == "test/path"
    assert args.output_folder == "output"
    assert args.output_format == "parquet"
    assert args.output_name == "test_genotype"
    assert args.read_chunk_size == 2000
    assert args.process_chunk_size == 500


def test_get_parser_invalid_format():
    parser = get_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--output_format", "invalid"])


@patch("plink_pipelines.make_dataset.validate_cl_args")
@patch("argparse.ArgumentParser.parse_args")
def test_get_cl_args(mock_parse_args, mock_validate):
    mock_args = Mock()
    mock_parse_args.return_value = mock_args

    result = get_cl_args()

    mock_validate.assert_called_once_with(mock_args)
    assert result == mock_args


@patch("plink_pipelines.make_dataset.luigi.build")
@patch("plink_pipelines.make_dataset.get_cl_args")
def test_main(mock_get_cl_args, mock_luigi_build):
    mock_args = Namespace(
        raw_data_path="test",
        output_folder="output",
        output_format="disk",
        output_name="genotype",
        read_chunk_size=8192,
        process_chunk_size=1024,
    )
    mock_get_cl_args.return_value = mock_args

    main()

    mock_get_cl_args.assert_called_once()
    mock_luigi_build.assert_called_once()

    call_args = mock_luigi_build.call_args
    assert call_args[1]["local_scheduler"] is True


def test_validate_cl_args():
    args = Namespace(
        raw_data_path="test",
        output_folder="output",
        output_format="disk",
        output_name="genotype",
        read_chunk_size=8192,
        process_chunk_size=1024,
    )

    validate_cl_args(args)
