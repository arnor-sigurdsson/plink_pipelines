from argparse import Namespace

import pytest

from plink_pipelines.validation_functions import validate_cl_args


def test_validate_cl_args_valid_chunk_sizes():
    args = Namespace(read_chunk_size=1000, process_chunk_size=500)
    validate_cl_args(args)


def test_validate_cl_args_equal_chunk_sizes():
    args = Namespace(read_chunk_size=1000, process_chunk_size=1000)
    validate_cl_args(args)


def test_validate_cl_args_invalid_chunk_sizes():
    args = Namespace(read_chunk_size=500, process_chunk_size=1000)

    with pytest.raises(
        ValueError,
        match="process_chunk_size \\(1000\\) must be <= read_chunk_size \\(500\\)",
    ):
        validate_cl_args(args)


def test_validate_cl_args_missing_attributes():
    args = Namespace(other_param="value")
    validate_cl_args(args)


def test_validate_cl_args_partial_attributes():
    args = Namespace(read_chunk_size=1000)
    validate_cl_args(args)

    args = Namespace(process_chunk_size=500)
    validate_cl_args(args)
