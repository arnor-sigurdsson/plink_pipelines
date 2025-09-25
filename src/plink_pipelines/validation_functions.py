from argparse import Namespace


def validate_cl_args(cl_args: Namespace) -> None:
    if hasattr(cl_args, "process_chunk_size") and hasattr(cl_args, "read_chunk_size"):
        if cl_args.process_chunk_size > cl_args.read_chunk_size:
            raise ValueError(
                f"process_chunk_size ({cl_args.process_chunk_size}) must be <= "
                f"read_chunk_size ({cl_args.read_chunk_size})"
            )
