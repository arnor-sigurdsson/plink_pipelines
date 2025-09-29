from unittest.mock import patch

import luigi
import pytest

from plink_pipelines.make_dataset import (
    CleanupIntermediateTaskOutputs,
    Config,
    RenameOnFailureMixin,
    RunAll,
)


class TestRenameOnFailureMixin:
    def test_on_failure_with_existing_file(self, tmp_path):
        class TestTask(RenameOnFailureMixin, luigi.Task):
            def output(self):
                return luigi.LocalTarget(str(tmp_path / "test_file.txt"))

        task = TestTask()
        target_file = tmp_path / "test_file.txt"
        target_file.write_text("test content")

        exception = Exception("Test exception")

        with patch.object(luigi.Task, "on_failure") as mock_super_on_failure:
            task.on_failure(exception)

        # Check that file was renamed
        failed_file = tmp_path / "test_file_FAILED.txt"
        assert failed_file.exists()
        assert not target_file.exists()
        # Luigi.Task.on_failure is called with (self, exception)
        mock_super_on_failure.assert_called_once_with(task, exception)

    def test_on_failure_with_nonexistent_file(self, tmp_path):
        class TestTask(RenameOnFailureMixin, luigi.Task):
            def output(self):
                return luigi.LocalTarget(str(tmp_path / "nonexistent_file.txt"))

        task = TestTask()
        exception = Exception("Test exception")

        with patch.object(luigi.Task, "on_failure") as mock_super_on_failure:
            task.on_failure(exception)

        mock_super_on_failure.assert_called_once_with(task, exception)


# Simple tests for basic Config functionality
class TestConfig:
    def test_file_name_not_implemented(self):
        config = Config(output_folder="test")
        with pytest.raises(NotImplementedError):
            _ = config.file_name

    def test_sample_fname_part(self):
        config = Config(output_folder="test")
        assert config._sample_fname_part == "full_inds/full_chrs"

    def test_output_target(self, tmp_path):
        config = Config(output_folder=str(tmp_path))
        target = config.output_target("processed/test_file.txt")

        expected_path = (
            tmp_path / "processed" / "full_inds" / "full_chrs" / "test_file.txt"
        )
        assert target.path == str(expected_path)


class TestRunAll:
    def test_requires(self):
        task_params = {
            "raw_data_path": "test_path",
            "output_folder": "test_output",
            "output_format": "disk",
            "output_name": "test",
            "array_chunk_size": 100,
        }

        task = RunAll(task_params)
        requirements = list(task.requires())  # Convert generator to list

        assert len(requirements) == 1
        assert isinstance(requirements[0], CleanupIntermediateTaskOutputs)
        assert requirements[0].raw_data_path == "test_path"
        assert requirements[0].output_folder == "test_output"
