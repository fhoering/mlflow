import pytest
import mlflow
from mlflow.projects import _project_spec
from mlflow.exceptions import ExecutionException
from mlflow.projects.yarn import _validate_yarn_env

from tests.projects.utils import TEST_YARN_PROJECT_DIR


def test_valid_project_backend_yarn():
    project = _project_spec.load_project(TEST_YARN_PROJECT_DIR)
    _validate_yarn_env(project)


def test_invalid_project_backend_yarn():
    project = _project_spec.load_project(TEST_YARN_PROJECT_DIR)
    project.name = None
    with pytest.raises(ExecutionException):
        _validate_yarn_env(project)
