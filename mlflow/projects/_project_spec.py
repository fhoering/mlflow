"""Internal utilities for parsing MLproject YAML files."""

import os
import yaml

from six.moves import shlex_quote

from mlflow import data
from mlflow.exceptions import ExecutionException
from mlflow.tracking import artifact_utils
from mlflow.utils.file_utils import get_local_path_or_none
from mlflow.utils.string_utils import is_string_type


MLPROJECT_FILE_NAME = "mlproject"
DEFAULT_CONDA_FILE_NAME = "conda.yaml"
DEFAULT_REQUIREMENTS_FILE_NAME = "requirements.txt"


def _find_mlproject(directory):
    filenames = os.listdir(directory)
    for filename in filenames:
        if filename.lower() == MLPROJECT_FILE_NAME:
            return os.path.join(directory, filename)
    return None


def load_project(directory):
    mlproject_path = _find_mlproject(directory)

    # TODO: Validate structure of YAML loaded from the file
    yaml_obj = {}
    if mlproject_path is not None:
        with open(mlproject_path) as mlproject_file:
            yaml_obj = yaml.safe_load(mlproject_file)

    project_name = yaml_obj.get("name")

    # Validate config if docker_env parameter is present
    docker_env = yaml_obj.get("docker_env")
    if docker_env:
        if not docker_env.get("image"):
            raise ExecutionException(
                "Project configuration (MLproject file) was invalid: Docker "
                "environment specified but no image attribute found."
            )
        if docker_env.get("volumes"):
            if not (
                isinstance(docker_env["volumes"], list)
                and all([isinstance(i, str) for i in docker_env["volumes"]])
            ):
                raise ExecutionException(
                    "Project configuration (MLproject file) was invalid: "
                    "Docker volumes must be a list of strings, "
                    """e.g.: '["/path1/:/path1", "/path2/:/path2"])"""
                )
        if docker_env.get("environment"):
            if not (
                isinstance(docker_env["environment"], list)
                and all(
                    [isinstance(i, list) or isinstance(i, str) for i in docker_env["environment"]]
                )
            ):
                raise ExecutionException(
                    "Project configuration (MLproject file) was invalid: "
                    "environment must be a list containing either strings (to copy environment "
                    "variables from host system) or lists of string pairs (to define new "
                    "environment variables)."
                    """E.g.: '[["NEW_VAR", "new_value"], "VAR_TO_COPY_FROM_HOST"])"""
                )

    conda_path = yaml_obj.get("conda_env")

    pip_path = yaml_obj.get("pip_path")

    if (
        (conda_path and pip_path)
        or (conda_path and docker_env)
        or (docker_env and pip_path)
        or (docker_env and pip_path and conda_path)
    ):
        raise ExecutionException(
            "Project can only contain either docker_env, conda_env or pip_path."
        )

    # Parse entry points
    entry_points = {}
    for name, entry_point_yaml in yaml_obj.get("entry_points", {}).items():
        parameters = entry_point_yaml.get("parameters", {})
        command = entry_point_yaml.get("command")
        entry_points[name] = EntryPoint(name, parameters, command)

    conda_env_path = _get_path_from_spec(conda_path, directory, DEFAULT_CONDA_FILE_NAME)
    pip_env_path = _get_path_from_spec(pip_path, directory, DEFAULT_REQUIREMENTS_FILE_NAME)

    return Project(
        conda_env_path=conda_env_path,
        entry_points=entry_points,
        docker_env=docker_env,
        pip_path=pip_env_path,
        name=project_name,
    )


def _get_path_from_spec(spec, directory, default_file_name):
    if spec:
        env_path = os.path.join(directory, spec)
        if not os.path.exists(env_path):
            raise ExecutionException(
                "Project specified environment file %s, but no such " "file was found." % env_path
            )
        return env_path

    default_env_path = os.path.join(directory, default_file_name)
    if os.path.exists(default_env_path):
        return default_env_path

    return None


class Project(object):
    """A project specification loaded from an MLproject file in the passed-in directory."""

    def __init__(self, conda_env_path, entry_points, docker_env, pip_path, name):
        self.conda_env_path = conda_env_path
        self._entry_points = entry_points
        self.docker_env = docker_env
        self.pip_path = pip_path
        self.name = name

    def get_entry_point(self, entry_point):
        if entry_point in self._entry_points:
            return self._entry_points[entry_point]
        _, file_extension = os.path.splitext(entry_point)
        ext_to_cmd = {".py": "python", ".sh": os.environ.get("SHELL", "bash")}
        if file_extension in ext_to_cmd:
            command = "%s %s" % (ext_to_cmd[file_extension], shlex_quote(entry_point))
            if not is_string_type(command):
                command = command.encode("utf-8")
            return EntryPoint(name=entry_point, parameters={}, command=command)
        elif file_extension == ".R":
            command = "Rscript -e \"mlflow::mlflow_source('%s')\" --args" % shlex_quote(entry_point)
            return EntryPoint(name=entry_point, parameters={}, command=command)
        raise ExecutionException(
            "Could not find {0} among entry points {1} or interpret {0} as a "
            "runnable script. Supported script file extensions: "
            "{2}".format(entry_point, list(self._entry_points.keys()), list(ext_to_cmd.keys()))
        )


class EntryPoint(object):
    """An entry point in an MLproject specification."""

    def __init__(self, name, parameters, command):
        self.name = name
        self.parameters = {k: Parameter(k, v) for (k, v) in parameters.items()}
        self.command = command

    def _validate_parameters(self, user_parameters):
        missing_params = []
        for name in self.parameters:
            if name not in user_parameters and self.parameters[name].default is None:
                missing_params.append(name)
        if missing_params:
            raise ExecutionException(
                "No value given for missing parameters: %s"
                % ", ".join(["'%s'" % name for name in missing_params])
            )

    def compute_parameters(self, user_parameters, storage_dir):
        """
        Given a dict mapping user-specified param names to values, computes parameters to
        substitute into the command for this entry point. Returns a tuple (params, extra_params)
        where `params` contains key-value pairs for parameters specified in the entry point
        definition, and `extra_params` contains key-value pairs for additional parameters passed
        by the user.

        Note that resolving parameter values can be a heavy operation, e.g. if a remote URI is
        passed for a parameter of type `path`, we download the URI to a local path within
        `storage_dir` and substitute in the local path as the parameter value.

        If `storage_dir` is `None`, report path will be return as parameter.
        """
        if user_parameters is None:
            user_parameters = {}
        # Validate params before attempting to resolve parameter values
        self._validate_parameters(user_parameters)
        final_params = {}
        extra_params = {}

        parameter_keys = list(self.parameters.keys())
        for key in parameter_keys:
            param_obj = self.parameters[key]
            key_position = parameter_keys.index(key)
            value = user_parameters[key] if key in user_parameters else self.parameters[key].default
            final_params[key] = param_obj.compute_value(value, storage_dir, key_position)
        for key in user_parameters:
            if key not in final_params:
                extra_params[key] = user_parameters[key]
        return self._sanitize_param_dict(final_params), self._sanitize_param_dict(extra_params)

    def compute_command(self, user_parameters, storage_dir):
        params, extra_params = self.compute_parameters(user_parameters, storage_dir)
        command_with_params = self.command.format(**params)
        command_arr = [command_with_params]
        command_arr.extend(["--%s %s" % (key, value) for key, value in extra_params.items()])
        return " ".join(command_arr)

    @staticmethod
    def _sanitize_param_dict(param_dict):
        return {str(key): shlex_quote(str(value)) for key, value in param_dict.items()}


class Parameter(object):
    """A parameter in an MLproject entry point."""

    def __init__(self, name, yaml_obj):
        self.name = name
        if is_string_type(yaml_obj):
            self.type = yaml_obj
            self.default = None
        else:
            self.type = yaml_obj.get("type", "string")
            self.default = yaml_obj.get("default")

    def _compute_uri_value(self, user_param_value):
        if not data.is_uri(user_param_value):
            raise ExecutionException(
                "Expected URI for parameter %s but got " "%s" % (self.name, user_param_value)
            )
        return user_param_value

    def _compute_path_value(self, user_param_value, storage_dir, key_position):
        local_path = get_local_path_or_none(user_param_value)
        if local_path:
            if not os.path.exists(local_path):
                raise ExecutionException(
                    "Got value %s for parameter %s, but no such file or "
                    "directory was found." % (user_param_value, self.name)
                )
            return os.path.abspath(local_path)
        target_sub_dir = "param_{}".format(key_position)
        download_dir = os.path.join(storage_dir, target_sub_dir)
        os.mkdir(download_dir)
        return artifact_utils._download_artifact_from_uri(
            artifact_uri=user_param_value, output_path=download_dir
        )

    def compute_value(self, param_value, storage_dir, key_position):
        if storage_dir and self.type == "path":
            return self._compute_path_value(param_value, storage_dir, key_position)
        elif self.type == "uri":
            return self._compute_uri_value(param_value)
        else:
            return param_value
