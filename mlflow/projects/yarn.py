import time
import logging
import skein
import os

from mlflow.exceptions import ExecutionException
from mlflow.projects.submitted_run import SubmittedRun
from mlflow.entities import RunStatus

_logger = logging.getLogger(__name__)

# All tags prefixed mlflow.* don't appear in the UI
# they are available in the DB though.
MLFLOW_YARN_APPLICATION_ID = "mlflow.yarn.application_id"
YARN_APPLICATION_ID = "yarn_application_id"

# Configuration parameter names
YARN_NUM_CORES = 'num_cores'
YARN_MEMORY = 'memory'
YARN_QUEUE = 'queue'
YARN_HADOOP_FILESYSTEMS = 'hadoop_filesystems'
YARN_HADOOD_CONF_DIR = 'hadoop_conf_dir'
# Extra parameters to configure skein setup
YARN_ENV = 'env'
YARN_ADDITIONAL_FILES = 'additional_files'

# default values for YARN related parameters
yarn_cfg_defaults = {
    YARN_NUM_CORES: 1,
    YARN_MEMORY: "1 GiB",
    YARN_QUEUE: "defautl",
    YARN_HADOOP_FILESYSTEMS: '',
    YARN_HADOOD_CONF_DIR: '',
    YARN_ENV: {},
    YARN_ADDITIONAL_FILES: []
}


def run_yarn_job(remote_run, uri, entry_point_obj, final_params, extra_params,
                 experiment_id, pex_env, backend_config=None):

    env_params = _get_key_from_params(extra_params, "env", remove_key=True)
    additional_files = _get_key_from_params(extra_params, "additional_files", remove_key=True)
    additional_files.append(pex_env)

    params = final_params.copy()
    params.update(extra_params)
    command = entry_point_obj.compute_command(params, storage_dir=None)

    command_parts = command.split()

    kind_of_script = command_parts.pop(0)
    if 'python' != kind_of_script:
        raise ExecutionException("Running on YARN backend supports only python jobs. "
                                 "You are using '%s'" % (kind_of_script))

    module_name = command_parts.pop(0)
    args = " ".join(command_parts)
    complete_module_name = os.path.join(uri, module_name)

    yarn_config = _parse_yarn_config(backend_config, extra_params=extra_params)

    env = _merge_env_lists(env_params, yarn_config[YARN_ENV])
    additional_files += yarn_config[YARN_ADDITIONAL_FILES]

    _logger.info("run = %s , uri = %s, command = %s , experiment_id = %s",
                 remote_run, uri, command, experiment_id)

    with skein.Client() as skein_client:
        app_id = _submit(
            skein_client=skein_client,
            module_name=complete_module_name,
            args=args,
            name="MLflow run for experiment {}".format(experiment_id),
            num_cores=yarn_config[YARN_NUM_CORES],
            memory=yarn_config[YARN_MEMORY],
            queue=yarn_config[YARN_QUEUE],
            hadoop_file_systems=yarn_config[YARN_HADOOP_FILESYSTEMS].split(','),
            hadoop_conf_dir=yarn_config[YARN_HADOOD_CONF_DIR],
            env_vars=env,
            additional_files=additional_files,
            pex_env=pex_env
        )

        _logger.info("YARN backend launched app_id : %s", app_id)
        return YarnSubmittedRun(skein_app_id=app_id, mlflow_run_id=remote_run.info.run_id)

    raise ExecutionException("Not able to launch your job to YARN.")


def _submit(skein_client, module_name, args=None, name="yarn_launcher",
            num_cores=1, memory="1 GiB", pex_env=None,
            hadoop_file_systems=None, hadoop_conf_dir="", queue=None, env_vars=None,
            additional_files=None, node_label=None, num_containers=1,
            user=None):

    env = dict(env_vars) if env_vars else dict()
    env.update(
        {
            'SKEIN_CONFIG': './.skein',
            'PEX_ROOT': "./.pex",
            "PYTHONPATH": ".",
            "GIT_PYTHON_REFRESH": "quiet"
        }
    )

    dict_files_to_upload = {os.path.basename(path): os.path.abspath(path)
                            for path in additional_files}

    python_bin = f"./{os.path.basename(pex_env)}" if pex_env.endswith(
        '.pex') else f"./{os.path.basename(pex_env)}/bin/python"

    launch_options = "-m" if not module_name.endswith(".py") else ""
    launch_args = args if args else ""
    _logger.info("""
                    set -x
                    env
                    export HADOOP_CONF_DIR=%s
                    %s %s %s %s
                """, hadoop_conf_dir, python_bin, launch_options, module_name, launch_args)

    _logger.info("ENV DICT = %s", env)
    _logger.info("ADDITIONAL FILES = %s", dict_files_to_upload)

    service = skein.Service(
        resources=skein.model.Resources(memory, num_cores),
        instances=num_containers,
        files=dict_files_to_upload,
        env=env,
        script="""
                    set -x
                    env
                    export HADOOP_CONF_DIR=%s
                    %s %s %s %s
                """ % (hadoop_conf_dir, python_bin, launch_options, module_name, launch_args)
    )

    spec = skein.ApplicationSpec(
        name=name,
        file_systems=hadoop_file_systems,
        services={name: service},
        acls=skein.model.ACLs(
            enable=True,
            ui_users=['*'],
            view_users=['*']
        )
    )

    if user:
        spec.user = user

    if queue:
        spec.queue = queue

    if node_label:
        service.node_label = node_label

    return skein_client.submit(spec)


def _merge_env_lists(env_params, env_yarn_cfg):
    env = dict(value.split('=') for value in env_params)
    env.update(dict(value.split('=') for value in env_yarn_cfg))
    return env


def _get_key_from_params(params, key, remove_key=True):
    if key not in params:
        return []

    values = params[key].split(',')
    if remove_key:
        del params[key]

    return values


def _parse_yarn_config(backend_config, extra_params={}):
    """
    Parses configuration for yarn backend and returns a dictionary
    with all needed values. In case values are not found in ``backend_config``
    dict passed, it is filled with the default values.
    """

    if not backend_config:
        raise ExecutionException("Backend_config file not found.")
    yarn_config = backend_config.copy()

    for cfg_key in [YARN_NUM_CORES, YARN_MEMORY, YARN_QUEUE,
                    YARN_HADOOP_FILESYSTEMS, YARN_HADOOD_CONF_DIR,
                    YARN_ENV, YARN_ADDITIONAL_FILES]:
        yarn_config[cfg_key] = extra_params.get(YARN_NUM_CORES, yarn_cfg_defaults[cfg_key])
    return yarn_config


def _format_app_report(report):
    attrs = [
        "queue",
        "start_time",
        "finish_time",
        "final_status",
        "tracking_url",
        "user"
    ]
    return os.linesep + os.linesep.join(
        f"{attr:>16}: {getattr(report, attr) or ''}" for attr in attrs)


def _get_application_logs(skein_client, app_id, wait_for_nb_logs=None, log_tries=15):
    for ind in range(log_tries):
        try:
            logs = skein_client.application_logs(app_id)
            nb_keys = len(logs.keys())
            _logger.info(f"Got {nb_keys}/{wait_for_nb_logs} log files")
            if not wait_for_nb_logs or nb_keys == wait_for_nb_logs:
                return logs
        except Exception:
            _logger.warning("Cannot collect logs (attempt %s/%s)",
                            ind+1, log_tries)
        time.sleep(3)
    return None


class YarnSubmittedRun(SubmittedRun):
    """
    Documentation goes here
    """

    POLL_STATUS_INTERNAL_SECS = 30

    def __init__(self, skein_app_id, mlflow_run_id):
        super(YarnSubmittedRun, self).__init__()
        self._skein_app_id = skein_app_id
        self._mlflow_run_id = mlflow_run_id

    @property
    def run_id(self):
        return self._mlflow_run_id

    def wait(self):
        status = skein.model.FinalStatus.UNDEFINED
        state = None

        with skein.Client() as skein_client:
            while True:
                app_report = skein_client.application_report(self._skein_app_id)
                if state != app_report.state:
                    _logger.info(_format_app_report(app_report))

                if app_report.final_status == skein.model.FinalStatus.FAILED:
                    _logger.info("YARN Application %s has failed", self._skein_app_id)

                if app_report.final_status != skein.model.FinalStatus.UNDEFINED:
                    break

                state = app_report.state
                time.sleep(self.POLL_STATUS_INTERNAL_SECS)

        return status == skein.model.FinalStatus.SUCCEEDED

    def cancel(self):
        with skein.Client() as skein_client:
            skein_client.kill_application(self._skein_app_id)

    def get_status(self):
        with skein.Client() as skein_client:
            app_report = skein_client.application_report(self._skein_app_id)
            return self._translate_to_runstate(app_report.state)

    def _translate_to_runstate(self, app_state):
        if app_state == skein.model.FinalStatus.SUCCEEDED:
            return RunStatus.FINISHED
        elif app_state == skein.model.FinalStatus.KILLED:
            return RunStatus.KILLEDs
        elif app_state == skein.model.FinalStatus.FAILED:
            return RunStatus.FAILED
        elif app_state == skein.model.FinalStatus.UNDEFINED:
            return RunStatus.RUNNING

        raise ExecutionException("YARN Application %s has invalid status: %s"
                                 % (self._skein_app_id, app_state))
