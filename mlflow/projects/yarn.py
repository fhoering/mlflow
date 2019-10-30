import time
import logging
import skein
import thx
import os

from mlflow.exceptions import ExecutionException
from mlflow.projects.submitted_run import SubmittedRun
from mlflow.entities import RunStatus

from thx.hadoop import yarn_launcher

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

# All tags prefixed mlflow.* don't appear in the UI
# they are available in the DB though.
MLFLOW_YARN_APPLICATION_ID = "mlflow.yarn.application_id"
YARN_APPLICATION_ID = "yarn_application_id"

# Configuration parameter names
YARN_NUM_CORES = 'num_cores'
YARN_MEMORY = 'memory'
YARN_QUEUE = 'queue'
YARN_ADDITIONAL_FILES = 'additional_files'

def run_yarn_job(remote_run,
    uri,
    entry_point,
    work_dir,
    command,
    experiment_id,
    backend_config=None
):
    _logger.info("uri={}, entry_point={}, work_dir={}, command={}, experiment_id={}".format(
        uri, entry_point, work_dir, command, experiment_id
    ))

    command_parts = command.split()

    kind_of_script = command_parts.pop(0)
    if 'python' != kind_of_script:
        raise ExecutionException("Running on YARN backend supports only python jobs. You are using '%s'"
             % (kind_of_script))
    
    module_name = command_parts.pop(0)
    args = " ".join(command_parts)
    complete_module_name = os.path.join(uri, module_name)

    yarn_config = _parse_yarn_config(backend_config)
    _logger.info("yarn_config = {}".format(yarn_config))

    with skein.Client() as skein_client:
        app_id = yarn_launcher.submit(
            skein_client=skein_client,
            module_name=complete_module_name,
            args=args,
            name="MLflow run for experiment {}".format(experiment_id),
            num_cores=yarn_config[YARN_NUM_CORES],
            memory=yarn_config[YARN_MEMORY],
            queue=yarn_config[YARN_QUEUE],
            env_vars={},
            additional_files=yarn_config[YARN_ADDITIONAL_FILES]
        )

        _logger.info("YARN backend launched app_id : {}".format(app_id))
        return YarnSubmittedRun(skein_app_id=app_id, mlflow_run_id=remote_run.info.run_id)

    raise ExecutionException("Got unsupported execution mode YARsN")


def _parse_yarn_config(backend_config):
    """
    Parses configuration for yarn backend and returns a dictionary
    with all needed values. In case values are not found in original
    dict passed, it is filled with the default values.
    """

    if not backend_config:
        raise ExecutionException("Backend_config file not found.")
    yarn_config = backend_config.copy()

    if YARN_NUM_CORES not in backend_config.keys():
        yarn_config[YARN_NUM_CORES] = 1
    
    if YARN_MEMORY not in backend_config.keys():
        yarn_config[YARN_MEMORY] = "1 GiB"

    if YARN_QUEUE not in backend_config.keys():
        yarn_config[YARN_QUEUE] = "ml"

    if YARN_ADDITIONAL_FILES not in backend_config.keys():
        yarn_config[YARN_ADDITIONAL_FILES] = []

    return yarn_config

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
                    _logger.info("YARN Application {} has failed".format(self._skein_app_id))

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
        
        raise ExecutionException("YARN Application {} has invalid status: {}"
             % (self._skein_app_id, app_state))


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