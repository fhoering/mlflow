"""
Utils and internal endpoints to support use of private version control systems

Settings passed in environment variables:
MLFLOW_PRIVATE_VCS_REGEX
MLFLOW_PRIVATE_VCS_REPO_URL
MLFLOW_PRIVATE_VCS_COMMIT_URL
"""
import os

from flask import request, Blueprint


private_vcs = Blueprint("private_vcs", __name__)


@private_vcs.route("/regex", methods=["GET"])
def get_private_vcs_regex():
    """Returns private VCS regex string used for repo URL matching in MLFlow UI

    String will be passed to RegExp() in UI to provide regex for matching.
    """
    vcs_regex = os.getenv("MLFLOW_PRIVATE_VCS_REGEX")
    status = 200 if vcs_regex is not None else 204

    return {"vcs_regex": vcs_regex}, status


@private_vcs.route("/url", methods=["GET"])
def get_private_vcs_url():
    """Generates and returns a private VCS URL pointing to requested source
    """
    vcs_url = None

    if "type" in request.args:
        url_type = request.args["type"].upper()
        vcs_url = os.getenv("MLFLOW_PRIVATE_VCS_{}_URL".format(url_type))

    return {"vcs_url": vcs_url}
