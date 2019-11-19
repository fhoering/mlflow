"""Utils and internal endpoints for usage of private version control systems
"""

import os

from mlflow.server import app


@app.route('/private_vcs_host', methods=['GET'])
def get_private_vcs_host():
    """Returns private VCS URI set in PRIVATE_VCS_HOST env var
    """
    vcs_host = os.getenv('PRIVATE_VCS_HOST')
    status = 200 if vcs_host is not None else 204

    return {'private_vcs_host': vcs_host}, status


@app.route('/private_vcs_regex', methods=['GET'])
def get_private_vcs_regex():
    """Returns private VCS regex string used for repo URL generation in MLFlow UI

    At this stage vcs_regex is a string representation to be passed to JS RegExp(),
    requiring escaping of special characters.
    """
    vcs_regex = os.getenv('PRIVATE_VCS_REGEX')
    status = 200 if vcs_regex is not None else 204

    return {'private_vcs_regex': vcs_regex}, status
