import json


def check_utf8(data):
    try:
        data.decode('utf-8')
        return True
    except UnicodeDecodeError:
        return False

def parse_access_token(token_file):
    """Parse the provided access token and store its contents

    Returns
    -------
    access_token: str
    headers: dict
    environment_urls: dict
    """

    INPUT = open(token_file)
    token_payload = json.load(INPUT)
    access_token = token_payload['access_token']
    headers = {'Authorization': 'Bearer ' + access_token, 'Content-Type': 'application/json'}
    environment_urls = {
        "dev": {
            "WORLD_URL": "https://portal.dev.gpas.ox.ac.uk",
            "API_PATH": "/ords/gpasdevpdb1/gpas_pub/gpasapi",
            "ORDS_PATH": "/ords/gpasdevpdb1/grep/electron",
            "DASHBOARD_PATH": "/ords/gpasdevpdb1/gpas/r/gpas-portal/lineages-voc",
            "ENV_NAME": "DEV"
        },
        "prod": {
            "WORLD_URL": "https://portal.gpas.ox.ac.uk",
            "API_PATH": "/ords/gpas_pub/gpasapi",
            "ORDS_PATH": "/ords/grep/electron",
            "DASHBOARD_PATH": "/ords/gpas/r/gpas-portal/lineages-voc",
            "ENV_NAME": ""
        },
        "staging": {
            "WORLD_URL": "https://portal.staging.gpas.ox.ac.uk",
            "API_PATH": "/ords/gpasuat/gpas_pub/gpasapi",
            "ORDS_PATH": "/ords/gpasuat/grep/electron",
            "DASHBOARD_PATH": "/ords/gpas/r/gpas-portal/lineages-voc",
            "ENV_NAME": "STAGE"
        },
        "atp-test": {
            "WORLD_URL": "https://admin-adb.dev.gpas.ox.ac.uk",
            "API_PATH": ":9000/ords/gpas_pub/gpasapi",
            "ORDS_PATH": ":9000/ords/grep/electron",
            "DASHBOARD_PATH": ":9000/ords/r/gpas/gpas-portal/lineages-voc",
            "ENV_NAME": "ATP-TEST"
        }
    }
    return(access_token, headers, environment_urls)
