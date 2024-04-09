import common_package.constants as constants
import requests

HOST = "http://10.30.78.48"
TIMEOUT = 1


def get_failure_message(dag_id, deploy_type=constants.DEPLOY_TYPE):
    return f"[AIRFLOW][{deploy_type}] Execute dag_id={dag_id} failure!!!"


def get_function_report_failure(text):
    def report_failure(context=None):
        requests.post(
            f"{HOST}:10300/monitor/api/alert-to-developers",
            headers={"agent": "ki-ki-log-worker"},
            params={"message": text},
            timeout=TIMEOUT,
        )

    return report_failure


def get_function_notification(text):
    def notify(context=None):
        requests.post(
            f"{HOST}:10300/monitor/api/alert-to-developers",
            headers={"agent": "ki-ki-log-worker"},
            params={"message": text},
            timeout=TIMEOUT,
        )

    return notify


if __name__ == "__main__":
    get_function_report_failure("test_code")(None)
