import common_package.constants as constants
import common_package.kiki_db_constants as kiki_db_constants
import mysql.connector
from loguru import logger

TIMEOUT = 5  # 5s


def get_kiki_db_params(deploy_type=constants.DEPLOY_TYPE):
    if deploy_type == "prod":
        return kiki_db_constants.MYSQL_PROD
    elif deploy_type == "stg":
        return kiki_db_constants.MYSQL_STG
    else:
        raise ValueError(f"Unknown deploy type {deploy_type}!")


def get_kmdb_db_params(deploy_type=constants.DEPLOY_TYPE):
    if deploy_type == "prod":
        return kiki_db_constants.KMDB_POSTGRES_PROD
    else:
        raise ValueError(f"Unknown deploy type {deploy_type}!")


def get_es_db_params(deploy_type=constants.DEPLOY_TYPE):
    if deploy_type == "prod":
        return kiki_db_constants.ELASTIC_SEARCH_PROD
    elif deploy_type == "stg":
        return kiki_db_constants.ELASTIC_SEARCH_STG
    else:
        raise ValueError(f"Unknown deploy type {deploy_type}!")


def get_updater_state(
    date: str, content: str, platform: str, deploy_type=constants.DEPLOY_TYPE
):
    query = f"""
        SELECT `state`
        FROM {kiki_db_constants.UPDATER_STATE_TABLE}
        WHERE date = '{date}' and content = '{content}' and platform = '{platform}';
    """
    logger.info(f"Get query: {query}")
    params = get_kiki_db_params(deploy_type)
    conn = mysql.connector.connect(**params, connection_timeout=TIMEOUT)
    cursor = conn.cursor()
    cursor.execute(query)
    res = cursor.fetchone()

    if cursor:
        cursor.close()
    if conn:
        conn.close()

    return res


def write_updater_state(
    date: str,
    content: str,
    platform: str,
    state: str,
    group=None,
    deploy_type=constants.DEPLOY_TYPE,
):
    query = f"""
        INSERT INTO {kiki_db_constants.UPDATER_STATE_TABLE} (`date`,`content`,`group`,`platform`,`state`) VALUES
            ('{date}','{content}','{group}','{platform}','{state}')
        ON DUPLICATE KEY UPDATE
            state = '{state}';
    """
    logger.info(f"Update query: {query}")
    params = get_kiki_db_params(deploy_type)
    conn = mysql.connector.connect(**params, connection_timeout=TIMEOUT)
    cursor = conn.cursor()
    cursor.execute(query)
    res = cursor.fetchone()
    conn.commit()

    if cursor:
        cursor.close()
    if conn:
        conn.close()

    return res
