import sys

sys.path.append("/home/zdeploy/airflow/dags")

import common_package.db_utils as db_utils
import mysql.connector
from loguru import logger
from termcolor import colored

VALID_DEPLOY_TYPE = ["local", "stg", "prod"]


if __name__ == "__main__":
    """create a table for tracking
    cmd: python table_creation.py schema.sql local 
    """    
    deploy_type = "prod"

    if deploy_type not in VALID_DEPLOY_TYPE:
        logger.info(
            colored("Unknown deploy type. Deploying with local settings", 'yellow'))
        deploy_type = "local"
    else:
        logger.info(
            colored("Deploying with type: " + deploy_type, 'yellow'))

    params = db_utils.get_kiki_db_params(deploy_type)
    conn = mysql.connector.connect(**params)
    cursor = conn.cursor()
    logger.info(colored("CONNECTED DATABASE SUCCESSFULLY", "blue"))
    cursor.execute("select count(*) from logics_tracking_v2")
    # cursor.execute("select * from logics_tracking_v2 where state = 'FAIL'")
    res = cursor.fetchall()
    print("res", res)
    cursor.close()
    conn.close()
    logger.info(colored("RUN SUCCESSFULLY", "blue"))
else:
    logger.info(
        colored("Unknown deploy type. Deploying with local settings", 'yellow'))
        
