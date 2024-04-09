import sys

import common_package.db_utils as db_utils
import mysql.connector
from loguru import logger
from termcolor import colored

VALID_DEPLOY_TYPE = ["local", "stg", "prod"]


if __name__ == "__main__":
    """create a table for tracking
    cmd: python table_creation.py schema.sql local 
    """    

    if len(sys.argv) == 3:
        schema_path = sys.argv[1]
        deploy_type = sys.argv[2]

        if deploy_type not in VALID_DEPLOY_TYPE:
            logger.info(
                colored("Unknown deploy type. Deploying with local settings", "yellow"))
            deploy_type = "local"
        else:
            logger.info(
                colored("Deploying with type: " + deploy_type, "yellow"))


        params = db_utils.get_kiki_db_params(deploy_type)
        conn = mysql.connector.connect(**params, connection_timeout=5)
        cursor = conn.cursor(buffered=True)
        db_conn = conn
        db_cursor = cursor
        logger.info(colored("CONNECTED DATABASE SUCCESSFULLY", "blue"))
        with open(schema_path, encoding="utf-8") as f:
            cursor.execute(f.read(), multi=True)
            db_conn.close()
            logger.info(colored("RUN SUCCESSFULLY", "blue"))
    else:
        logger.info(
            colored("Unknown deploy type. Deploying with local settings", "yellow"))
        
