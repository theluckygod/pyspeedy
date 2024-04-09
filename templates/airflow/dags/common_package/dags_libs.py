import datetime

import common_package.constants as constants
import dateutil.parser
from airflow.decorators import task
from loguru import logger


@task.python(provide_context=True)
def determine_date(**kwargs):
    CURRENT_DT = dateutil.parser.parse(str(kwargs["ts"])).astimezone(
        constants.LOCAL_TIMEZONE
    )

    if kwargs["dag_run"].external_trigger:
        # manual run
        if "date" in kwargs["dag_run"].conf and datetime.datetime.strptime(
            kwargs["dag_run"].conf["date"], constants.DATE_FORMAT
        ):
            # run with config
            _YESTERDAY_STR = str(kwargs["dag_run"].conf["date"])
            logger.info(f"Manual run with config execute date {_YESTERDAY_STR}")
        else:
            _YESTERDAY_STR = (CURRENT_DT - datetime.timedelta(1)).strftime(
                constants.DATE_FORMAT
            )
            logger.info(f"Manual run with execute date {_YESTERDAY_STR}")
    else:
        # scheduled run
        _YESTERDAY_STR = CURRENT_DT.strftime(constants.DATE_FORMAT)
        logger.info(f"Scheduled run with execute date {_YESTERDAY_STR}")
    return _YESTERDAY_STR
