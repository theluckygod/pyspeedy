import datetime
import gc

import common_package.constants as constants
import common_package.file_utils as file_utils
import common_package.kiki_logs_constants as kiki_logs_constants
import common_package.process_logs_utils as process_logs_utils
from generate_testcase_all import generate_testcase_all
from tqdm import tqdm

if __name__ == "__main__":
    from_date = "2023-08-14"
    to_date = "2023-08-15"
    app = kiki_logs_constants.APP.APP_CARS

    start = datetime.datetime.strptime(from_date, constants.DATE_FORMAT)
    end = datetime.datetime.strptime(to_date, constants.DATE_FORMAT)
    info_lst = {}
    for _ in tqdm(range((end - start).days)):
        date = start.strftime(constants.DATE_FORMAT)
        print(f"extract date: {date}")

        if date == to_date:
            break

        gc.collect()

        data = file_utils.load_logs_data(date)
        data = process_logs_utils.filt_by_app_type(data, app_types=app)

        # path = generate_all_testcase_this_week(date, utils.get_app_name(app))
        path = generate_testcase_all(data, date, process_logs_utils.get_app_name(app))
        # monitoring_by_id_zmp3(date)

        today = datetime.datetime.today().date()
        start = start + datetime.timedelta(1)
