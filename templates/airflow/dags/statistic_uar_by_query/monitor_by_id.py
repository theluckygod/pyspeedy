import traceback

import common_package.db_utils as db_utils
import mysql.connector
import statistic_uar_by_query.constants as constants
from loguru import logger


def load_whitelist_id(deploy_type="prod"):
    params = db_utils.get_kiki_db_params(deploy_type)

    conn = None
    cursor = None
    whitelist_id = None
    query = """
        SELECT * FROM monitor_whitelist_id;
    """
    try:
        conn = mysql.connector.connect(**params)
        cursor = conn.cursor()
        cursor.execute(query)
        whitelist_id = cursor.fetchall()
    except Exception as e:
        logger.info(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return whitelist_id


def monitoring_by_id(data, date, whitelist_id):
    info_lst = {}

    for id, _type, tracking_type, _ in whitelist_id:
        action = _type + "_" + id
        id_df = data[data["action"] == action]
        if len(id_df) == 0:
            continue

        for intent in id_df["intent"].unique():
            if intent in constants.CONFIRM_OPEN_MP3_INTENTS:
                continue
            intent_df = id_df[id_df["intent"] == intent]
            frequency = sum(intent_df["frequency"])
            duration_num = sum(intent_df["duration_num"])
            accepted_num = sum(intent_df["accepted_num"])
            uar = 0
            if duration_num > 0:
                uar = accepted_num / duration_num
            info = {
                "date": date,
                "id": id,
                "tracking_type": tracking_type,
                "type": _type,
                "name": intent_df.iloc[0]["action_verbose"],
                "uar": uar,
                "frequency": frequency,
                "duration_num": duration_num,
                "accepted_num": accepted_num,
                "intent": intent,
            }

            for key in info.keys():
                lst = info_lst.get(key, [])
                lst.append(info[key])
                info_lst[key] = lst

    if info_lst:
        write_db(info_lst)

    return info_lst


def write_db(info_lst, deploy_type="prod"):
    params = db_utils.get_kiki_db_params(deploy_type)

    conn = None
    cursor = None
    error = None

    query = """
        INSERT INTO monitor_by_id (date, id, type, tracking_type, name, uar, frequency, duration_num, accepted_num, intent) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE tracking_type=VALUES(tracking_type), 
                                uar=VALUES(uar), 
                                frequency=VALUES(frequency), 
                                duration_num=VALUES(duration_num), 
                                accepted_num=VALUES(accepted_num);
    """
    values = list(
        zip(
            info_lst["date"],
            info_lst["id"],
            info_lst["type"],
            info_lst["tracking_type"],
            info_lst["name"],
            info_lst["uar"],
            info_lst["frequency"],
            info_lst["duration_num"],
            info_lst["accepted_num"],
            info_lst["intent"],
        )
    )
    logger.info(f"values: {values}")
    try:
        conn = mysql.connector.connect(**params)
        cursor = conn.cursor()
        cursor.executemany(query, values)
        conn.commit()
        logger.info("Write database sucessfully!!!")
    except Exception as e:
        logger.info(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    if error:
        raise error


if __name__ == "__main__":
    import pandas as pd

    df = pd.read_csv("zing_apps_outputs/testcases_all_nofilter_confuse_2022-10-01.csv")
    #     whitelist_id = load_whitelist_id()
    whitelist_id = [
        ("ZZDI9B7U", "song", "monitor", ""),
        ("ZW8IZECW", "song", "monitor", ""),
        ("ZZC0OBFB", "song", "monitor", ""),
        ("Z60BO68C", "song", "missing_content", ""),
    ]
    print("whitelist_id", whitelist_id)
    info_lst = monitoring_by_id(df, "2022-10-01", whitelist_id)
    info_df = pd.DataFrame()
    for key in info_lst:
        info_df[key] = info_lst[key]
    print(info_df)
