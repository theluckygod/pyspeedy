import traceback

import mysql.connector
import pandas as pd

params = {
    "host": "10.30.78.12",
    "database": "ki_ki_dev",
    "user": "ki_ki_dev",
    "password": "BR4jhExEHcpW5Ghy",
    "port": "3309",
}


if __name__ == "__main__":
    df = pd.read_csv("whitelist_id.csv", encoding="utf-8", sep=",")

    try:
        conn = mysql.connector.connect(**params, connection_timeout=5)
        cursor = conn.cursor()
        query = """
            INSERT INTO `monitor_whitelist_id` (`id`, `tracking_type`) 
            VALUES (%s, %s);
        """
        query_with_type = """
            INSERT INTO `monitor_whitelist_id` (`id`, `type`, `tracking_type`) 
            VALUES (%s, %s, %s);
        """

        executed_rows = 0
        for idx, row in df.iterrows():
            _id = row.get("id", None)
            _type = row.get("type", None)
            _tracking_type = row.get("tracking_type", None)
            values = []

            if _id is None:
                print("WARNING: Ignore a row! id is empty")
                continue
            values.append(_id)
            if _type != None:
                values.append(_type)
            if _tracking_type is None:
                print("WARNING: Ignore a row! tracking_type is empty")
                continue
            values.append(_tracking_type)

            if _type:
                cursor.execute(query_with_type, values)
            else:
                cursor.execute(query, values)
            executed_rows += 1

        if executed_rows:
            conn.commit()
        print(f"Execute {executed_rows} rows successfully!!!")

    except Exception as e:
        print(traceback.format_exc())
        print("Write db failure!!!")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
