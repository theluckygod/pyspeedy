MYSQL_TEST = {
    "host": "localhost",
    "database": "test_db",
    "user": "root",
    "password": "root",
    "port": "3306",
}

MYSQL_STG = {
    "host": "10.30.80.206",
    "database": "ki_ki_dev",
    "user": "ki_ki_dev",
    "password": "BR4jhExEHcpW5Ghy",
    "port": "3309",
}

MYSQL_PROD = {
    "host": "10.30.78.12",
    "database": "ki_ki_dev",
    "user": "ki_ki_dev",
    "password": "BR4jhExEHcpW5Ghy",
    "port": "3309",
}

MYSQL_CRAWLER_PROD = MYSQL_PROD
FAST_MYSQL_CRAWLER_PROD = {
    "host": "10.30.80.206",
    "database": "ki_ki_dev",
    "user": "ki_ki_dev",
    "password": "BR4jhExEHcpW5Ghy",
    "port": "3309",
}
FAST_POSTGRES_CRAWLER_PROD = {
    "host": "10.30.78.60",
    "port": "5432",
    "database": "kiki_production",
    "user": "kiki_production",
    "password": "mijUbreme2e41resPIrl",
}

KMDB_POSTGRES_PROD = {
    "host": "10.30.78.60",
    "dbname": "gazetteer",
    "user": "postgres",
    "password": "postgres",
    "port": "5436",
}

UPDATER_STATE_TABLE = "kiki_updater_state"

MONGODB_YOUTUBE = "mongodb://10.30.78.60:27017"

ELASTIC_SEARCH_PROD = "10.30.78.48:9200"
ELASTIC_SEARCH_STG = "10.30.78.101:9200"
