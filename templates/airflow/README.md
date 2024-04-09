# Airflow template



## 1. Quick start
```
# env: conda activate airflow
export AIRFLOW_HOME=~/airflow

airflow webserver --port 8080 -D
airflow scheduler -D
```
### *Or restart after troubleshoot*
```
# env: conda activate airflow
export AIRFLOW_HOME=~/airflow
cat $AIRFLOW_HOME/airflow-webserver.pid | xargs kill -9
cat $AIRFLOW_HOME/airflow-scheduler.pid | xargs kill -9
rm $AIRFLOW_HOME/*.pid

# (WARNING) clean all airflow process
# ps aux | grep 'airflow'
## select and kill only process airflow (not env named airflow_py38 on 78.101)

# rerun
airflow webserver --port 8080 -D
airflow scheduler -D
```

*Currently, Airflow is being deployed on 78.48 for production and 78.101 for staging. On production install to run Updater dags to update data for services. Staging also includes data processing tasks for PO.*

## 2. Debug
```
Error logs at:
${AIRFLOW_HOME}/airflow-scheduler.err
${AIRFLOW_HOME}/airflow-webserver.err
```

## 3. Maintenance dags ([here](https://github.com/teamclairvoyant/airflow-maintenance-dags))

## 4. Best practices

### 4.1  A dag should have less than 10 tasks