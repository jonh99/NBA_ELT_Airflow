# NBA_ELT_Airflow
Airflow ELT for Cloud Composer (Apache Airflow)

This is an Apache Airflow DAG that loads a csv file every morning into BigQuery, performs a series of SQL transformations on it, appends the transformed rows to the data warehouse, deletes the csv file from Cloud Storage and the two staging tables from BigQuery. 
![dag model](https://github.com/jonh99/NBA_ELT_Airflow/blob/main/IMG_0715.jpg)
