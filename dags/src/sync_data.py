import os
import pandas as pd 


def sync_data_with_source():
        os.system("curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz --output /opt/airflow/dags/data/tolldata.tgz")
        os.system('tar -xvzf /opt/airflow/dags/data/tolldata.tgz')
        print('Data synced with source')


