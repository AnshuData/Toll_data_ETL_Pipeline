from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
import pandas as pd 
import os

from src.read_csv import extract_data_from_csv 
from src.read_text import extract_data_from_text
from src.read_tsv import extract_data_from_tsv
from src.sync_data import sync_data_with_source
from src.merge import merge_parsed_csv_files 
from src.transform import transform_data 
from src.move_data_to_s3 import upload_csv_files

"""
Airflow pipeline that sync the data from source, parse them,
merge them togther, perform transformation and save the transformed
file to S3 bucket
"""

default_args={ "owner":"me",
                "start_date":dt.datetime(2022,7,17),
                "retries":2,
                "retries-delay":dt.timedelta(minutes=1)
            }


with DAG("Toll_data_ETL",
        description="ETL_for_toll_data",
        default_args=default_args,
        ) as dag:

    Sync_data_with_source  = PythonOperator(task_id="Sync_data_with_source",
                                   python_callable = sync_data_with_source)

    Read_csv_files  = PythonOperator(task_id = "Read_csv_files",
                           python_callable = extract_data_from_csv)

    Read_tsv_files  = PythonOperator(task_id = "Read_tsv_files",
                           python_callable = extract_data_from_tsv)

    Read_text_files = PythonOperator(task_id = "Read_text_files",
                           python_callable = extract_data_from_text)

    Merge_data      = PythonOperator(task_id = "Merge_data",
                           python_callable = merge_parsed_csv_files)

    Transform_data  = PythonOperator(task_id = "Transform_data",
                           python_callable = transform_data)

    Upload_to_S3    = PythonOperator(task_id = "Upload_data_to_S3",
                           python_callable = upload_csv_files)


Sync_data_with_source >> [Read_csv_files, Read_tsv_files, Read_text_files] >> Merge_data  >> Transform_data >> Upload_to_S3
 
