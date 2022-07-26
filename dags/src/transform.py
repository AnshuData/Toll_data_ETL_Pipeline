import os
import pandas as pd 


def transform_data():
    """
    Function to perform transform operation !!
    """

    # Read merged data for transformation operation
    for_transform=pd.read_csv("/opt/airflow/dags/data/processed_files/merged_vehicle_data.csv")

    # Perform transform operation
    for_transform["Vehicle_type"]=for_transform["Vehicle_type"].str.upper()

    # Save transformed data to csv file
    for_transform.to_csv("/opt/airflow/dags/data/processed_files/transformed_vehicle_data.csv")

    print(for_transform.head())
    


