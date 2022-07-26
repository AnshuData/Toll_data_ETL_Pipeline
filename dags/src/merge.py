import os
import pandas as pd 


def merge_parsed_csv_files():
        """
        Merge parsed csv files to create a single data file
        """

        # read all csv files
        csv1=pd.read_csv("/opt/airflow/dags/data/processed_files/parsed_vehicle_data.csv")
        csv2=pd.read_csv("/opt/airflow/dags/data/processed_files/parsed_tsv_vehicle_data.csv")
        csv3=pd.read_csv("/opt/airflow/dags/data/processed_files/parsed_text_vehicle_data.csv")

        # Concatenate all dataframes to create single df
        merged_df=pd.concat([csv1,csv2,csv3], axis=1)

        # Export merged data frame to a csv file
        merged_df.to_csv("/opt/airflow/dags/data/processed_files/merged_vehicle_data.csv")

        print(merged_df.head())
        print("Data merged and exported !!!")   


