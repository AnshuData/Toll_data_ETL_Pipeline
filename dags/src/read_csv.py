import os
import pandas as pd 

def extract_data_from_csv():
        """
        Parse the csv file, drop the date and export it to new csv file
        """

        # Read raw csv file
        csv_df = pd.read_csv("/opt/airflow/dags/data/vehicle-data.csv", header=None)

        # Read only required data
        csv_data=csv_df.iloc[:, 0:4]

        # Assign new columns label to data
        csv_data.columns=["Rowid", "Timestamp", "Anonymized_Vehicle_number", "Vehicle_type"]

        # Export parsed data to csv files
        csv_data.to_csv("/opt/airflow/dags/data/processed_files/parsed_vehicle_data.csv")

        print(csv_data.head())
        print("data_from_csv_file_parsed_and_saved !!!")

