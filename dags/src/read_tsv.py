import os
import pandas as pd 


def extract_data_from_tsv():
        """
        Parse the tsv file, drop the date and export it to new csv file
        """

        # read raw tsv file
        tsv_df = pd.read_csv("/opt/airflow/dags/data/tollplaza-data.tsv", sep="\t", header=None)

        # Read only required column
        tsv_data=tsv_df.iloc[:, [2, 5, 6]]

        # Assign new columns label to data
        tsv_data.columns=["Number_of_axles", "Tollplaza_id","Tollplaza_code"]

        # Export cleaned data to csv file
        tsv_data.to_csv("/opt/airflow/dags/data/processed_files/parsed_tsv_vehicle_data.csv")

        print(tsv_data.head())
        print("data_from_tsv_file_parsed_and_saved !!!")

