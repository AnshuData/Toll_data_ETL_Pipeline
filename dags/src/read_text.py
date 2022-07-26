import os
import pandas as pd 

def extract_data_from_text():
        """
        Parse the text file, read certain column and export it to new csv file
        """

        # Read the raw text file
        txt_df=pd.read_csv("/opt/airflow/dags/data/payment-data.txt", sep=" ", header=None)

        # Read only required data
        txt_data=txt_df.iloc[:, 10:12]

        # Assign new columns label to data
        txt_data.columns=["Type of Payment code", "Vehicle Code"]

        # Export parsed data to csv files        
        txt_data.to_csv("/opt/airflow/dags/data/processed_files/parsed_text_vehicle_data.csv")

        print(txt_data.head())
        print("extract_data_from_txt_file_parsed_and_saved !!!")


