import os
import pandas as pd

def load_data_from_directory(directory):
    # Assuming the directory contains only CSV files
    csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]
    
    # Load the first CSV file as DataFrame
    if csv_files:
        file_path = os.path.join(directory, csv_files[0])
        df = pd.read_csv(file_path)
        return df, file_path
    else:
        raise FileNotFoundError("No CSV files found in the directory.")

def main():
    directory = input("Enter the directory containing data files: ")

    try:
        df, file_path = load_data_from_directory('../'+directory)
        dsdescriptionFile = 'dataset_description.txt'
        data_directory = '../'+directory
        return df, file_path, data_directory
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

