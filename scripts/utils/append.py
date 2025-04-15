import pandas as pd
import os

# Read csv files in a directory and append and save as a new csv file with directory name as file name and delete old csv files on success
def append_csv_files(directory_path):
  csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
  if not csv_files:
    print("No CSV files found in the directory.")
    return
  
  dataframes = []

  for file in csv_files:
    file_path = os.path.join(directory_path, file)
    df = pd.read_csv(file_path)
    dataframes.append(df)

  combined_df = pd.concat(dataframes, ignore_index=True)

  combined_file_name = os.path.basename(directory_path) + '.csv'
  combined_file_path = os.path.join(directory_path, combined_file_name)

  combined_df.to_csv(combined_file_path, index=False)

  for file in csv_files:
    file_path = os.path.join(directory_path, file)
    os.remove(file_path)
    print(f"Deleted {file_path}")

  print(f"Combined CSV file saved as {combined_file_path}")


# append_csv_files("output/orders")
append_csv_files("output/order_items")