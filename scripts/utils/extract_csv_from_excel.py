import pandas as pd
import os

def get_xlsx_from_dir(dir_path):
    xlsx_files = []
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if file.endswith(".xlsx"):
                xlsx_files.append(os.path.join(root, file))
    return xlsx_files

def excel_to_csv_all_sheets(excel_file, output_dir):
    # Load the Excel file
    xls = pd.ExcelFile(excel_file)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Loop through each sheet
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)

        # Generate a filename-safe version of the sheet name
        safe_sheet_name = "".join(c if c.isalnum() else "_" for c in sheet_name)

        # Build the output file path
        csv_file = os.path.join(output_dir, f"{safe_sheet_name}.csv")

        # Save to CSV
        df.to_csv(csv_file, index=False)
        print(f"Saved: {csv_file}")

xlsx_files = get_xlsx_from_dir("data/")

excel_to_csv_all_sheets(xlsx_files[0], "output/orders/")
excel_to_csv_all_sheets(xlsx_files[1], "output/order_items/")

