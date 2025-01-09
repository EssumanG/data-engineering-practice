import boto3
import glob
from pathlib import Path
import pandas as pd
import json
base_dir ="data"

re_csv_files_path = "data/**/*.json" 
def main():
    # your code here
    json_files = glob.glob(re_csv_files_path, recursive=True )

    print(f"Found {len(json_files)} JSON files.")
    combined_data = pd.DataFrame()
    for json_file in json_files:
        try:
            
            print(f"Reading file: {json_file}")
            with open(json_file, 'r') as f:
                json_data = f.read()
                
                
                try:
                    flattened_json = pd.json_normalize(eval(json_data))
                    # print(f'\n \n \n Normalized form:\n {flattened_json}')
                    csv_file_name =str(Path(json_file).parent) + "/"+ Path(json_file).stem + ".csv" 

                    flattened_json.to_csv(csv_file_name, index=False)
                    combined_data = pd.concat([combined_data, flattened_json], ignore_index=True)
                    # print(f"Flattened JSON saved to: {csv_file_name}")
                except Exception as error:
                    print(f"An Error occurred: {error}")
        except Exception as error:
            print(f"An Error occurred: {error}")

    print(f"The data combine as on:\n{combined_data}")


if __name__ == "__main__":
    main()
