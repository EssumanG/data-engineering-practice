import requests
import pandas as pd
from bs4 import BeautifulSoup
import os


directory = "downloads"

base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"


def create_directory(directory: str):
    try:
        os.makedirs(directory, exist_ok=True)
        print(f'The directory {directory} has been created succesfully!')
    except FileExistsError:
        print(f"Directory {directory} already exist")


def download_file(url: str, dest_path: str|None=None) -> str|None:
    res = requests.get(url)
    if res.status_code == 200:
        file_name = url.split('/')[-1]
        file_path = os.path.join(dest_path, file_name)
        try:
            with open(file_path, 'wb') as file:
                file.write(res.content)
                print(f"File with file name, {file_name} has been downloaded succesfully")
                return file_path        

        except Exception as e:
            print(f"Something happened: {e}")   
            return None    
    else:
        print(f"Failed to download the file, {url}")
        return None
        

def main():
    # web scraping the web page
    try:
        res = requests.get(base_url)
        bs = BeautifulSoup(res.content, 'html.parser')
        table_rows = bs.find_all('tr')
        full_file_url = ""
        local_file_dir = ""

        for row in table_rows:
            if row.find('th'):
                continue
            else:
                column_data = row.find_all("td")
                last_modified_data = column_data[1].text.strip()

                if last_modified_data == '2024-01-19 10:27':
                    file_url = column_data[0].find('a')['href']
                    full_file_url = os.path.join(base_url, file_url)
                    print(last_modified_data, full_file_url)
                    break
    except Exception as e:
        print(f"{e}")

    # download the csv using the url retrieved onto the specific directory (ie download dir, which is created)
    create_directory(directory)
    local_file_dir = download_file(full_file_url, directory)
    print(local_file_dir)

    # using panda to get the highest HourlyDryBulbTemperature from the downloaded file
    df = pd.read_csv(local_file_dir)
    highest_HDBT = df[df['HourlyDryBulbTemperature'] == df['HourlyDryBulbTemperature'].max()]
    print(f"The record with the highest HourlyDryBulbTemperature: \n {highest_HDBT}" )



if __name__ == "__main__":
    main()
