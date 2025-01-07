import requests
import os
import zipfile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


directory = "downloads"

def create_directory(directory: str):
    try:
        os.makedirs(directory, exist_ok=True)
        print(f'The directory {directory} has been created succesfully!')
    except FileExistsError:
        print(f"Directory {directory} already exist")

def download_file(url: str, dest_path: str|None=None):
    res = requests.get(url)
    if res.status_code == 200:
        file_name = url.split('/')[-1]
        file_path = os.path.join(dest_path, file_name)
        try:
            with open(file_path, 'wb') as file:
                file.write(res.content)
                print(f"File with file name, {file_name} has been downloaded succesfully")
        except:
            pass
        else:
            unzip_file(file_path, directory)
            remove_file(file_path)        
    else:
        print(f"Failed to download the file, {url}")

def unzip_file(file_path: str, dest_path: str | None = None):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            zip_file.extractall(dest_path)
            print(f"succesfully extracted files in {file_path}")
    except Exception as Error:
        print(f"Couldn't extract the zip file with file name, {file_path} \n Exception Error: {Error}")

def remove_file(file_path: str):
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"The file {file_path} has been removed sucessfully")
    else:
        print(f"The file, {file_path} does not exist")


def main():
    #create the directory 'downloads'
    create_directory(directory)

    for url in download_uris:
        download_file(url, directory)


if __name__ == "__main__":
    main()
