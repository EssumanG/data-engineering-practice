import boto3
import boto3.s3
import os
import gzip, shutil
from pathlib import Path


def create_directory(directory: str):
    try:
        os.makedirs(directory, exist_ok=True)
        print(f'The directory {directory} has been created succesfully!')
    except Exception as error:
        print(f"An error occurred: {error}")
        return None


def s3_download_file(s3, bucket_name: str, key: str, file_path: str):
    try:
        if not Path(file_path).exists():
            s3.download_file(bucket_name, key, file_path)
            print(f'File downloaded: {file_path}')
        else:
            print(f'File already exists: {file_path}')
    except Exception as error:
        print(f'An error occurred: {error}')

def unzip_gz_file(file_path: str, dest_path):
    dest_file_without_gz = os.path.join(dest_path, os.path.splitext(os.path.basename(file_path))[0])
    print(f"Unzipping {file_path} to {dest_path}")
    with gzip.open(file_path) as gf:
        with open(dest_file_without_gz, 'wb') as ff:
           shutil.copyfileobj(gf, ff)
    return dest_file_without_gz

def read_file_line(file_path: str):
    try:
        with open(file_path, 'r') as ff:
            line = ff.readline().strip()
            print(f"First Line: {line}")
            return line
    except FileExistsError as e:
        print(f"File, {file_path} does not exist")
    except Exception as e:
        print(f"Could not open the file, {file_path}")
    return None


def read_file_lines(file_path: str):
    try:
        with open(file_path, 'r', encoding='utf-8') as ff:
            for line in ff:
                print(line.strip())
    except FileNotFoundError as e:
        print(f"File, {file_path} does not exist")
    except Exception as e:
        print(f"Could not open the file, {file_path}: {e}")
    

def main():
    # your code here

    s3 = boto3.client('s3')
    bucket_name = 'commoncrawl'
    key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    local_file = 'wet.paths.gz'
    directory = "downloads"
    create_directory(directory)
    full_path = os.path.join(directory, local_file)
    s3_download_file(s3, bucket_name, key, full_path)
    unzip_file = unzip_gz_file(full_path, directory)

    if unzip_file:
        url_2 = read_file_line(unzip_file)
        if url_2:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=url_2)
            if 'Contents' not in response:
                print(f"Key not found in S3: {url_2}")
            else:    
                local_file_2 = Path(url_2).name
                print(f"local file 2 : {local_file_2}")
                full_path_2 = os.path.join(directory, local_file_2)
                
                print(f'Downloading: {url_2} -> {full_path_2}')
                s3_download_file(s3, bucket_name, url_2, full_path_2)
                
                
                unzip_file_2 = unzip_gz_file(full_path_2, directory)

                if unzip_file_2 and Path(unzip_file_2).exists():
                    print(f"reading file {unzip_file_2}")
                    # read_file_lines(unzip_file_2)
                else:
                    print(f"Unzipped file not found or corrupted: {unzip_file_2}")
                    # read_file_lines(unzip_file_2)



if __name__ == "__main__":
    main()
