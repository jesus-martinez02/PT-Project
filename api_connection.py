import requests
import py7zr
import zipfile
import os


## Function to download 1 day of data
def download_day_data(day:str, api_key:str, real_time_path:str, static_path: str):
    url_real_time = ("https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/sl/TripUpdates?date="
                     + day + '&key=' + api_key)

    url_static = ("https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/sl/GTFSStatic?date="
                     + day + '&key=' + api_key)

    download_file(url_real_time, file_name = real_time_path + '.7z', extract_path = real_time_path, is_zip = False)
    download_file(url_static, file_name = static_path + '.zip', extract_path = static_path, is_zip = True)

## Function to download a file from the API given a URL
def download_file(url,file_name, extract_path,is_zip=False):
    if os.path.exists(file_name): ## Do not call the API if the file to download already exists
        return

    extract_path = "data/" + extract_path

    response = requests.get(url)
    if response.status_code == 200:
        with open(file_name, "wb") as f:
            f.write(response.content)
        print("File downloaded.")

        if is_zip: ## Static data is returned in a zip file while Real Time data is returned in a 7Zip
            with zipfile.ZipFile(file_name, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
        else:
            with py7zr.SevenZipFile(file_name, mode='r') as archive:
                archive.extractall(path=extract_path)
        print(f"File extracted to {extract_path}")

    else:
        print(f"Download failed with status code: {response.status_code}")
