import pysftp
import os

MOSDAC_HOST = 'download.mosdac.gov.in'
MOSDAC_PORT = 22
USERNAME = 'swapna_0'  
PASSWORD = 'swapna65@K' 
REQUEST_ID = 'Jul25_137795'      
LOCAL_DOWNLOAD_DIR = 'mosdac_swapna'  

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None  

with pysftp.Connection(host=MOSDAC_HOST, username=USERNAME, password=PASSWORD, port=MOSDAC_PORT, cnopts=cnopts) as sftp:
    print("Connected to MOSDAC SFTP server.")
    request_folder = f"/Order"

    if sftp.exists(request_folder):
        print(f"Found folder: {request_folder}")
        sftp.cwd(request_folder)
        subfolders = [f for f in sftp.listdir() if sftp.isdir(f)]

        if not subfolders:
            print("No subfolders found in the /Order directory.")
        else:
            subfolders.sort()
            last_folder = subfolders[-1]
            print(f"Last folder found: {last_folder}")
            
            sftp.cwd(last_folder)

            if sftp.exists("images") and sftp.isdir("images"):
                sftp.cwd("images")
                os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)
                files = sftp.listdir()
                print(f"Total files in 'images': {len(files)}")

                for file in files:
                    if "IR1" in file or "IR2" in file:
                        local_path = os.path.join(LOCAL_DOWNLOAD_DIR, file)
                        try:
                            print(f"Downloading: {file}...")
                            sftp.get(file, local_path)
                            print(f"Successfully downloaded: {file}")
                        except Exception as e:
                            print(f"Failed to download {file}: {e}")
                    else:
                        print(f"Skipping: {file} (does not contain IR1 or IR2)")

                print("Filtered IR1/IR2 files download complete.")
            else:
                print("No 'images' folder found inside the last folder.")
    else:
        print(f"Folder not found: {request_folder}")
