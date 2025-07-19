import pysftp
import os

MOSDAC_HOST = 'download.mosdac.gov.in'
MOSDAC_PORT = 22
USERNAME = 'deep_das'  
PASSWORD = 'BdS@1234' 
REQUEST_ID = 'Jul2025_137485'      
LOCAL_DOWNLOAD_DIR = 'mosdac'  
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None  #

with pysftp.Connection(host=MOSDAC_HOST, username=USERNAME, password=PASSWORD, port=MOSDAC_PORT, cnopts=cnopts) as sftp:
    print("Connected to MOSDAC SFTP server.")
    request_folder = f"/Order"
    
    if sftp.exists(request_folder):
        print(f"Found folder: {request_folder}")
        sftp.cwd(request_folder)
        subfolders = [f for f in sftp.listdir() if sftp.isdir(f)]
        
        if not subfolders:
            print("❌ No subfolders found in the /Order directory.")
        else:
            # Sort subfolders and pick the last one
            subfolders.sort()
            last_folder = subfolders[-1]
            print(f"📂 Last folder found: {last_folder}")
            
            sftp.cwd(last_folder)
            os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)
            files = sftp.listdir()
            print(f"📦 Total files found in {last_folder}: {len(files)}")
            
            for file in files:
                local_path = os.path.join(LOCAL_DOWNLOAD_DIR, file)
                try:
                    print(f"⬇ Downloading: {file}...")
                    sftp.get(file, local_path)
                    print(f"✅ Successfully downloaded: {file}")
                except Exception as e:
                    print(f"❌ Failed to download {file}: {e}")
            
            print("✅ All files processed.")
    else:
        print(f"❌ Folder not found: {request_folder}")
