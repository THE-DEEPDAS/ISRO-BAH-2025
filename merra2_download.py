import os
import requests
from urllib.parse import urlparse

# === EARTHDATA CREDENTIALS (as you provided)
USERNAME = "deep_das"
PASSWORD = "password daalo idhar"

# === INPUT: text file with MERRA-2 URLs
LINKS_FILE = "subset_M2T1NXSLV_5.12.4_20250720_113518_.txt"
OUTPUT_DIR = "Merra-2"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Setup session with Earthdata login
session = requests.Session()
session.auth = (USERNAME, PASSWORD)
session.headers.update({
    "User-Agent": "merra-downloader/1.0",
    "Accept": "*/*"
})

# === Downloader with redirect and auth handling
def download_file(url):
    filename = os.path.basename(urlparse(url).path)
    output_path = os.path.join(OUTPUT_DIR, filename)

    try:
        # Initial request to check for redirect
        r = session.get(url, allow_redirects=False)
        if r.status_code == 302 and "Location" in r.headers:
            redirected_url = r.headers["Location"]
            session.get(redirected_url, allow_redirects=True)  # Auth via redirect

        # Final download
        with session.get(url, stream=True) as r:
            r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        print(f"✅ Downloaded: {filename}")

    except Exception as e:
        print(f"❌ Failed: {url}\nError: {e}")

# === Main execution
if not os.path.exists(LINKS_FILE):
    print(f"❌ File not found: {LINKS_FILE}")
else:
    with open(LINKS_FILE, "r") as f:
        urls = [line.strip() for line in f if line.strip()]

    if not urls:
        print("❌ No URLs found in file.")
    else:
        print(f"🔗 Starting download of {len(urls)} files...\n")
        for url in urls:
            download_file(url)
