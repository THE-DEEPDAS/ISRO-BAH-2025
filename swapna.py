import pandas as pd
from PIL import Image
from PIL.ExifTags import TAGS
import os

# Step 1: Extract timestamp from image EXIF
def get_image_timestamp_from_filename(mosdac_swapna):
    try:
        image = Image.open(mosdac_swapna)
        exif_data = image._getexif()
        if not exif_data:
            return None
        for tag_id, value in exif_data.items():
            tag = TAGS.get(tag_id)
            if tag == 'DateTimeOriginal':
                return pd.to_datetime(value, format='%Y:%m:%d %H:%M:%S')
    except Exception as e:
        print(f"Error reading {mosdac_swapna}: {e}")
    return None

# Step 2: Read all image timestamps from folder
image_folder = 'mosdac'  # path to your JPGs
image_data = []


for filename in os.listdir(image_folder):
    if filename.lower().endswith(".jpg"):
        filepath = os.path.join(image_folder, filename)
        timestamp = get_image_timestamp(filepath)
        print(f"📷 {filename} → Timestamp: {timestamp}")  # Debug print
        if timestamp:
            image_data.append({"image_file": filename, "timestamp": timestamp})


images_df = pd.DataFrame(image_data)
print(images_df.head())

locations_df = pd.read_csv("CPCB/location_6981.csv")

# Ensure the timestamp column exists before renaming
if "timestamp" in images_df.columns:
    images_df.rename(columns={"timestamp": "period.datetimeFrom.utc"}, inplace=True)
else:
    print("Error: 'timestamp' column not found in images_df.")
    exit(1)  # Exit the script if the column is missing

# Ensure the locations_df has the correct datetime column and is sorted
locations_df["period.datetimeFrom.utc"] = pd.to_datetime(locations_df["period.datetimeFrom.utc"])
locations_df.sort_values("period.datetimeFrom.utc", inplace=True)

# Ensure images_df is sorted by the timestamp column
images_df.sort_values("period.datetimeFrom.utc", inplace=True)

# Merge images with nearest (previous or next) location timestamp
merged_df = pd.merge_asof(images_df, locations_df, on="period.datetimeFrom.utc", direction='nearest')

# Save the result
merged_df.to_csv("merged_output.csv", index=False)
print("Merged data saved to merged_output.csv")