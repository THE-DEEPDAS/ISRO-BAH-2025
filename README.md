We are Team SVNIT Coders, We (Bodhini, Deep and Swapna) participated in the ISRO's BAH 2025 competition and this repo has the dataset you would need for its PS3

We have also mentioned approach in the PPT you can use that but we haven't completed the project due to time limitations.

# Surface-Level PM Estimation Using Satellite AOD and Reanalysis Data

This repository contains the dataset and implementation details for our project on estimating surface-level particulate matter (PM) concentrations using multi-source data including satellite-derived Aerosol Optical Depth (AOD), reanalysis datasets (MERRA-2), and ground-based measurements (CPCB).

## 📂 Project Structure

```
├── dataset/
│   ├── insat_aod/                 # INSAT-3D/3DR/3DS AOD data (30-min temporal resolution)
│   ├── merra2/                    # MERRA-2 reanalysis data (hourly time-averaged .nc4 files)
│   ├── cpcb_pm/                   # Ground truth data (PM2.5, PM10 from CPCB)
├── approach_presentation.pptx     # Full project pipeline & model architecture
├── preprocess/                    # Scripts for data cleaning, temporal/spatial alignment
├── models/                        # Random Forest and other model training scripts
├── evaluation/                    # Model evaluation and PM map generation
├── README.md                      # You are here
```

## 🛰️ Data Sources

- **Satellite AOD:** INSAT-3D/3DR/3DS Level-2 aerosol products
- **Reanalysis Data:** MERRA-2 `M2T1NXSLV.5.12.4` hourly 2D single-level diagnostics
- **Ground Truth PM Data:** CPCB stations across India

## 🔍 Objective

To estimate **high-resolution surface-level PM2.5 and PM10 concentrations** using a combination of:
- Satellite AOD measurements
- MERRA-2 meteorological and chemical variables
- Ground-based PM observations for supervised training

## 📊 Model

We utilize ensemble-based machine learning (e.g., Random Forest) for:
- Feature extraction from satellite + reanalysis data
- Spatial interpolation
- PM2.5 and PM10 estimation across different Indian regions

## 🛠️ Preprocessing Includes:
- Temporal matching of satellite (30-min) and MERRA-2 (hourly) data
- Spatial alignment using nearest neighbor / bilinear interpolation
- Filtering based on cloud cover or low-AOD confidence
- Conversion of `.nc4` to structured tabular features

## 🧪 Evaluation

Model performance is evaluated using:
- MAE, RMSE, R² score
- Spatial PM maps overlaid on regional grids
- Cross-validation with unseen time ranges or locations

## 📁 Dataset Download (MERRA-2)

MERRA-2 hourly data is downloaded using EarthData credentials via scripted URL batch download. All `.nc4` files are processed into model-ready tabular format.

## 🧾 Acknowledgements

- **NASA GES DISC** for MERRA-2 data
- **ISRO MOSDAC** for INSAT AOD
- **CPCB** for ground PM measurements

---

## 📢 Citation

If you use this project, please consider citing or acknowledging the authors and the data providers listed above.

---

## 📎 License

This project is for research use only. Please refer to respective data providers' terms of use for redistribution rights.


## Merging the data
for merging the data main thing is that this .nc4 and .h5 images have coordinates in them

so we need to extract the coordinates from which we have taken the CPCB data and find the AOD and reanlysis data's values for that location and add that values in the merged dataframe