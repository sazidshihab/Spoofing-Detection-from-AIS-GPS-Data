# 🚢 GPS Spoofing Detection with AIS Data and Parallel Computing

A complete end-to-end project to detect GPS spoofing in maritime Automatic Identification System (AIS) data. This project processes real-world ship trajectory logs (~19M rows) to identify anomalous behavior using both sequential and parallel (multiprocessing) approaches, followed by interactive visualization using Folium.

## 🔍 Features

- **Data Preprocessing**:
  - Loaded and merged 30 CSV files (~19 million rows).
  - Cleaned, filtered, and sorted AIS data by ship MMSI and timestamp.
  
- **Anomaly Detection Modules**:
  - **Location-Based Detection**: Geodesic-based detection of jumps and speed inconsistencies.
  - **Course-Based Detection**: Identifies irregular course-over-ground behavior under different speed conditions.

- **Execution Modes**:
  - **Sequential**: Straightforward implementation for comparison.
  - **Parallel**: Utilized `multiprocessing` to drastically reduce runtime (4–5x faster).

- **Visualization**:
  - Generated interactive maps highlighting detected anomalies per vessel.
  - Maps saved as standalone `.html` files using `Folium`.

## ⚙️ Tech Stack

- `Python`, `pandas`, `geopy`, `folium`, `multiprocessing`, `tabulate`

## 📁 Project Structure

.
├── Data_preprocess.py # Prepares and cleans data
├── Execution.py # CLI to run all modules
├── lon_lat_sequential.py # Sequential location spoofing detection
├── lon_lat_multiprocess.py # Parallel location spoofing detection
├── course_sequential.py # Sequential course anomaly detection
├── course_multiprocess.py # Parallel course anomaly detection
├── course_map.py # Maps for course anomalies
├── lon_lat_map.py # Maps for location anomalies
└── data/ # AIS CSV files

markdown
Copy
Edit

## 🚀 How to Run

1. Place all your AIS `.csv` files in the `data/` directory.
2. Run the main CLI script:

```bash
python Execution.py
Choose from:

"sequential loc" – Location anomaly detection (sequential)

"multi loc" – Location anomaly detection (parallel + map)

"sequential course" – Course anomaly detection (sequential)

"multi course" – Course anomaly detection (parallel + map)

📊 Output
Anomaly summaries printed in terminal

Interactive maps saved as HTML files in categorized folders

📌 Result
Detected hundreds of spoofing events

Reduced runtime drastically with parallel computing

Improved anomaly interpretability with maps

Feel free to fork, clone, and adapt for your own maritime or AIS-related research.
