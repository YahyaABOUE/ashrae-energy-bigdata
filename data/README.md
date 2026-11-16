# Data

The dataset is not included in this repository (~700 MB).

Download it from Kaggle: [ASHRAE – Great Energy Predictor III](https://www.kaggle.com/c/ashrae-energy-prediction/data)

Files used:

| File | Rows | Content |
|---|---|---|
| `train.csv` | 20,216,100 | Hourly meter readings (building_id, meter, timestamp, meter_reading) |
| `building_metadata.csv` | 1,449 | Building characteristics (site, primary use, surface, year built, floors) |
| `weather_train.csv` | 139,773 | Hourly weather per site |

Copy them to the HDP sandbox, then run `scripts/load_hdfs.sh`.
