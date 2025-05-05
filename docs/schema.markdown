# French Road Accident Dataset Schema

## Overview
The French Road Accident dataset (2005–2020) from data.gouv.fr contains accident details in `caracteristiques` CSVs, combined into `accidents_2005_2020_cleaned.csv` (~1.06M rows, 93MB).

## Key Columns
| Column   | Description                          | Type   | Notes                              |
|----------|--------------------------------------|--------|------------------------------------|
| Num_Acc  | Accident ID                          | String | Unique identifier                 |
| an       | Year of accident                    | Int    | 2005–2020                        |
| mois     | Month of accident                   | Int    | 1–12                             |
| jour     | Day of accident                     | Int    | 1–31                             |
| hrmn     | Time of accident (HH:MM)            | String | Requires parsing to datetime      |
| grav     | Severity (1: Uninjured, 4: Fatal)   | Int    | Target for ML prediction          |
| lat      | Latitude                            | Float  | May have missing values           |
| long     | Longitude                           | Float  | May have missing values           |
| dep      | Department code                     | String | e.g., '75' for Paris              |
| atm      | Weather conditions                  | Int    | Categorical (e.g., 1: Normal)     |

## Notes
- **Encoding**: Latin-1 (`latin1`) due to French characters.
- **Separator**: Semicolon (`;`).
- **Partitioning**: Partition by `an` (year) and `dep` (department) for storage.
- **Cleaning Needs**: Handle missing `lat`/`long`, parse `hrmn` to datetime, encode `atm` categories.
- **ML Features**: `lat`, `long`, `hrmn`, `atm`, `dep` for severity prediction.
- **Size**: ~1.06M rows, 93MB, suitable for Big Data pipeline.
- **Issues**: Removed one problematic row (line 958469) due to unescaped commas in address field.

## Source
- [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2022/)