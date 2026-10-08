# Workshop 002 - Automatización de ETL

## Integrantes

Valentina Velasco Mera

## Descripción

En este proyecto se desarrolló un pipeline ETL utilizando Apache Airflow para integrar información de Spotify y los premios Grammy.

El proceso incluye la extracción de los datos, validación de calidad, transformación y limpieza, integración mediante el nombre del artista, carga de los resultados en SQLite y generación de un archivo CSV.

Finalmente, se realizó un dashboard utilizando la información almacenada en la base de datos.

## Fuentes de datos

- Spotify Tracks Dataset
- Grammy Awards Dataset

## Pipeline

Spotify CSV (114.000 registros)
↓
Grammys DB (4.810 registros)
↓
Data Quality
↓
Transformación y limpieza
↓
Merge por artista
↓
Dataset integrado (23.944 registros)
↓
SQLite + merged_dataset.csv
↓
Dashboard

## Tareas del DAG

El DAG `workshop_002` está compuesto por las siguientes tareas:

- `read_spotify`
- `read_grammys`
- `data_quality`
- `transform_merge`
- `load_database`
- `export_csv`

La ejecución final del DAG terminó correctamente con estado exitoso.

## Resultados

- Registros Spotify: 114.000
- Registros Grammys: 4.810
- Registros del dataset integrado: 23.944
- Columnas del dataset integrado: 14
- Base de datos: SQLite
- Archivo de salida: `merged_dataset.csv`

## Dashboard

El dashboard se construyó utilizando como fuente la información almacenada en la base de datos SQLite.
