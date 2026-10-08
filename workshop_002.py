from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import sqlite3
import pandera as pa

SPOTIFY_PATH = "/mnt/c/Users/velas/Documents/Workshop 002/dataset.csv"
DB_PATH = "/mnt/c/Users/velas/Documents/Workshop 002/workshop.db"
OUTPUT_PATH = "/mnt/c/Users/velas/Documents/Workshop 002/merged_dataset.csv"

@dag(
    dag_id="workshop_002",
    start_date=datetime(2026, 10, 7),
    schedule=None,
    catchup=False
)
def workshop_002():

    @task
    def read_spotify():
        spotify = pd.read_csv(SPOTIFY_PATH)
        print("Spotify:", spotify.shape)


    @task
    def read_grammys():
        con = sqlite3.connect(DB_PATH)
        grammys = pd.read_sql("SELECT * FROM grammys", con)
        con.close()
        print("Grammys:", grammys.shape)

    @task
    def data_quality():
        print("Iniciando validación de calidad de datos...")

        spotify = pd.read_csv(SPOTIFY_PATH)

        con = sqlite3.connect(DB_PATH)
        grammys = pd.read_sql("SELECT * FROM grammys", con)
        con.close()

        spotify_ok = (
            spotify["track_id"].notna().all()
            and spotify["popularity"].between(0, 100).all()
            and spotify["danceability"].between(0, 1).all()
            and spotify["energy"].between(0, 1).all()
        )

        grammys_ok = (
            grammys["year"].notna().all()
            and grammys["category"].notna().all()
        )

        print("Spotify - registros:", len(spotify))
        print("Grammys - registros:", len(grammys))
        print("Spotify - valores nulos:", spotify.isnull().sum().sum())
        print("Grammys - valores nulos:", grammys.isnull().sum().sum())

        if spotify_ok and grammys_ok:
            print("DATA QUALITY: PASS")
        else:
            raise ValueError("DATA QUALITY: FAIL")
    @task
    def transform_merge():
        spotify = pd.read_csv(SPOTIFY_PATH)

        con = sqlite3.connect(DB_PATH)
        grammys = pd.read_sql("SELECT * FROM grammys", con)
        con.close()

        spotify["artist_clean"] = spotify["artists"].str.strip().str.lower()
        grammys["artist_clean"] = grammys["artist"].str.strip().str.lower()

        spotify = spotify.dropna(subset=["artist_clean"])
        grammys = grammys.dropna(subset=["artist_clean"])

        spotify_selected = spotify[
            [
                "track_id",
                "artists",
                "track_name",
                "popularity",
                "danceability",
                "energy",
                "tempo",
                "track_genre",
                "artist_clean"
            ]
        ]

        grammys_selected = grammys[
            [
                "year",
                "category",
                "nominee",
                "artist",
                "winner",
                "artist_clean"
            ]
        ]

        merged = spotify_selected.merge(
            grammys_selected,
            on="artist_clean",
            how="inner"
        )

        print("Resultado del merge:", merged.shape)

        merged.to_csv(OUTPUT_PATH, index=False)

    @task
    def load_database():
        merged = pd.read_csv(OUTPUT_PATH)

        con = sqlite3.connect(DB_PATH)

        merged.to_sql(
            "merged_data",
            con,
            if_exists="replace",
            index=False
        )

        con.close()

        print("Datos cargados en la tabla merged_data:", len(merged))

    @task
    def export_csv():
        merged = pd.read_csv(OUTPUT_PATH)
        merged.to_csv(OUTPUT_PATH, index=False)
        print("CSV exportado correctamente:", OUTPUT_PATH)

    spotify_task = read_spotify()
    grammys_task = read_grammys()
    quality_task = data_quality()
    merge_task = transform_merge()
    database_task = load_database()
    csv_task = export_csv()

    [spotify_task, grammys_task] >> quality_task
    quality_task >> merge_task >> database_task >> csv_task


workshop_002()
