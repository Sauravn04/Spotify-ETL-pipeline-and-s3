from spotify_api import SpotifyAPI
import pandas as pd
import logging
from create_db import create_postgresql_database
import os
import psycopg2
from dotenv import load_dotenv
import boto3
import botocore.exceptions
from io import StringIO

load_dotenv()


def extract_spotify_data(client_id, client_secret, user_id):
    spotify_api = SpotifyAPI(client_id, client_secret, user_id)

    track_data = []
    artist_data = []
    album_data = []
    playlist_data = []

    user_item = spotify_api.get_user_data()
    user_name = user_item.get("display_name", "")
    user_dict = {"user_name": user_name, "user_id": user_id}
    user_data = [user_dict]

    playlist_item = spotify_api.get_playlists()
    if playlist_item:
        for item in playlist_item:
            playlist_id = item["id"]
            playlist_name = item["name"]
            playlist_dict = {
                "playlist_id": playlist_id,
                "user_id": user_id,
                "playlist_name": playlist_name,
            }
            playlist_data.append(playlist_dict)

            tracks_item = spotify_api.get_playlist_tracks(playlist_id)
            for i in tracks_item:
                track = i["track"]
                if not track:
                    continue

                track_id = track["id"]
                track_name = track["name"]
                artist = track["artists"][0]
                artist_id = artist.get("id")
                artist_name = artist.get("name")
                album = track["album"]
                album_id = album.get("id")
                album_name = album.get("name")

                track_dict = {
                    "track_id": track_id,
                    "playlist_id": playlist_id,
                    "artist_id": artist_id,
                    "album_id": album_id,
                    "track_name": track_name,
                }
                artist_dict = {"artist_id": artist_id, "artist_name": artist_name}
                album_dict = {"album_id": album_id, "album_name": album_name}

                track_data.append(track_dict)
                artist_data.append(artist_dict)
                album_data.append(album_dict)

            logging.info(f"Playlist {playlist_id} has {len(tracks_item)} tracks.")
    else:
        logging.info("No playlists found.")

    return user_data, playlist_data, track_data, album_data, artist_data


def transform_data(track_data, artist_data, album_data, playlist_data, user_data):
    track_df = pd.DataFrame(track_data).rename(
        columns={
            "track_id": "id",
            "playlist_id": "playlist_id",
            "artist_id": "artist_id",
            "album_id": "album_id",
            "track_name": "name",
        }
    )
    artist_df = pd.DataFrame(artist_data).rename(
        columns={"artist_id": "id", "artist_name": "name"}
    )
    album_df = pd.DataFrame(album_data).rename(
        columns={"album_id": "id", "album_name": "name"}
    )
    playlist_df = pd.DataFrame(playlist_data).rename(
        columns={"playlist_id": "id", "playlist_name": "name"}
    )
    user_df = pd.DataFrame(user_data).rename(
        columns={"user_id": "id", "user_name": "name"}
    )
    return user_df, playlist_df, track_df, album_df, artist_df


def check_duplicates_and_missing_values(*dfs):
    for df, name in zip(dfs, ["user", "playlist", "track", "album", "artist"]):
        if df.duplicated().any():
            logging.warning(f"Duplicate {name} records found. Removing duplicates.")
            df.drop_duplicates(inplace=True)
        if df.isnull().values.any():
            logging.warning(f"Missing values detected in {name}. Please investigate.")


def load_data_to_postgresql(user_df, playlist_df, track_df, album_df, artist_df):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )
        cursor = conn.cursor()

        def insert_df(df, table):
            for _, row in df.iterrows():
                columns = ", ".join(row.index)
                values = ", ".join(["%s"] * len(row))
                sql = f'INSERT INTO "{table}" ({columns}) VALUES ({values}) ON CONFLICT (id) DO NOTHING'
                cursor.execute(sql, tuple(row))

        insert_df(user_df, "user")
        insert_df(playlist_df, "playlist")
        insert_df(artist_df, "artist")
        insert_df(album_df, "album")
        insert_df(track_df, "track")

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("✅ Data loaded to PostgreSQL successfully.")
    except Exception as e:
        logging.error(f"❌ Error inserting data into PostgreSQL: {e}", exc_info=True)

    os.makedirs("data", exist_ok=True)
    user_df.to_csv("data/user.csv", index=False)
    playlist_df.to_csv("data/playlist.csv", index=False)
    track_df.to_csv("data/track.csv", index=False)
    album_df.to_csv("data/album.csv", index=False)
    artist_df.to_csv("data/artist.csv", index=False)
    logging.info("✅ DataFrames saved as CSV files in the data/ folder.")

def upload_to_s3(
    df,
    key,
    folder_name="",
    aws_access_key=None,
    aws_secret_key=None,
    aws_region=None,
    aws_s3_bucket_name=None,
):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_client = boto3.client(
            service_name="s3",
            region_name=aws_region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )

        full_key = f"{folder_name}/{key}" if folder_name else key
        s3_client.put_object(
            Body=csv_buffer.getvalue(), Bucket=aws_s3_bucket_name, Key=full_key
        )
        logging.info(f"✅ Uploaded {key} to S3 bucket: {aws_s3_bucket_name}")
    except Exception as e:
        logging.error(f"❌ Error uploading {key} to S3: {e}", exc_info=True)


def etl_pipeline():
    try:
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        user_id = os.getenv("SPOTIFY_USER_ID")

        aws_access_key = os.getenv("AWS_ACCESS_KEY")
        aws_secret_key = os.getenv("AWS_SECRET_KEY")
        aws_s3_bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
        aws_region = os.getenv("AWS_REGION")

        user_data, playlist_data, track_data, album_data, artist_data = (
            extract_spotify_data(client_id, client_secret, user_id)
        )

        user_df, playlist_df, track_df, album_df, artist_df = transform_data(
            track_data, artist_data, album_data, playlist_data, user_data
        )

        check_duplicates_and_missing_values(
            user_df, playlist_df, track_df, album_df, artist_df
        )

        # Create PostgreSQL tables
        create_postgresql_database()

        # Load to PostgreSQL
        load_data_to_postgresql(user_df, playlist_df, track_df, album_df, artist_df)

        # Optional S3 upload
        if aws_access_key and aws_secret_key and aws_s3_bucket_name:
            for df, name in [
                (user_df, "user.csv"),
                (playlist_df, "playlist.csv"),
                (track_df, "track.csv"),
                (album_df, "album.csv"),
                (artist_df, "artist.csv"),
            ]:
                upload_to_s3(
                    df,
                    name,
                    folder_name="csv-files",
                    aws_access_key=aws_access_key,
                    aws_secret_key=aws_secret_key,
                    aws_region=aws_region,
                    aws_s3_bucket_name=aws_s3_bucket_name,
                )

    except Exception as e:
        logging.error(f"❌ ETL pipeline failed: {e}", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    etl_pipeline()
