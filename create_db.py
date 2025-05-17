import psycopg2
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=dotenv_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_postgresql_database():
    try:
        # Read PostgreSQL connection settings from environment variables
        db_name = os.getenv("POSTGRES_DB")
        db_user = os.getenv("POSTGRES_USER")
        db_password = os.getenv("POSTGRES_PASSWORD")
        db_host = os.getenv("POSTGRES_HOST")
        db_port = os.getenv("POSTGRES_PORT")

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        cursor = conn.cursor()
        logging.info("Connected to PostgreSQL.")

        # Create tables with foreign key constraints
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS "user" (
                id TEXT PRIMARY KEY,
                name TEXT
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS playlist (
                id TEXT PRIMARY KEY,
                user_id TEXT REFERENCES "user"(id),
                name TEXT
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS artist (
                id TEXT PRIMARY KEY,
                name TEXT
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS album (
                id TEXT PRIMARY KEY,
                name TEXT
            );
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS track (
                id TEXT PRIMARY KEY,
                playlist_id TEXT REFERENCES playlist(id),
                artist_id TEXT REFERENCES artist(id),
                album_id TEXT REFERENCES album(id),
                name TEXT
            );
            """
        )

        # Commit the changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("✅ PostgreSQL tables created successfully.")

    except psycopg2.Error as e:
        logging.error(f"❌ Error creating PostgreSQL database: {e}", exc_info=True)


if __name__ == "__main__":
    create_postgresql_database()
