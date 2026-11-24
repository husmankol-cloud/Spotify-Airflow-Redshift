
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator

import pandas as pd
import psycopg
import boto3
from botocore.exceptions import ClientError
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 11, 1, tzinfo=timezone.utc),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# S3 bucket and file paths
BUCKET_NAME = 'spotify-batch-pipeline-784608886546-2026'
SONGS_FILE_PATH = 'spotify-prefix/songs.csv'
USERS_FILE_PATH = 'spotify-prefix/users.csv'
STREAMS_PREFIX = 'spotify-prefix/streams/'
ARCHIVE_PREFIX = 'spotify-prefix/streams/archived/'


# Redshift connection settings
REDSHIFT_REGION = 'us-east-2'
REDSHIFT_WORKGROUP = 'spotify-analytics'
REDSHIFT_DATABASE = 'dev'
REDSHIFT_HOST = 'spotify-analytics.784608886546.us-east-2.redshift-serverless.amazonaws.com'
REDSHIFT_PORT = 5439


REQUIRED_COLUMNS = {
    'songs': [
        'id', 'track_id', 'artists', 'album_name', 'track_name',
        'popularity', 'duration_ms', 'explicit', 'danceability',
        'energy', 'key', 'loudness', 'mode', 'speechiness',
        'acousticness', 'instrumentalness', 'liveness', 'valence',
        'tempo', 'time_signature', 'track_genre'
    ],
    'streams': ['user_id', 'track_id', 'listen_time'],
    'users': [
        'user_id', 'user_name', 'user_age',
        'user_country', 'created_at'
    ]
}


def list_s3_files(prefix, bucket=BUCKET_NAME):
    """List CSV files in the S3 bucket matching the supplied prefix."""

    s3 = boto3.client('s3')

    try:
        paginator = s3.get_paginator('list_objects_v2')

        files = sorted(
            item['Key']
            for page in paginator.paginate(
                Bucket=bucket,
                Prefix=prefix
            )
            for item in page.get('Contents', [])
            if item['Key'].endswith('.csv')
            and not item['Key'].startswith(ARCHIVE_PREFIX)
        )

        logging.info(
            f"Successfully listed files with prefix {prefix}: {files}"
        )

        return files

    except Exception as e:
        logging.error(
            f"Failed to list files from S3: {str(e)}"
        )
        raise


def read_s3_csv(file_name, bucket=BUCKET_NAME):
    """Read a CSV file directly from S3."""

    s3 = boto3.client('s3')

    try:
        obj = s3.get_object(
            Bucket=bucket,
            Key=file_name
        )

        logging.info(f"Successfully read {file_name} from S3")

        return pd.read_csv(obj['Body'])

    except Exception as e:
        logging.error(
            f"Failed to read {file_name} from S3: {str(e)}"
        )
        raise


def validate_datasets(ti):
    """Validate songs, users, and stream datasets."""

    validation_results = {
        'songs': False,
        'users': False,
        'streams': False
    }

    # Validate songs dataset
    try:
        songs_data = read_s3_csv(SONGS_FILE_PATH)

        missing_columns = (
            set(REQUIRED_COLUMNS['songs'])
            - set(songs_data.columns)
        )

        if not missing_columns:
            validation_results['songs'] = True
            logging.info("All required columns present in songs")
        else:
            logging.warning(
                f"Missing columns in songs: {missing_columns}"
            )

    except Exception as e:
        logging.error(f"Failed to validate songs: {e}")
        raise

    # Validate users dataset
    try:
        users_data = read_s3_csv(USERS_FILE_PATH)

        missing_columns = (
            set(REQUIRED_COLUMNS['users'])
            - set(users_data.columns)
        )

        if not missing_columns:
            validation_results['users'] = True
            logging.info("All required columns present in users")
        else:
            logging.warning(
                f"Missing columns in users: {missing_columns}"
            )

    except Exception as e:
        logging.error(f"Failed to validate users: {e}")
        raise

    # Validate stream datasets
    try:
        stream_files = list_s3_files(STREAMS_PREFIX)

        ti.xcom_push(
            key='stream_files',
            value=stream_files
        )

        if not stream_files:
            logging.info("No input stream files found")
        else:
            validation_results['streams'] = True

        for stream_file in stream_files:
            streams_data = read_s3_csv(stream_file)

            missing_columns = (
                set(REQUIRED_COLUMNS['streams'])
                - set(streams_data.columns)
            )

            if missing_columns:
                validation_results['streams'] = False

                logging.warning(
                    f"Missing columns in {stream_file}: "
                    f"{missing_columns}"
                )

                break

            logging.info(
                f"All required columns present in {stream_file}"
            )

    except Exception as e:
        logging.error(f"Failed to validate streams: {e}")
        raise

    return validation_results


def branch_task(ti):
    """Choose whether to calculate KPIs or end the DAG."""

    validation_results = ti.xcom_pull(
        task_ids='validate_datasets'
    )

    if all(validation_results.values()):
        return 'calculate_genre_level_kpis'

    return 'end_dag'


def upsert_to_redshift(df, table_name, id_columns):
    """
    Upload KPI data to Redshift using temporary IAM credentials.

    Data is first loaded into a staging table.
    Matching records in the permanent table are deleted.
    New records are then inserted into the permanent table.
    """

    conn = None
    cursor = None

    try:
        # Request temporary Redshift credentials from AWS
        redshift_client = boto3.client(
            'redshift-serverless',
            region_name=REDSHIFT_REGION
        )

        credentials = redshift_client.get_credentials(
            workgroupName=REDSHIFT_WORKGROUP,
            dbName=REDSHIFT_DATABASE
        )

        # Connect securely to Redshift
        conn = psycopg.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            dbname=REDSHIFT_DATABASE,
            user=credentials['dbUser'],
            password=credentials['dbPassword'],
            sslmode='require',
            connect_timeout=15
        )

        cursor = conn.cursor()

        logging.info(
            "Connected to Redshift using temporary IAM credentials"
        )

        # Convert DataFrame rows into database-ready Python values
        data_tuples = [
            tuple(
                None if pd.isna(value)
                else value.item() if hasattr(value, 'item')
                else value
                for value in row
            )
            for row in df.itertuples(index=False, name=None)
        ]

        # Prepare column names and placeholders
        cols = ', '.join(df.columns)

        vals = ', '.join(
            ['%s'] * len(df.columns)
        )

        # Clear the temporary staging table
        cursor.execute(
            f"DELETE FROM reporting_schema.tmp_{table_name}"
        )

        # Insert records into the staging table
        tmp_table_query = (
            f"INSERT INTO reporting_schema.tmp_{table_name} "
            f"({cols}) VALUES ({vals})"
        )

        if data_tuples:
            cursor.executemany(
                tmp_table_query,
                data_tuples
            )

        # Build the matching condition for the upsert
        delete_condition = ' AND '.join(
            [
                f"tmp_{table_name}.{col} = {table_name}.{col}"
                for col in id_columns
            ]
        )

        # Delete old matching records from the permanent table
        cursor.execute(
            f"""
            DELETE FROM reporting_schema.{table_name}
            USING reporting_schema.tmp_{table_name}
            WHERE {delete_condition}
            """
        )

        # Insert the new records into the permanent table
        cursor.execute(
            f"""
            INSERT INTO reporting_schema.{table_name} ({cols})
            SELECT {cols}
            FROM reporting_schema.tmp_{table_name}
            """
        )

        # Empty the temporary staging table
        cursor.execute(
            f"DELETE FROM reporting_schema.tmp_{table_name}"
        )

        # Save all database changes
        conn.commit()

        logging.info(
            f"Data ingested and merged successfully into {table_name}"
        )

    except Exception as e:

        if conn is not None:
            conn.rollback()

        logging.error(
            f"Failed to ingest data into {table_name}: {e}"
        )

        raise

    finally:

        if cursor is not None:
            cursor.close()

        if conn is not None:
            conn.close()


def calculate_genre_level_kpis(ti):
    """Calculate daily genre-level KPIs."""

    stream_files = ti.xcom_pull(
        task_ids='validate_datasets',
        key='stream_files'
    ) or []

    streams_data = pd.concat(
        [
            read_s3_csv(file)
            for file in stream_files
        ],
        ignore_index=True
    )

    songs_data = read_s3_csv(SONGS_FILE_PATH)

    streams_data['listen_date'] = (
        pd.to_datetime(streams_data['listen_time']).dt.date
    )

    merged_data = streams_data.merge(
        songs_data,
        on='track_id',
        how='left'
    )

    # KPI 1: Number of listens per genre per day
    genre_listen_count = (
        merged_data
        .groupby(['listen_date', 'track_genre'])
        .size()
        .reset_index(name='listen_count')
    )

    # KPI 2: Average listening duration per genre per day
    merged_data['duration_seconds'] = (
        merged_data['duration_ms'] / 1000
    )

    avg_duration = (
        merged_data
        .groupby(['listen_date', 'track_genre'])[
            'duration_seconds'
        ]
        .mean()
        .reset_index(name='average_duration')
    )

    # Calculate total listens per day
    total_listens = (
        merged_data
        .groupby('listen_date')
        .size()
        .reset_index(name='total_listens')
    )

    # KPI 3: Genre popularity index
    genre_listen_count = genre_listen_count.merge(
        total_listens,
        on='listen_date'
    )

    genre_listen_count['popularity_index'] = (
        genre_listen_count['listen_count']
        / genre_listen_count['total_listens']
    )

    # KPI 4: Most popular track per genre per day
    most_popular_track = (
        merged_data
        .groupby([
            'listen_date',
            'track_genre',
            'track_id'
        ])
        .size()
        .reset_index(name='track_count')
    )

    most_popular_track = most_popular_track.sort_values(
        by=[
            'listen_date',
            'track_genre',
            'track_count'
        ],
        ascending=[True, True, False]
    )

    most_popular_track = (
        most_popular_track
        .drop_duplicates(
            subset=['listen_date', 'track_genre'],
            keep='first'
        )
        .rename(
            columns={'track_id': 'most_popular_track_id'}
        )
    )

    # Combine all genre-level KPIs
    final_kpis = genre_listen_count[
        [
            'listen_date',
            'track_genre',
            'listen_count',
            'popularity_index'
        ]
    ]

    final_kpis = final_kpis.merge(
        avg_duration,
        on=['listen_date', 'track_genre']
    )

    final_kpis = final_kpis.merge(
        most_popular_track[
            [
                'listen_date',
                'track_genre',
                'most_popular_track_id'
            ]
        ],
        on=['listen_date', 'track_genre']
    )

    logging.info("Genre-level KPI columns:")
    logging.info(final_kpis.columns.tolist())

    # Load results into Redshift
    upsert_to_redshift(
        final_kpis,
        'genre_level_kpis',
        ['listen_date', 'track_genre']
    )


def calculate_hourly_kpis(ti):
    """Calculate hourly listening KPIs."""

    stream_files = ti.xcom_pull(
        task_ids='validate_datasets',
        key='stream_files'
    ) or []

    streams_data = pd.concat(
        [
            read_s3_csv(file)
            for file in stream_files
        ],
        ignore_index=True
    )

    songs_data = read_s3_csv(SONGS_FILE_PATH)
    users_data = read_s3_csv(USERS_FILE_PATH)

    streams_data['listen_date'] = (
        pd.to_datetime(streams_data['listen_time']).dt.date
    )

    streams_data['listen_hour'] = (
        pd.to_datetime(streams_data['listen_time']).dt.hour
    )

    # Combine streams, songs, and users
    full_data = (
        streams_data
        .merge(songs_data, on='track_id')
        .merge(users_data, on='user_id')
    )

    # KPI 1: Unique listeners per hour
    hourly_unique_listeners = (
        full_data
        .groupby(['listen_date', 'listen_hour'])['user_id']
        .nunique()
        .reset_index(name='unique_listeners')
    )

    # KPI 2: Most listened-to artist per hour
    artist_listen_counts = (
        full_data
        .groupby([
            'listen_date',
            'listen_hour',
            'artists'
        ])
        .size()
        .reset_index(name='listen_counts')
    )

    top_artist = artist_listen_counts.loc[
        artist_listen_counts
        .groupby(['listen_date', 'listen_hour'])[
            'listen_counts'
        ]
        .idxmax()
    ]

    top_artist = top_artist.rename(
        columns={'artists': 'top_artist'}
    )

    # KPI 3: Average listening sessions per user per hour
    full_data['session_id'] = (
        full_data['user_id'].astype(str)
        + '-'
        + full_data['listen_time'].astype(str)
    )

    sessions_per_user = (
        full_data
        .groupby([
            'listen_date',
            'listen_hour',
            'user_id'
        ])['session_id']
        .nunique()
        .reset_index(name='session_id')
    )

    avg_sessions_per_user = (
        sessions_per_user
        .groupby(['listen_date', 'listen_hour'])[
            'session_id'
        ]
        .mean()
        .reset_index(name='avg_sessions_per_user')
    )

    # KPI 4: Hourly track diversity index
    track_diversity = (
        full_data
        .groupby(['listen_date', 'listen_hour'])['track_id']
        .agg(['nunique', 'count'])
        .reset_index()
    )

    track_diversity['diversity_index'] = (
        track_diversity['nunique']
        / track_diversity['count']
    )

    # KPI 5: Most engaged age group per hour
    full_data['age_group'] = pd.cut(
        full_data['user_age'],
        bins=[17, 25, 35, 45, 55, 65, 100],
        labels=[
            '18-25',
            '26-35',
            '36-45',
            '46-55',
            '56-65',
            '66+'
        ]
    )

    user_group_engagement = (
        full_data
        .groupby([
            'listen_date',
            'listen_hour',
            'age_group'
        ], observed=True)
        .size()
        .reset_index(name='streams')
    )

    most_engaged_group = user_group_engagement.loc[
        user_group_engagement
        .groupby(['listen_date', 'listen_hour'])[
            'streams'
        ]
        .idxmax()
    ]

    most_engaged_group = most_engaged_group.rename(
        columns={
            'age_group': 'most_engaged_age_group'
        }
    )

    # Combine all hourly KPIs
    final_kpis = hourly_unique_listeners.merge(
        top_artist,
        on=['listen_date', 'listen_hour']
    )

    final_kpis = final_kpis.merge(
        avg_sessions_per_user,
        on=['listen_date', 'listen_hour']
    )

    final_kpis = final_kpis.merge(
        track_diversity[
            [
                'listen_date',
                'listen_hour',
                'diversity_index'
            ]
        ],
        on=['listen_date', 'listen_hour']
    )

    final_kpis = final_kpis.merge(
        most_engaged_group[
            [
                'listen_date',
                'listen_hour',
                'most_engaged_age_group'
            ]
        ],
        on=['listen_date', 'listen_hour']
    )

    # Select columns to match the Redshift table
    final_kpis = final_kpis[
        [
            'listen_date',
            'listen_hour',
            'unique_listeners',
            'listen_counts',
            'top_artist',
            'avg_sessions_per_user',
            'diversity_index',
            'most_engaged_age_group'
        ]
    ]

    logging.info("Hourly KPI columns:")
    logging.info(final_kpis.columns.tolist())

    # Load results into Redshift
    upsert_to_redshift(
        final_kpis,
        'hourly_kpis',
        ['listen_date', 'listen_hour']
    )


def move_processed_files(ti):
    """Move successfully processed stream files into the archive."""

    s3 = boto3.client('s3')

    try:
        stream_files = ti.xcom_pull(
            task_ids='validate_datasets',
            key='stream_files'
        ) or []

        for file in stream_files:

            copy_source = {
                'Bucket': BUCKET_NAME,
                'Key': file
            }

            destination_key = (
                ARCHIVE_PREFIX
                + file[len(STREAMS_PREFIX):]
            )

            try:
                s3.head_object(
                    Bucket=BUCKET_NAME,
                    Key=file
                )

            except ClientError as error:

                if error.response['Error']['Code'] not in (
                    '404',
                    'NoSuchKey',
                    'NotFound'
                ):
                    raise

                # The file may already have been archived
                s3.head_object(
                    Bucket=BUCKET_NAME,
                    Key=destination_key
                )

                logging.info(
                    f"Source already archived: {file}"
                )

                continue

            # Copy the file to the archive
            s3.copy_object(
                CopySource=copy_source,
                Bucket=BUCKET_NAME,
                Key=destination_key
            )

            # Delete the original file
            s3.delete_object(
                Bucket=BUCKET_NAME,
                Key=file
            )

            logging.info(
                f"Moved {file} to {destination_key}"
            )

    except Exception as e:
        logging.error(
            f"Failed to move files from {STREAMS_PREFIX} "
            f"to {ARCHIVE_PREFIX}: {str(e)}"
        )
        raise


# Define the Airflow DAG
with DAG(
    'data_validation_and_kpi_computation',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    validate_task = PythonOperator(
        task_id='validate_datasets',
        python_callable=validate_datasets
    )

    check_validation = BranchPythonOperator(
        task_id='check_validation',
        python_callable=branch_task
    )

    genre_task = PythonOperator(
        task_id='calculate_genre_level_kpis',
        python_callable=calculate_genre_level_kpis
    )

    hourly_task = PythonOperator(
        task_id='calculate_hourly_kpis',
        python_callable=calculate_hourly_kpis
    )

    move_files = PythonOperator(
        task_id='move_processed_files',
        python_callable=move_processed_files
    )

    end_dag = EmptyOperator(
        task_id='end_dag'
    )

    validate_task >> check_validation >> [
        genre_task,
        end_dag
    ]

    genre_task >> hourly_task >> move_files

