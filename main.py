import os
import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import storage, bigquery
from pyspark.sql.functions import month
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


def filter_by_year(file, pair, years):
    """
    This method is to filter the years of the csv files and write them to separate parquet files.
    It uses PySpark to
    This method also adds a month column which chooses the month of the data by the date column
    :param file:
    :param pair:
    :param years:
    :return:
    """
    # Read the input CSV file
    spark = SparkSession.builder.appName("FilterByYear").getOrCreate()
    dataf = spark.read.options(inferSchema="true", delimiter=",", header="true").csv(file)
    from pyspark.sql.functions import year
    dataf = dataf.withColumn("Year", year("Date"))
    dataf = dataf.withColumn("Month", month("Date").cast(StringType()))

    for year in years:
        year_dir = f"converted_files/{pair}/{year}"
        os.makedirs(year_dir, exist_ok=True)

        filtered_dataf = dataf.filter(dataf.Year == year).coalesce(1)
        filtered_dataf.write.csv(f"{year_dir}/{pair}_{year}_filtered.csv", header=True, mode="overwrite")
        filtered_dataf.write.parquet(f"{year_dir}/{pair}_{year}/{pair}_{year}_filtered.parquet", mode="overwrite")
        filtered_dataf.show()

    filtered_dataf_all_years = dataf.filter(dataf.Year.isin(*years)).coalesce(1)
    filtered_dataf_all_years.write.csv(f"converted_files/{pair}/{pair}_all_years_filtered.csv", header=True,
                                       mode="overwrite")
    filtered_dataf_all_years.write.parquet(f"converted_files/{pair}/{pair}_all_years_filtered.parquet",
                                           mode="overwrite")


def upload_parquets_to_cloud_bucket(pair, years):
    """
    This method is to upload the parquets from local to cloud bucket storage
    :param pair:
    :param years:
    :return:
    """
    project_id = 'regal-fortress-378317'
    bucket_name = 'example_bucket1121212'

    for year in years:
        parquet_dir_path = f'converted_files/{pair}/{year}/{pair}_{year}/{pair}_{year}_filtered.parquet'
        os.chmod(parquet_dir_path, 0o777)  # add this line to change directory permissions
        for file in os.listdir(parquet_dir_path):
            if file.endswith(".parquet"):
                parquet_file_path = os.path.join(parquet_dir_path, file)
                client = storage.Client(project=project_id)
                bucket = client.get_bucket(bucket_name)
                blob = bucket.blob(f'{pair}_{year}_filtered.parquet')
                blob.upload_from_filename(parquet_file_path)
                print(f'Uploaded {parquet_file_path} to {blob.public_url}')

    parquet_dir_path = f'converted_files/{pair}/{pair}_all_years_filtered.parquet'
    os.chmod(parquet_dir_path, 0o777)  # add this line to change directory permissions
    for file in os.listdir(parquet_dir_path):
        if file.endswith(".parquet"):
            parquet_file_path = os.path.join(parquet_dir_path, file)
            client = storage.Client(project=project_id)
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(f'{pair}_all_years_filtered.parquet')
            blob.upload_from_filename(parquet_file_path)
            print(f'Uploaded {parquet_file_path} to {blob.public_url}')


def upload_to_cloud(pair, years):
    """
    This method is to upload the parquet files and data from the google cloud bucket to BigQuery.
    It will make tables for the data and get the parquet files into the right format.
    :param pair:
    :param years:
    :return:
    """
    project_id = 'regal-fortress-378317'
    dataset_name = f'{pair}_dataset'
    table_name = f'{pair}_table'
    bucket_name = 'example_bucket1121212'
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_ref)
        print(f'Dataset {dataset_name} already exists')
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print(f'Created dataset {dataset_name}')

    schema = [bigquery.SchemaField('Date', 'TIMESTAMP'), bigquery.SchemaField('Open', 'FLOAT'),
              bigquery.SchemaField('High', 'FLOAT'), bigquery.SchemaField('Low', 'FLOAT'),
              bigquery.SchemaField('Close', 'FLOAT'), bigquery.SchemaField('Volume Pair', 'FLOAT'),
              bigquery.SchemaField('Volume', 'FLOAT'), bigquery.SchemaField('Month', 'STRING'), ]

    table_ref = dataset_ref.table(table_name)
    try:
        client.get_table(table_ref)
        table = bigquery.Table(table_ref, schema=schema)
        print(f'Table {table_name} already exists')
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f'Created table {table_name}')
    total_rows = 0
    total_rows_all_tables = 0

    # Upload each filtered Parquet file to BigQuery
    for year in years:
        # Load the Parquet file into a Pandas dataframe
        parquet_file = f'gs://{bucket_name}/{pair}_{year}_filtered.parquet'
        df = pd.read_parquet(parquet_file)

        # Convert the datatype of each column
        df['Date'] = pd.to_datetime(df['Date'])
        df['Open'] = df['Open'].astype(float)
        df['High'] = df['High'].astype(float)
        df['Low'] = df['Low'].astype(float)
        df['Close'] = df['Close'].astype(float)
        df['Volume Pair'] = df['Volume Pair'].astype(float)
        df['Volume'] = df['Volume'].astype(float)
        df['Month'] = df['Month'].astype(str)

        # Upload the modified dataframe to BigQuery
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        load_job.result()
        total_rows += load_job.output_rows

        print(f'Loaded {load_job.output_rows} rows from {parquet_file} to BigQuery table {table_name}')

        combined_parquet_file_path = f'converted_files/{pair}_all_years_filtered.parquet'

        # Define the Google Cloud Storage URI of the combined Parquet file
        uri = f'gs://{bucket_name}/{os.path.basename(combined_parquet_file_path)}'

        # Define the BigQuery table load job configuration for the combined Parquet file
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            autodetect=True,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        load_job = client.load_table_from_uri(
            uri,
            table_ref,
            job_config=job_config,
        )
        load_job.result()

    print(f'Total number of rows loaded to {table_name}: {total_rows}')


def upload_market_share_data():
    """
    This method is to upload the market share data csv file to google cloud bucket and then to BigQuery
    :return:
    """
    project_id = 'regal-fortress-378317'
    bucket_name = 'example_bucket1121212'
    file = 'source_files/Market-Cap-Share - Nomics-Dashboard-USD-1d-2023-03-18T13_51_38.023Z.csv'
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file)
    blob.upload_from_filename(file)
    print(f'Uploaded {file} to {blob.public_url}')

    dataset_name = 'market_dataset'
    table_name = 'market_table'
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_ref)
        print(f'Dataset {dataset_name} already exists')
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print(f'Created dataset {dataset_name}')

    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("price_date", "STRING"),
        bigquery.SchemaField("price_timestamp", "STRING"),
        bigquery.SchemaField("circulating_supply", "INT64"),
        bigquery.SchemaField("max_supply", "INT64"),
        bigquery.SchemaField("market_cap", "INT64"),
        bigquery.SchemaField("market_cap_dominance", "FLOAT"),
        bigquery.SchemaField("volume", "FLOAT"),
        bigquery.SchemaField("price_change", "FLOAT"),
        bigquery.SchemaField("price_change_pct", "FLOAT"),
        bigquery.SchemaField("volume_change", "FLOAT"),
        bigquery.SchemaField("volume_change_pct", "FLOAT"),
        bigquery.SchemaField("market_cap_change", "FLOAT"),
        bigquery.SchemaField("market_cap_change_pct", "FLOAT"),
    ]
    uri = 'gs://example_bucket1121212/source_files/Market-Cap-Share - Nomics-Dashboard-USD-1d-2023-03-18T13_51_38.023Z.csv'
    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config,
    )
    load_job.result()


if __name__ == '__main__':
    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
    filter_by_year('source_files/Gemini_BTCUSD_1h.csv', 'BTC_USD', years)
    filter_by_year('source_files/Gemini_ETHUSD_1h.csv', 'ETH_USD', years)
    filter_by_year('source_files/Gemini_ETHBTC_1h.csv', 'ETH_BTC', years)

    upload_parquets_to_cloud_bucket('BTC_USD', years)
    upload_parquets_to_cloud_bucket('ETH_USD', years)
    upload_parquets_to_cloud_bucket('ETH_BTC', years)

    upload_to_cloud('BTC_USD', years)
    upload_to_cloud('ETH_USD', years)
    upload_to_cloud('ETH_BTC', years)
    upload_market_share_data()
