import json
import os

from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound


def main(event, context):
    file = event
    bucket_name = file["bucket"]
    file_name = file["name"]

    if not file_name.endswith(".csv"):
        print(f"Ignoring non-CSV file: {file_name}")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(file_name)

    if not blob:
        print(f"Could not retrieve blob {file_name} from bucket {bucket_name}")
        return

    table_id = blob.metadata.get("site") if blob.metadata else None
    if not table_id:
        print(f"Missing 'site' in metadata for {file_name}")
        return
    schema_json = blob.metadata.get("schema") if blob.metadata else None
    if not schema_json:
        print(f"No schema defined for table: {table_id}")
        return

    try:
        data = json.loads(schema_json)
    except json.JSONDecodeError as e:
        print(f"Failed to decode schema JSON: {e}")
        return

    schema = []
    for f in data:
        name = f.get("Name")
        field_type = f.get("Type")
        if not name or not field_type:
            raise ValueError(f"Invalid field in schema metadata: {f}")
        schema.append(bigquery.SchemaField(name, field_type))

    dataset_id = os.environ["BQ_DATASET"]
    uri = f"gs://{bucket_name}/{file_name}"

    bq_client = bigquery.Client()
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
    )

    try:
        bq_client.get_table(table_ref)
    except NotFound:
        print(f"Table {table_ref} not found. Creating from file {file_name}...")
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()

    print(f"Loaded file {file_name} into {table_ref}")
