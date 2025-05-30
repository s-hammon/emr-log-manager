import os

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def main(event, context):
    file = event
    bucket_name = file["bucket"]
    file_name = file["name"]

    if not file_name.endswith(".csv"):
        print(f"Ignoring non-CSV file: {file_name}")
        return

    dataset_id = os.environ["BQ_DATASET"]
    table_id = os.environ["BQ_TABLE"]
    uri = f"gs://{bucket_name}/{file_name}"

    client = bigquery.Client()
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("AppointmentID", "INTEGER"),
            bigquery.SchemaField("Accession", "STRING"),
            bigquery.SchemaField("AppointmentDate", "DATE"),
            bigquery.SchemaField("Location", "STRING"),
            bigquery.SchemaField("PatientMRN", "STRING"),
            bigquery.SchemaField("PatientLastName", "STRING"),
            bigquery.SchemaField("PatientFirstName", "STRING"),
            bigquery.SchemaField("PatientMiddleName", "STRING"),
            bigquery.SchemaField("Insurance_Plan", "STRING"),
            bigquery.SchemaField("Insurance_PlanSecondary", "STRING"),
            bigquery.SchemaField("Insurance_PlanTertiary", "STRING"),
            bigquery.SchemaField("ReferringPhysicianFirstName", "STRING"),
            bigquery.SchemaField("ReferringPhysicianLastName", "STRING"),
            bigquery.SchemaField("Insurance_SubscriberNumber", "STRING"),
            bigquery.SchemaField("Insurance_SubscriberNumberSecondary", "STRING"),
            bigquery.SchemaField("Insurance_SubscriberNumberTertiary", "STRING"),
            bigquery.SchemaField("ExamResultsStatus", "STRING"),
            bigquery.SchemaField("ExamFinalizedDate", "TIMESTAMP"),
            bigquery.SchemaField("ExamFinalizedDate_hourofday", "INTEGER"),
            bigquery.SchemaField("CPTCode", "STRING"),
            bigquery.SchemaField("CPTDescription", "STRING"),
            bigquery.SchemaField("Exam_ExamCode", "STRING"),
            bigquery.SchemaField("ExamDescriptionDisplay", "STRING"),
        ],
    )

    try:
        client.get_table(table_ref)
    except NotFound:
        print(f"Table {table_ref} not found. Creating from file {file_name}...")
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()

    print(f"Loaded file {file_name} into {table_ref}")
