import io

import polars as pl
from google.cloud import bigquery

from companies_house.config import settings


def save_dataframe_to_bq(
    df: pl.DataFrame,
    table_name: str,
    replace: bool = True,
) -> None:
    """Write DataFrame to stream as parquet file; does not hit disk."""
    client = bigquery.Client()
    params = {"write_disposition": "WRITE_TRUNCATE"} if replace else {}
    table_id = f"{settings.DATASET}.{table_name}"
    with io.BytesIO() as stream:
        df.write_parquet(stream)
        stream.seek(0)
        job = client.load_table_from_file(
            stream,
            destination=table_id,
            project=settings.GCP_PROJECT,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                **params,
            ),
        )
    job.result()  # Waits for the job to complete
