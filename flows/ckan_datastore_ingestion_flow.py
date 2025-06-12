import os
import json
import requests
from frictionless import Resource, Schema, validate, system

from prefect import flow, get_run_logger, task
from sqlalchemy import create_engine

from ckanapi import RemoteCKAN
from dependencies.utils import (
    CKANFlowException,
    frictionless_to_ckan_schema,
    async_set_ckan_preflow_status,
    df_import_data_to_postgres,
)


@task(retries=3, retry_delay_seconds=10)
def fetch_ckan_resource_file(ckan_api, resource_dict):
    """
    Download a CKAN resource file and return the local file path.
    Updates CKAN status on success or failure.
    """
    logger = get_run_logger()
    resource_id = resource_dict.get("id")
    file_url = resource_dict.get("url")
    try:
        logger.info(f"Fetching resource from {file_url}")
        headers = {
            "Authorization": ckan_api.apikey,
        }
        resp = requests.get(file_url, headers=headers)
        if resp.status_code == 200:
            local_file_path = f"/tmp/{resource_id}.csv"
            with open(local_file_path, "wb") as f:
                f.write(resp.content)
            logger.info(f"File downloaded to {local_file_path}")
            async_set_ckan_preflow_status(
                ckan_api,
                resource_id=resource_id,
                message="Downloading file completed successfully.",
            )
            return local_file_path
        else:
            raise CKANFlowException(
                ckan_api,
                resource_id,
                f"Failed to download file: {resp.status_code}",
            )
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise CKANFlowException(
            ckan_api,
            resource_id=resource_id,
            message=f"Failed to download file: {resp.status_code}",
        )


@task(retries=3, retry_delay_seconds=10)
def create_ckan_datastore_table(ckan_api, resource_dict):
    """
    Create or update a CKAN datastore table for the given resource.
    Deletes the existing table if it exists, then creates a new one.
    Updates CKAN status on success or failure.
    """
    logger = get_run_logger()
    resource_id = resource_dict.get("id")
    try:
        schema = resource_dict.get("schema", {})
        ckan_schema = frictionless_to_ckan_schema(schema.get("fields", []))
        # Try to delete existing datastore table if it exists
        try:
            logger.info(
                f"Attempting to delete existing datastore table for resource {resource_id}"
            )
            ckan_api.action.datastore_delete(resource_id=resource_id, force=True)
            logger.info(f"Existing datastore table for resource {resource_id} deleted.")
        except Exception as del_exc:
            logger.warning(
                f"No existing datastore table to delete or failed to delete: {del_exc}"
            )
        logger.info(
            f"Creating datastore table for resource {resource_id} with schema: {ckan_schema}"
        )
        ckan_api.action.datastore_create(
            resource_id=resource_id, fields=ckan_schema, force=True
        )
        async_set_ckan_preflow_status(
            ckan_api,
            resource_id=resource_id,
            message=f"Datastore table created successfully with schema {ckan_schema}",
        )
    except Exception as e:
        logger.error(f"Failed to create datastore table: {e}")
        raise CKANFlowException(
            ckan_api,
            resource_id=resource_id,
            message=f"Failed to create datastore table in CKAN: {e}",
        )


@task(retries=0, retry_delay_seconds=10)
def load_csv_to_postgres_and_cleanup(
    ckan_api, resource_dict, local_file, datastore_db_url, table_name
):
    """
    Load CSV data into Postgres, rename unnamed columns, add _id, and cleanup file.
    Updates CKAN status on success or failure.
    """

    logger = get_run_logger()
    engine = create_engine(datastore_db_url)
    try:
        resource_id = resource_dict.get("id")
        fields = resource_dict.get("schema", {}).get("fields", [])
        if not any(f.get("name") == "_id" for f in fields):
            fields.append({"name": "_id", "type": "integer"})
        schemaa = Schema({"fields": fields})
        resource = Resource(path=local_file, schema=schemaa)
        chunk_size = 10000
        with resource:
            chunk = []
            idx = 1
            for row in resource.row_stream:
                row["_id"] = idx
                chunk.append(row)
                idx += 1
                if len(chunk) == chunk_size:
                    df_import_data_to_postgres(chunk, engine, table_name, chunk_size)
                    chunk = []
            df_import_data_to_postgres(chunk, engine, table_name, chunk_size)

        logger.info(f"Data ingested to datastore for resource {resource_id}")

        async_set_ckan_preflow_status(
            ckan_api,
            resource_id=resource_id,
            message="Data ingestion to datastore completed successfully",
            state="completed",
        )
        if os.path.exists(local_file):
            os.remove(local_file)
            logger.info(f"Local file {local_file} cleaned up successfully")
        else:
            logger.warning(f"Local file {local_file} does not exist for cleanup")
    except Exception as e:
        logger.error(f"Failed to ingest or cleanup file: {e}")
        raise CKANFlowException(
            ckan_api,
            resource_id=resource_id,
            message=f"Failed to ingest data to datastore.",
        )


@task(retries=0, retry_delay_seconds=10)
def validate_ckan_resource(local_file, ckan_api, resource_dict: dict):
    """
    Validate the CKAN resource schema using Frictionless.
    """
    logger = get_run_logger()
    resource_id = resource_dict.get("id")
    format = resource_dict.get("format", "CSV").lower()
    schema = Schema(resource_dict.get("schema", {}))
    logger.info(
        f"Validating resource {resource_id} with format {format} and schema {schema.to_dict()}"
    )
    async_set_ckan_preflow_status(
        ckan_api,
        resource_id=resource_id,
        message="Validating data against schema...",
        type="info",
    )
    try:
        with system.use_context(trusted=True):
            resource = Resource(path=local_file, schema=schema, format=format)
            report = validate(resource).to_dict()
        if report.get("valid"):
            async_set_ckan_preflow_status(
                ckan_api,
                resource_id=resource_id,
                message="Data validated successfully",
                validation_report=report,
            )
        else:
            logger.error(f"Validation failed for resource {resource_id}: {report}")
            raise CKANFlowException(
                ckan_api,
                resource_id=resource_id,
                message=f"Data is not valid according to the schema.",
                validation_report=report,
            )
    except Exception as e:
        logger.error(f"Validation error for resource {resource_id}: {e}")
        raise CKANFlowException(
            ckan_api,
            resource_id=resource_id,
            message=f"Failed to validate data against schema: {e}",
        )


@flow(name="ckan_datastore_ingestion")
def ckan_datastore_ingestion(resource_dict: dict, ckan_config: dict):
    """
    Orchestrates the CKAN resource ingestion to the datastore.
    """
    logger = get_run_logger()
    logger.info("Starting CKAN data ingestion flow")
    logger.info
    ckan_url = ckan_config.get("ckan_url")
    api_key = ckan_config.get("api_key")
    datastore_db_url = ckan_config.get("datastore_db_url")
    table_name = resource_dict.get("id")
    schema = resource_dict.get("schema", {})
    ckan_api = RemoteCKAN(ckan_url, apikey=api_key)

    if not schema:
        raise CKANFlowException(
            ckan_api=ckan_api,
            resource_id=resource_dict.get("id"),
            message="Resource schema is required for CKAN datastore ingestion.",
        )
    try:
        local_file = fetch_ckan_resource_file(ckan_api, resource_dict)
        validate_ckan_resource(local_file, ckan_api, resource_dict)
        create_ckan_datastore_table(ckan_api, resource_dict)
        load_csv_to_postgres_and_cleanup(
            ckan_api, resource_dict, local_file, datastore_db_url, table_name
        )
        logger.info("CKAN data ingestion flow completed successfully.")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
