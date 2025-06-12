import threading
import pandas as pd


def frictionless_to_ckan_schema(fields):
    """
    Convert Frictionless schema fields to CKAN field types.
    """
    FRICTIONLESS_TO_CKAN = {
        "integer": "int",
        "number": "float",
        "datetime": "timestamp",
        "timestamptz": "timestamptz",
        "date": "date",
        "time": "time",
        "string": "text",
        "duration": "interval",
        "boolean": "bool",
        "object": "json",
        "array": "array",
        "year": "text",
        "yearmonth": "text",
        "geopoint": "text",
        "geojson": "json",
        "any": "text",
    }
    return [
        {
            "id": f.get("name"),
            "type": FRICTIONLESS_TO_CKAN.get(f.get("type", "string").lower(), "text"),
        }
        for f in fields
    ]


def ckan_to_frictionless_schema(field_type):
    """
    Convert CKAN field types to Frictionless field types.
    """
    CKAN_TO_FRICTIONLESS = {
        "int": "integer",
        "integer": "integer",
        "float": "number",
        "numeric": "number",
        "timestamp": "datetime",
        "timestamptz": "timestamptz",
        "date": "date",
        "time": "time",
        "text": "string",
        "interval": "duration",
        "bool": "boolean",
        "boolean": "boolean",
        "json": "object",
        "jsonb": "object",
        "array": "array",
    }
    return [
        {
            "name": f.get("id"),
            "type": CKAN_TO_FRICTIONLESS.get(f.get("type", "text").lower(), "string"),
        }
        for f in field_type
    ]


def set_ckan_preflow_status(ckan_api, **kwargs) -> None:
    """
    Update the preflow status in CKAN for a given resource.
    Required kwargs: resource_id, message
    Optional kwargs: type ('info' or 'error'), state
    """
    try:
        ckan_api.action.preflow_status_update(**kwargs)
    except Exception as e:
        raise RuntimeError(f"Failed to update preflow status in CKAN: {e}")


def async_set_ckan_preflow_status(*args, **kwargs):
    """
    Asynchronously set the CKAN preflow status using a separate thread.
    """
    thread = threading.Thread(target=set_ckan_preflow_status, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()


def df_import_data_to_postgres(chunk, engine, table_name, chunk_size=1000):
    """
    "Insert a chunk of data into a PostgreSQL table using panda DataFrame's to_sql method.
    Args:
        chunk (list): List of dictionaries representing the data to insert.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine connected to the PostgreSQL database.
        table_name (str): Name of the table to insert data into.
        chunk_size (int): Number of rows to insert in each batch.
    """
    if not chunk:
        return
    df = pd.DataFrame(chunk)
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=chunk_size,
        method="multi",
    )


class CKANFlowException(Exception):
    def __init__(self, ckan_api, **kwargs):
        if "type" not in kwargs:
            kwargs["type"] = "error"
        set_ckan_preflow_status(ckan_api, **kwargs)
        super().__init__(kwargs["message"])
