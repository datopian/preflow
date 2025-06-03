

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


def set_ckan_preflow_status(
    ckan_api, resource_id, message, type="info", state="running"
):
    """
    Update the preflow status in CKAN for a given resource.
    If type is 'error', send as error. Otherwise, send as message.
    """
    try:
        ckan_api.action.preflow_status_update(
            resource_id=resource_id,
            **({"error": message} if type == "error" else {"message": message}),
            state=state,
        )
    except Exception as e:
        raise RuntimeError(f"Failed to update preflow status in CKAN: {e}")


class CKANFlowException(Exception):
    """Custom exception for CKAN Prefect flows that also updates CKAN status."""

    def __init__(self, ckan_api, resource_id, message, state="Failed"):
        set_ckan_preflow_status(ckan_api, resource_id, message, state)
        super().__init__(message)
