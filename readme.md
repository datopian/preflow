# Preflow - CKAN Prefect Workflow

Preflow is an open-source, [Prefect](https://www.prefect.io/) based workflow system designed to automate and orchestrate data processing for CKAN. It can be used for a wide range of data operations, including harvesting, validation, transformation, and loading of resource files into the CKAN Datastore, as well as many other extensible data pipeline tasks. Preflow is built for flexibility and can be easily extended to support additional data operations per your requirements.


## Available Flows
- **ckan_datastore_ingestion**: Ingests resource files into the CKAN Datastore. This flow is triggered by the [ckanext-preflow](https://github.com/datopian/ckanext-preflow)extension when a resource is created or updated in CKAN.


## Quick Start

1. **Clone this repository and install dependencies:**

   ```bash
   git clone https://github.com/datopian/preflow.git
   ```
2.  ** Build and run the Docker container:**

   ```bash
   docker compose -f docker-compose.yml up --build
   ```

3. **Deploy the Prefect flow:**

   ```bash
   docker exec -it prefect-worker prefect deploy -n ckan_datastore_ingestion 
   ```

4. **Trigger the flow run** from the Prefect API, UI or CLI. 


## Configuration
- Edit `flows/prefect.yaml` to adjust deployments, parameters, and schedules.

## Notes
- The workflow **requires** the `ckanext-preflow` extension to be installed and enabled on your CKAN instance for status
  trigger the Prefect flow if you want to use the `ckan_datastore_ingestion` flow.

## References
- [Prefect Documentation](https://docs.prefect.io/)
- [ckanext-preflow](https://github.com/datopian/ckanext-preflow)
- [CKAN API Guide](https://docs.ckan.org/en/latest/api/)

