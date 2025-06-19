# froide evidencecollection

## Import from NocoDB

In order to import data from NocoDB, you need to set the following environment variables:

- `FROIDE_EVIDENCECOLLECTION_NOCODB_API_URL`: The base URL of the NocoDB instance.
- `FROIDE_EVIDENCECOLLECTION_NOCODB_API_TOKEN`: The API token for accessing the NocoDB API.
- IDs for the tables in NocoDB that contain the data to be imported. You can get a table's ID by right-clicking on the name in the left-hand menu in NocoDB.
  - `FROIDE_EVIDENCECOLLECTION_NOCODB_PERSON_TABLE`
  - `FROIDE_EVIDENCECOLLECTION_NOCODB_SOURCE_TABLE`
  - `FROIDE_EVIDENCECOLLECTION_NOCODB_EVIDENCE_TABLE`
  - `FROIDE_EVIDENCECOLLECTION_NOCODB_GROUP_TABLE`

Additional configuration options can be set in the Django settings file.

The import can be run manually using the following command:

```bash
python manage.py import_nocodb
```

If `DEBUG` is set to `True`, any import errors will be caught and logged to the console. If `DEBUG` is set to `False`, the import will fail immediately on any error and any changes will be rolled back.

## Rebuild Search Index

To rebuild the search index for the `froide_evidencecollection` app, you can use the following command:

```bash
python manage.py search_index --rebuild --models froide_evidencecollection
```
