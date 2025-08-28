# froide evidencecollection

## Import from NocoDB

### Setup

In order to import data from NocoDB, you need to set the following environment variables:

- `FROIDE_EVIDENCECOLLECTION_NOCODB_API_URL`: The base URL of the NocoDB instance.
- `FROIDE_EVIDENCECOLLECTION_NOCODB_API_TOKEN`: The API token for accessing the NocoDB API.
- IDs for the tables in NocoDB that contain the data to be imported. You can get a table's ID by right-clicking on the name in the left-hand menu in NocoDB.
  - `FROIDE_EVIDENCECOLLECTION_NOCODB_TABLE_ACTOR`
  - `FROIDE_EVIDENCECOLLECTION_NOCODB_TABLE_AFFILIATION`
  - `FROIDE_EVIDENCECOLLECTION_NOCODB_TABLE_EVIDENCE`
  - `FROIDE_EVIDENCECOLLECTION_NOCODB_TABLE_ROLE`
- IDs for certain views in NocoDB that are used for the import:
  - `FROIDE_EVIDENCECOLLECTION_NOCODB_VIEW_ACTOR_ORGANIZATION`
  - `FROIDE_EVIDENCECOLLECTION_NOCODB_VIEW_ACTOR_PERSON`
- `FROIDE_EVIDENCECOLLECTION_NOCODB_IMPORT_CONFIG`: a dictionary containing additional configuration options, see the `settings/base.py` file in the `fragdenstaat_de` project.

### Run the Import

The import can be run manually using the following command:

```bash
python manage.py import_nocodb
```

This will by default import data from the configured NocoDB tables except for the `Evidence` table. For importing the full data run

```bash
python manage.py import_nocodb --full
```

If `DEBUG` is set to `True`, any import errors will be caught and logged. If `DEBUG` is set to `False`, the import will fail immediately on any error and any changes will be rolled back.

## Rebuild Search Index

To rebuild the search index for the `froide_evidencecollection` app, you can use the following command:

```bash
python manage.py search_index --rebuild --models froide_evidencecollection
```
