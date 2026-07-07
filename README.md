# froide evidencecollection

## Rebuild Search Index

To rebuild the search index for the `froide_evidencecollection` app, you can use the following command:

```bash
python manage.py search_index --rebuild --models froide_evidencecollection
```

## Running Tests

Run tests with pytest:

```bash
docker compose -f compose-dev.yaml up
# --create-db option is only needed the first time.
pytest --create-db
```

Run tests with coverage:

```bash
coverage run -m pytest && coverage report
```

Alternatively, you can run `make test` or `make testci`.
