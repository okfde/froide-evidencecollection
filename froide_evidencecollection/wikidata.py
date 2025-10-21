from django.db import transaction

import requests

from froide_evidencecollection.models import Person
from froide_evidencecollection.utils import ImportStatsCollection

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
HEADERS = {"User-Agent": "froide-evidencecollection/1.0"}
# Wikidata SPARQL endpoint has limits on query complexity; using batches to avoid issues.
BATCH_SIZE = 100


class ImportError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class WikidataImporter:
    def __init__(self):
        self.stats = ImportStatsCollection()

    @transaction.atomic
    def run(self):
        aw_ids = Person.objects.filter(wikidata_id__isnull=True).values_list(
            "aw_id", flat=True
        )
        aw_to_wikidata = self.get_wikidata_ids_from_aw_ids(aw_ids)

        existing = Person.objects.filter(wikidata_id__in=aw_to_wikidata.values())
        if existing.exists():
            id_str = ", ".join([f"{p} ({p.wikidata_id})" for p in existing])
            msg = f"Some Wikidata IDs are already assigned to exising persons: {id_str}"
            raise ImportError(msg)

        for aw_id, wikidata_id in aw_to_wikidata.items():
            person = Person.objects.filter(aw_id=aw_id).first()
            person.wikidata_id = wikidata_id
            person.save()

            self.stats.track_updated(Person, person.last_synced_state, person)

    def fetch_from_api(self, query):
        response = requests.get(
            WIKIDATA_SPARQL_URL,
            params={"query": query, "format": "json"},
            headers=HEADERS,
        )
        response.raise_for_status()
        return response.json()

    def get_wikidata_ids_from_aw_ids(self, aw_ids):
        """
        Retrieve Wikidata IDs for given Abgeordnetenwatch IDs.
        """
        if not aw_ids:
            return {}

        query = """
        SELECT ?item ?aw_id WHERE {
          VALUES ?aw_id { %s }
          ?item wdt:P5355 ?aw_id .
        }
        """

        result = {}

        for i in range(0, len(aw_ids), BATCH_SIZE):
            batch_aw_ids = aw_ids[i : i + BATCH_SIZE]
            batch_query = query % " ".join(f'"{aw_id}"' for aw_id in batch_aw_ids)

            data = self.fetch_from_api(batch_query)

            for item in data["results"]["bindings"]:
                aw_id = item["aw_id"]["value"]
                wikidata_id = item["item"]["value"].split("/")[-1]
                result[aw_id] = wikidata_id

        return result

    def log_stats(self):
        """Return collected stats from all importers"""
        return self.stats.to_dict()
