import json
import logging

from django.db import transaction

import requests

from froide_evidencecollection.models import Parliament, ParliamentPeriod

logger = logging.getLogger(__name__)

API_URL = "https://www.abgeordnetenwatch.de/api/v2"
ENTITY_TYPE_PARLIAMENTS = "parliaments"
ENTITY_TYPE_PARLIAMENT_PERIODS = "parliament-periods"


class AbgeordnetenwatchDataFetcher:
    def __init__(self, entity_type):
        self.entity_type = entity_type

    def iter_rows(self, extra_params=None):
        offset = 0

        while True:
            data = self.fetch_from_api(offset, extra_params)
            rows = data.get("data", [])
            page_info = data.get("meta", {}).get("result", {})

            for row in rows:
                yield row

            offset += page_info["range_end"]

            if offset >= page_info["total"]:
                break

    def fetch_from_api(self, offset=0, extra_params=None):
        url = f"{API_URL}/{self.entity_type}"

        params = {
            "sort_by": "id",
            "sort_direction": "asc",
        }

        params.update(extra_params or {})
        if offset > 0:
            params["range_start"] = offset

        response = requests.get(url, params=params)
        response.raise_for_status()

        return response.json()


class AbgeordnetenwatchEnricher:
    @transaction.atomic
    def setup(self):
        # try:
        self.create_parliaments()
        self.set_fractions()
        self.create_parliament_periods()
        # except Exception as e:
        #    logger.exception("Error during Abgeordnetenwatch enrichment setup")

    def create_parliaments(self):
        """
        Create parliaments from Abgeordnetenwatch data if they do not already exist.

        This method only has to be run once to populate the database with parliaments.
        """
        if Parliament.objects.exists():
            return

        fetcher = AbgeordnetenwatchDataFetcher(ENTITY_TYPE_PARLIAMENTS)
        to_create = [
            Parliament(aw_id=row["id"], name=row["label_external_long"])
            for row in fetcher.iter_rows()
        ]
        Parliament.objects.bulk_create(to_create)

        logger.info(
            f"Created {len(to_create)} parliaments from Abgeordnetenwatch data."
        )

    def set_fractions(self):
        """
        Set the fraction for each parliament that does not have one set yet.
        """
        parliaments = Parliament.objects.filter(fraction__isnull=True)

        if not parliaments.exists():
            return

        success = True

        for parliament in parliaments:
            try:
                parliament.set_fraction()
            except ValueError as e:
                success = False
                logger.error(
                    f"Error setting fraction for parliament {parliament.name}: {e}"
                )

        if success:
            logger.info(f"Set fractions for {len(parliaments)} parliaments.")

    def create_parliament_periods(self):
        """
        Create parliament periods from Abgeordnetenwatch data if they do not already exist.
        """
        fetcher = AbgeordnetenwatchDataFetcher(ENTITY_TYPE_PARLIAMENT_PERIODS)
        existing_ids = ParliamentPeriod.objects.values_list("aw_id", flat=True)
        parliaments = Parliament.objects.in_bulk(field_name="aw_id")

        extra_params = {
            "type": "legislature",
            "sort_by": "start_date_period",
        }

        if existing_ids:
            extra_params["id[notin]"] = json.dumps(existing_ids)

        to_create = []
        for row in fetcher.iter_rows(extra_params):
            parliament = parliaments.get(row["parliament"]["id"])
            period = ParliamentPeriod(
                aw_id=row["id"],
                name=row["label"],
                parliament=parliament,
                start_date=row["start_date_period"],
                end_date=row["end_date_period"],
            )
            to_create.append(period)

        ParliamentPeriod.objects.bulk_create(to_create)

        logger.info(
            f"Created {len(to_create)} parliament periods from Abgeordnetenwatch data."
        )
