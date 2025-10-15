import json
import logging

from django.conf import settings
from django.db import transaction

import requests

from froide_evidencecollection.models import (
    Affiliation,
    Election,
    LegislativePeriod,
    Parliament,
    Person,
    Role,
)
from froide_evidencecollection.utils import (
    ImportStatsCollection,
    equals,
    filter_future_date,
)

logger = logging.getLogger(__name__)

CONFIG = settings.FROIDE_EVIDENCECOLLECTION_ABGEORDNETENWATCH_CONFIG
MANDATE_ROLE_UUID = CONFIG.get("mandate_role_uuid")
CANDIDATE_ROLE_UUID = CONFIG.get("candidate_role_uuid")
PARTY_ID = CONFIG.get("party_id")
FRACTIONS = CONFIG.get("fractions", [])
API_URL = "https://www.abgeordnetenwatch.de/api/v2"
ENTITY_TYPE_PARLIAMENTS = "parliaments"
ENTITY_TYPE_PARLIAMENT_PERIODS = "parliament-periods"
ENTITY_TYPE_POLITICIANS = "politicians"
ENTITY_TYPE_CANDIDACIES_MANDATES = "candidacies-mandates"
RESULTS_PER_PAGE = 500

# Fix `previous_period` for "Hessen 2018 - 2024".
PREVIOUS_PERIOD_MAP = {116: 55}


class AbgeordnetenwatchDataFetcher:
    def __init__(self, entity_type):
        self.entity_type = entity_type

    def iter_rows(self, extra_params=None):
        for rows in self.iter_pages(extra_params):
            for row in rows:
                yield row

    def iter_pages(self, extra_params=None):
        page = 0
        offset = 0

        while True:
            data = self.fetch_from_api(page, extra_params)
            rows = data.get("data", [])
            page_info = data.get("meta", {}).get("result", {})

            if not rows:
                break

            yield rows

            offset += page_info["results_per_page"]

            if offset >= page_info["total"]:
                break

            page += 1

    def fetch_from_api(self, page=0, extra_params=None):
        url = f"{API_URL}/{self.entity_type}"

        params = {
            "sort_by": "id",
            "sort_direction": "asc",
            "page": page,
            "pager_limit": RESULTS_PER_PAGE,
        }

        params.update(extra_params or {})

        logger.debug(f"Fetching data from {url} with params {params}")

        response = requests.get(url, params=params)
        response.raise_for_status()

        logger.info(f"Fetched data from {response.url}")

        return response.json()


class ImportError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class AbgeordnetenwatchDataImporter:
    """Base class for all Abgeordnetenwatch importers"""

    def __init__(self, model, entity_type):
        self.model = model
        self.model_name = model.__name__
        self.entity_type = entity_type
        self.fetcher = AbgeordnetenwatchDataFetcher(entity_type)
        self.obj_data = {}
        self.id_field = "aw_id"

    def run(self):
        """Main method to run the import process"""
        self.init_related_entities()
        self.collect_data()
        if self.obj_data:
            self.create_or_update_instances()

    def init_related_entities(self):
        """Initialize related entities. Override in subclasses if needed."""
        pass

    def collect_data(self):
        """Collect data from abgeordnetenwatch API. Override in subclasses."""
        pass

    def get_extra_params(self):
        """Get extra parameters for API request. Override in subclasses if needed."""
        return {}

    def process_row(self, row):
        """Process a single row from the API. Override in subclasses."""
        pass

    def create_or_update_instances(self):
        """Create or update instances based on collected data. Override in subclasses."""
        pass


class HelperModelImporter(AbgeordnetenwatchDataImporter):
    """Base class for helper models that can be created in bulk"""

    def __init__(self, model, entity_type):
        super().__init__(model, entity_type)
        self.existing_ids = self.model.objects.values_list(self.id_field, flat=True)

    def collect_data(self):
        extra_params = self.get_extra_params()

        if self.existing_ids:
            extra_params["id[notin]"] = json.dumps(list(self.existing_ids))

        for row in self.fetcher.iter_rows(extra_params):
            self.process_row(row)

    def create_or_update_instances(self):
        missing_ids = set(self.obj_data.keys()) - set(self.existing_ids)

        to_create = [
            self.create_instance(data)
            for id_, data in self.obj_data.items()
            if id_ in missing_ids
        ]

        self.model.objects.bulk_create(to_create)

        if to_create:
            logger.info(
                f"Created {len(to_create)} {self.model_name.lower()}s from abgeordnetenwatch data."
            )

    def create_instance(self, data):
        return self.model(**data)


class ParliamentImporter(HelperModelImporter):
    """Importer for parliaments from abgeordnetenwatch"""

    def __init__(self):
        super().__init__(Parliament, ENTITY_TYPE_PARLIAMENTS)

    def process_row(self, row):
        data = {
            "aw_id": row["id"],
            # `label_external_long`: "Abgeordnetenhaus Berlin" vs. `label`: "Berlin"
            "name": row["label_external_long"],
        }

        try:
            parliament = Parliament(**data)
            fraction = parliament.find_matching_fraction()
            data["fraction"] = fraction
            self.obj_data[row["id"]] = data
        except ValueError as e:
            msg = f"Error finding fraction: {e}"
            raise ImportError(msg) from e


class PeriodImporter(HelperModelImporter):
    """Class for period importers (elections and legislative periods)"""

    def __init__(self, model, period_type):
        super().__init__(model, ENTITY_TYPE_PARLIAMENT_PERIODS)
        self.period_type = period_type
        self.parliaments = {}

    def init_related_entities(self):
        super().init_related_entities()
        self.parliaments = Parliament.objects.in_bulk(field_name=self.id_field)

    def get_extra_params(self):
        return {
            "type": self.period_type,
            "sort_by": "start_date_period",
        }

    def get_parliament(self, parliament_id):
        """Get the parliament instance by its abgeordnetenwatch ID"""
        parliament = self.parliaments.get(parliament_id)

        if not parliament:
            msg = f"Parliament with abgeordnetenwatch ID {parliament_id} not found"
            raise ImportError(msg)

        return parliament


class ElectionImporter(PeriodImporter):
    """Importer for elections from abgeordnetenwatch"""

    def __init__(self):
        super().__init__(Election, "election")

    def process_row(self, row):
        self.obj_data[row["id"]] = {
            "aw_id": row["id"],
            "name": row["label"],
            "parliament_id": row["parliament"]["id"],
            "start_date": row["start_date_period"],
            # `end_date_period` is sometimes but not always identical to `election_date`.
            # Use always `election_date` to be consistent.
            "end_date": row["election_date"],
        }

    def create_instance(self, data):
        parliament_id = data.pop("parliament_id")
        parliament = self.get_parliament(parliament_id)

        return Election(parliament=parliament, **data)


class LegislativePeriodImporter(PeriodImporter):
    """Importer for legislative periods from abgeordnetenwatch"""

    def __init__(self):
        super().__init__(LegislativePeriod, "legislature")
        self.elections = {}

    def init_related_entities(self):
        super().init_related_entities()
        self.elections = Election.objects.in_bulk(field_name=self.id_field)

    def process_row(self, row):
        self.obj_data[row["id"]] = {
            "aw_id": row["id"],
            "name": row["label"],
            "parliament_id": row["parliament"]["id"],
            "election_id": self.get_election_id(row),
            "start_date": row["start_date_period"],
            "end_date": row["end_date_period"],
        }

    def get_election_id(self, row):
        """Determine the election ID for a legislative period"""
        previous_period = row.get("previous_period")
        fixed_election_id = PREVIOUS_PERIOD_MAP.get(row["id"])
        return (fixed_election_id or previous_period["id"]) if previous_period else None

    def create_instance(self, data):
        parliament_id = data.pop("parliament_id")
        parliament = self.get_parliament(parliament_id)

        election_id = data.pop("election_id")
        election = self.get_election(election_id)

        return LegislativePeriod(parliament=parliament, election=election, **data)

    def get_election(self, election_id):
        """Get the election instance and check if it's already linked to another period"""
        if not election_id:
            return None

        election = self.elections.get(election_id)

        # Check if election is already linked to another period.
        if election and hasattr(election, "legislative_period"):
            msg = f"Election {election} is already linked to legislative period {election.legislative_period}"
            raise ImportError(msg)

        return election


class MainModelImporter(AbgeordnetenwatchDataImporter):
    """Class for main models that require create/update logic"""

    def __init__(self, model, entity_type):
        super().__init__(model, entity_type)
        self.stats = ImportStatsCollection()
        self.existing = self.model.objects.in_bulk(field_name=self.id_field)

    @transaction.atomic
    def create_or_update_instances(self):
        for aw_id, data in self.obj_data.items():
            if aw_id in self.existing:
                self.update_instance(self.existing[aw_id], data)
            else:
                self.create_instance(data)

        self.stats.log_summary(self.model)

    def update_instance(self, instance, data):
        changed = False

        for field, value in data.items():
            old_value = getattr(instance, field)

            # Do not overwrite existing values.
            if (not old_value) and (not equals(old_value, value)):
                setattr(instance, field, value)
                changed = True

        if changed:
            instance.save()
            self.stats.track_updated(self.model, instance.last_synced_state, instance)

    def create_instance(self, data):
        instance = self.model.objects.create(**data)
        self.stats.track_created(self.model, instance)


class PoliticianImporter(MainModelImporter):
    """Class for importing politicians (persons) from abgeordnetenwatch"""

    def __init__(self, politician_ids=None):
        super().__init__(Person, ENTITY_TYPE_POLITICIANS)
        self.politician_ids = politician_ids

    def collect_data(self):
        extra_params = self.get_extra_params()

        if self.politician_ids:
            missing_ids = set(self.politician_ids) - set(self.existing.keys())

            if not missing_ids:
                return

            extra_params["id[in]"] = json.dumps(list(missing_ids))

        for row in self.fetcher.iter_rows(extra_params):
            self.process_row(row)

    def process_row(self, row):
        self.obj_data[row["id"]] = {
            "aw_id": row["id"],
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "title": row.get("field_title") or "",
            "wikidata_id": row.get("qid_wikidata"),
        }


class AffiliationImporter(MainModelImporter):
    """Class for importing affiliations (candidacies and mandates)"""

    def __init__(self, role_uuid, role_type_name, periods_model):
        super().__init__(Affiliation, ENTITY_TYPE_CANDIDACIES_MANDATES)
        if not role_uuid:
            msg = f"No {role_type_name} role UUID configured for abgeordnetenwatch.de {role_type_name} import"
            raise ImportError(msg)

        self.role_uuid = role_uuid
        self.role_type_name = role_type_name

        self.role = Role.objects.filter(sync_uuid=self.role_uuid).first()
        if not self.role:
            msg = f"Role with sync UUID {self.role_uuid} not found"
            raise ImportError(msg)

        self.periods_model = periods_model
        self.periods = {}

    def init_related_entities(self):
        super().init_related_entities()
        self.periods = self.periods_model.objects.in_bulk(field_name="aw_id")

    def collect_data(self):
        extra_params = self.get_extra_params()

        for rows in self.fetcher.iter_pages(extra_params):
            self.process_rows(rows)

    def process_rows(self, rows):
        persons = self.get_persons(rows)

        for row in rows:
            period_id = row["parliament_period"]["id"]
            period = self.get_period(period_id)
            person = persons.get(row["politician"]["id"])
            organization = period.parliament.fraction

            self.obj_data[row["id"]] = {
                "aw_id": row["id"],
                "person_id": person.id,
                "organization_id": organization.id,
                "role_id": self.role.id,
                "start_date": row["start_date"] or period.start_date,
                "end_date": filter_future_date(row["end_date"] or period.end_date),
                "comment": row["info"] or "",
            }

        return len(rows)

    def get_persons(self, rows):
        """Get politician data and make sure the corresponding Person instances exist"""
        politician_ids = {row["politician"]["id"] for row in rows}
        politician_importer = PoliticianImporter(politician_ids)
        politician_importer.run()
        self.stats.merge(politician_importer.stats)

        return Person.objects.in_bulk(politician_ids, field_name="aw_id")

    def get_period(self, period_id):
        """Get the legislative period instance by its abgeordnetenwatch ID"""
        period = self.periods.get(period_id)

        if not period:
            msg = f"{self.periods_model.__name__} with abgeordnetenwatch ID {period_id} not found"
            raise ImportError(msg)

        return period


class CandidacyImporter(AffiliationImporter):
    """Importer for candidacies from abgeordnetenwatch"""

    def __init__(self):
        super().__init__(CANDIDATE_ROLE_UUID, "candidacy", Election)

        if not PARTY_ID:
            msg = "No party ID configured for abgeordnetenwatch.de candidacy import"
            raise ImportError(msg)

    def get_extra_params(self):
        return {
            "type": "candidacy",
            "party": PARTY_ID,
            "current_on": "all",
        }


class MandateImporter(AffiliationImporter):
    """Importer for mandates from abgeordnetenwatch"""

    def __init__(self):
        super().__init__(MANDATE_ROLE_UUID, "mandate", LegislativePeriod)

        if not FRACTIONS:
            msg = "No fractions configured for abgeordnetenwatch.de mandate import"
            raise ImportError(msg)

    def collect_data(self):
        """
        Collect mandate data for all fractions

        Filtering mandates by a set of fractions via the API does not seem to work
        (if their short names are given) so we have to do it one fraction at a time.
        """
        for fraction in FRACTIONS:
            self.collect_fraction_data(fraction)

    def collect_fraction_data(self, fraction):
        """Collect mandate data for a specific fraction"""
        extra_params = {
            "type": "mandate",
            "fraction": fraction,
            "current_on": "all",
        }

        rows = list(self.fetcher.iter_rows(extra_params))
        if not rows:
            return

        self.process_rows(rows)


class AbgeordnetenwatchImporter:
    """Master importer that coordinates all individual importers"""

    def __init__(self, only_setup=False):
        self.stats = ImportStatsCollection()
        self.importers = [
            ParliamentImporter(),
            ElectionImporter(),
            LegislativePeriodImporter(),
        ]

        if not only_setup:
            self.importers.extend(
                [
                    CandidacyImporter(),
                    MandateImporter(),
                ]
            )

    @transaction.atomic
    def run(self):
        for importer in self.importers:
            importer.run()

            if hasattr(importer, "stats"):
                self.stats.merge(importer.stats)

    def log_stats(self):
        """Return collected stats from all importers"""
        return self.stats.to_dict()
