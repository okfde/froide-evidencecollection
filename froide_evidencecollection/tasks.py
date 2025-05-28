from googleapiclient.http import HttpError

from . import models

try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    GSHEET_AVAILABLE = True
except ImportError:
    GSHEET_AVAILABLE = False

import datetime
import logging
import time
from itertools import zip_longest

from django.conf import settings
from django.utils import timezone

from froide.celery import app as celery_app
from froide_evidencecollection.importer import NocoDBImporter

logger = logging.getLogger(__name__)


def get_object_data(sheet_config, model_class, row, object=None):
    object_data = {}
    for key, value in sheet_config["field_map"].items():
        if isinstance(value, str):
            object_data[value] = row[key]
        elif value["type"] == "foreign_key":
            field = getattr(model_class, value["field_name"])
            try:
                obj = field.get_queryset().get(**{value["match_field"]: row[key]})
                object_data[value["field_name"]] = obj
            except field.field.related_model.DoesNotExist:
                if not field.field.null:
                    return
        elif value["type"] == "date":
            if not isinstance(row[key], int):
                return
            excel_base_date = datetime.date(1899, 12, 30)
            object_data[value["field_name"]] = excel_base_date + datetime.timedelta(
                days=row[key]
            )
        elif value["type"] in ["store_true_date", "store_false_date"]:
            target = value["type"] == "store_true_date"
            if row[key] == target:
                if not object or object and not getattr(object, value["field_name"]):
                    object_data[value["field_name"]] = timezone.now()
            else:
                if object and getattr(object, value["field_name"]):
                    object_data[value["field_name"]] = None
        else:
            raise Exception(f"{key}, {value}")
    return object_data


def get_sheet_service(config):
    creds = Credentials.from_service_account_info(config["service_account"])
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    return sheet


def get_sheet_data(config, sheet):
    result = (
        sheet.values()
        .batchGet(
            spreadsheetId=config["spreadsheet"],
            ranges=[x["sheet_name"] for x in config["sheets"]],
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )
    return result


@celery_app.task(name="froide_evidencecollection.import_evidence_gsheet")
def import_evidence_gsheet(config=None, ignore_existing_ids=False):
    assert GSHEET_AVAILABLE, "google sheets api client must be installed"
    if config is None:
        config = settings.FROIDE_EVIDENCECOLLECTION_GSHEET_IMPORT_CONFIG

    sheet = get_sheet_service(config)
    try:
        result = get_sheet_data(config, sheet)
    except HttpError:
        logger.warning("Failed to get sheet data, returning")
        return

    for sheet_config, sheet_data in zip(
        config["sheets"], result["valueRanges"], strict=False
    ):
        model_class = getattr(models, sheet_config["model_name"])

        headers, *data = sheet_data["values"]
        id_idx = headers.index("ID")
        id_coll = chr(ord("A") + id_idx)

        known_ids = [row[id_idx] for row in data if row[id_idx]]
        model_class.objects.exclude(pk__in=known_ids).delete()

        for i, row in enumerate(data):
            row = dict(zip_longest(headers, row, fillvalue=""))
            if not row["ID"] or ignore_existing_ids:
                object_data = get_object_data(sheet_config, model_class, row)
                if object_data is None:
                    continue
                model = model_class.objects.create(**object_data)
                sheet.values().update(
                    spreadsheetId=config["spreadsheet"],
                    range=f"{sheet_config['sheet_name']}!{id_coll}{i+2}:{id_coll}{i+3}",
                    valueInputOption="RAW",
                    body={"values": [[str(model.pk)]]},
                ).execute()
                time.sleep(1)
            else:
                object = model_class.objects.get(pk=row["ID"])
                object_data = get_object_data(
                    sheet_config, model_class, row, object=object
                )
                if object_data is None:
                    continue
                for k, v in object_data.items():
                    setattr(object, k, v)
                object.save()


@celery_app.task(name="froide_evidencecollection.import_evidence_nocodb")
def import_evidence_nocodb():
    importer = NocoDBImporter()
    try:
        importer.run()
    except Exception as e:
        logger.error(f"Failed to import data from NocoDB: {e}")
        raise
