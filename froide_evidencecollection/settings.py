import uuid

from froide.settings import Test as FroideTest


class Test(FroideTest):
    INSTALLED_APPS = FroideTest.INSTALLED_APPS.default + [
        "cms",
        "menus",
        "froide_evidencecollection",
    ]

    CMS_CONFIRM_VERSION4 = True

    DATABASES = {
        "default": {
            "ENGINE": "django.contrib.gis.db.backends.postgis",
            "NAME": "froide_evidencecollection",
            "USER": "froide_evidencecollection",
            "PASSWORD": "froide_evidencecollection",
            "HOST": "localhost",
            "PORT": "5432",
        }
    }

    FROIDE_EVIDENCECOLLECTION_CONFIG = {
        # Do not restrict selectable regions for tests.
        # "selectable_regions": {
        #    "ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
        # },
    }

    FROIDE_EVIDENCECOLLECTION_ABGEORDNETENWATCH_CONFIG = {
        "mandate_role_uuid": uuid.uuid4(),  # "Abgeordnete*r"
        "candidate_role_uuid": uuid.uuid4(),  # "Kandidatur"
        "party_id": 9,  # AfD
        "fractions": ["AfD"],
    }
