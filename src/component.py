import csv
import logging
import requests
import json
import sys

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_USERNAME = 'username'
KEY_PASSWORD = '#password'
KEY_SERVER_HOSTNAME = 'hostname'
KEY_APPLICATIONS = 'applications'

REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_PASSWORD, KEY_APPLICATIONS, KEY_SERVER_HOSTNAME]
REQUIRED_IMAGE_PARS = []

DATE_FROM = '2000-01-01'


def login(email, password, hostname):
    response = requests.request(
        "POST",
        "https://%s/api/token/new" % (hostname),
        data=dict(
            email=email,
            password=password,
        )
    )

    if not response.status_code == 200:
        logging.error("Unable to login to Sirius API")
        sys.exit(1)

    return json.loads(response.text)


def get_data(token, hostname, params):
    response = requests.request(
        "GET",
        "https://%s/api/dashboard/getRatingsInTime" % (hostname),
        params=params,
        headers={
            'Content-Type': "application/json",
            'cache-control': "no-cache",
            "Authorization": "Bearer " + token["access"],
        }
    )

    if not response.status_code == 200:
        logging.error("Unable to post data: %s" % (response.text))
        sys.exit(2)

    return response


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        if len(self.configuration.tables_output_mapping) != 1:
            logging.error("Output table mapping with one entry is required")
            sys.exit(1)

        token = login(
            params[KEY_USERNAME],
            params[KEY_PASSWORD],
            params[KEY_SERVER_HOSTNAME]
        )

        applications = params[KEY_APPLICATIONS].split(",")

        resp_google = get_data(
            token,
            params[KEY_SERVER_HOSTNAME],
            {
                'application': applications,
                'dateFrom': DATE_FROM,
                'platform': 'google'
            }
        )
        ratings_google = json.loads(resp_google.text).get('ratings')

        resp_apple = get_data(
            token,
            params[KEY_SERVER_HOSTNAME],
            {
                'application': applications,
                'dateFrom': DATE_FROM,
                'platform': 'apple'
            }
        )
        ratings_apple = json.loads(resp_apple.text).get('ratings')

        records = []
        for app in ratings_apple + ratings_google:
            for r in app['ratings']:
                records.append({
                    'platform': app['app']['platform'],
                    'app_name': app['app']['label'],
                    'date': r['date'],
                    'stars1': r['stars1'],
                    'stars2': r['stars2'],
                    'stars3': r['stars3'],
                    'stars4': r['stars4'],
                    'stars5': r['stars5']
                })

        result_filename = self.configuration.tables_output_mapping[0]['source']
        table = self.create_out_table_definition(
            result_filename,
            primary_key=['app_name', 'platform', 'date']
        )

        with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(
                out_file,
                fieldnames=['app-name', 'platform', 'date', 'stars1', 'stars2', 'stars3', 'stars4', 'stars5']
            )
            writer.writeheader()
            writer.writerows(records)

        self.write_manifest(table)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
