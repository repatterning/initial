"""Module special.py"""
import json
import sys

import boto3
import requests

import src.functions.secret


class Special:
    """
    Special
    """

    def __init__(self, connector: boto3.session.Session):
        """

        :param connector: A boto3 session instance, it retrieves the developer's <default> Amazon
                          Web Services (AWS) profile details, which allows for programmatic interaction with AWS.
        """

        # Hence
        self.__secret = src.functions.secret.Secret(connector=connector)

        # Headers
        self.__headers = self.__get_headers()

    def __get_headers(self) -> dict:
        """
        This function sets up an ephemeral data retrieval token dict via a client's key.

        :return:
        """

        token_url = 'https://timeseries.sepa.org.uk/KiWebPortal/rest/auth/oidcServer/token'
        access_key = self.__secret.exc(secret_id='HydrographyProject', node='sepa')
        headers =  {'Authorization':'Basic ' + access_key}
        response_token= requests.post(token_url, headers = headers, data = 'grant_type=client_credentials', timeout=600)
        access_token = response_token.json()['access_token']

        return {'Authorization':'Bearer ' + access_token}

    def __get_content(self, url: str) -> str:
        """

        :param url: A data set's uniform resource locator
        :return:
        """

        try:
            response = requests.get(url=url, headers=self.__headers, timeout=600)
            response.raise_for_status()
        except requests.exceptions.Timeout as err:
            raise err from err
        except Exception as err:
            raise err from err

        if response.status_code == 200:
            content = response.content.decode(encoding='utf-8')
            return content

        sys.exit(response.status_code)

    def exc(self, url: str) -> dict | list[dict]:
        """

        :param url: A data set's uniform resource locator
        :return:
        """

        content = self.__get_content(url=url)

        return json.loads(content)
