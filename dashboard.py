#! /usr/env/bin python

"""This module contains classes and methods for building the Peloton
Dashboard

Use
---

    TBD

Authors
-------

    - Matthew Bourque

Dependencies
------------


"""

import json
import os
import requests


class Dashboard:
    """The main class of the ``dashboard`` module.

    Attributes
    ----------
    base_url : str
        The base URL for the peloton API
    headers : dict
        Header content for API requests

    Methods
    -------
    _login()
        Authenticate with the peloton API
    """

    def __init__(self):
        """Initialize the ``Dashboard`` class."""

        self.base_url = "https://api.onepeloton.com"
        self.headers = {
            "Content-Type": "application/json",
            "User-Agent": "peloton_dashboard",
        }

    def _login(self):
        """Authenticate with the peloton API.

        If authentication is sucessfull, the ``user_id`` is set.
        """

        # Get login credentials
        with open("config.json", "r") as f:
            credentials = json.load(f)

        # Build request
        login_url = os.path.join(self.base_url, "auth", "login")
        auth_payload = {
            "username_or_email": credentials["username"],
            "password": credentials["password"],
        }

        # Start session and send request
        session = requests.Session()
        try:
            response = session.post(
                login_url, json=auth_payload, headers=self.headers, timeout=5
            ).json()
            self.user_id = response["user_id"]
            print(f"Login sucessfull for user {self.user_id}")
        except KeyError:
            print("Login failed")


if __name__ == "__main__":

    dashboard = Dashboard()
    dashboard._login()
