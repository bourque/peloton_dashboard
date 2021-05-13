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
        self.session = requests.Session()

    def _get_url(self, url, verbose=True):
        """Send a request to the peloton API for the given URL

        Parameters
        ----------
        url : str
            The URL to ping.
        verbose : bool
            If ``True``, print out a message that indicates which URL
            is being used.
        """

        if verbose:
            print(f'\tGathering data for {url}')

        data = self.session.get(url, timeout=30).json()
        return data

    def _get_user_data(self):
        """Return user data

        Returns
        -------
        user_data : dict
            A dictionary of user data
        """

        user_url = f'{self.base_url}/api/me'
        user_data = self._get_url(user_url)

        return user_data

    def get_workout_metadata(self, workouts):
        """Build a list of workouts with ride and instructor metadata
        added.

        Parameters
        ----------
        workouts : list
            A list of workouts

        Returns
        -------
        workout_metadata_list : list
            A list of workouts with supplemental metadata
        """

        workout_metadata_list = []
        for workout in workouts:
            workout_id = workout['id']
            workout_url = f'{self.base_url}/api/workout/{workout_id}?joins=ride,ride.instructor'
            print(f'\tGathering data for {workout["ride"]["title"]}')
            data = self._get_url(workout_url, verbose=False)
            workout_metadata_list.append(data)

        return workout_metadata_list

    def get_workouts(self):
        """Return a list of workouts

        Returns
        -------
        workouts : dict
            A dictionary of workout data.
        """

        # Determine total number of workouts and number of pages
        user_data = self._get_user_data()
        total_num_workouts = user_data['total_workouts']
        page_limit = 100
        total_pages = total_num_workouts // page_limit
        remainder = total_num_workouts % page_limit

        # Iterate through pages, gather workouts into list
        workouts = []
        page_number = 0
        while page_number < total_pages:
            full_workout_url = f'{self.base_url}/api/user/{self.user_id}/workouts?sort_by=-created&page={page_number}&limit={page_limit}&joins=ride'
            workouts.extend(self._get_url(full_workout_url)['data'])
            page_number += 1
        if remainder != 0:
            full_workout_url = f'{self.base_url}/api/user/{self.user_id}/workouts?sort_by=-created&page={page_number}&limit={page_limit}&joins=ride'
            workouts.extend(self._get_url(full_workout_url)['data'])

        return workouts


    def login(self):
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
        try:
            response = self.session.post(
                login_url, json=auth_payload, headers=self.headers, timeout=30
            ).json()
            self.user_id = response["user_id"]
            print(f"\nLogin successful for user {self.user_id}\n")
        except KeyError:
            print("Login failed")


if __name__ == "__main__":

    dashboard = Dashboard()
    dashboard.login()

    workouts = dashboard.get_workouts()
    workout_metadata_list = dashboard.get_workout_metadata(workouts)

