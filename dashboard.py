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

    - ``pandas``
    - ``requests``
"""

import datetime
from functools import reduce
from pytz import timezone

import json
import os

import pandas as pd
import requests


class APIHandler:
    """Methods and attributes dealing with gathering, combining, and
    cleaning data from the Peloton API.

    Attributes
    ----------
    base_url : str
        The base URL for the peloton API
    headers : dict
        Header content for API requests
    session : obj
        A session to be used to conenct to the Peloton API

    Methods
    -------
    _clean_data
        "Clean" a given ``pandas`` dataframe
    _convert_to_dataframe
        Converts a list of ``json`` objects to a ``pandas`` dataframe
    _get_url
        Sends a request to the Peloton API for a given url
    _get_user_data
        Retrieves the user data from the Peloton API
    get_workout_metadata
        Retrieves workout metadata from the Peloton API
    get_workouts
        Retrieves a list of workouts for a user from the Peloton API
    login()
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

    def _clean_data(self, dataframe):
        """'Clean' the given dataframe.

        By 'clean', I am referring to (1) convert peloton API time
        from UNIX timestamp to a ``datetime`` object, (2) create

        Parameters
        ----------
        dataframe : obj
            A ``pandas`` ``dataframe`` object containing Peloton API
            data

        Returns
        -------
        cleaned_dataframe : obj
            A 'cleaned' version of the given ``pandas`` ``dataframe``.
        """

        # Convert times to datetime objects
        for key in ['start_time', 'end_time', 'created_at', 'created', 'device_time_created_at']:
            conversion = {}
            for time in dataframe[key]:
                time_in_datetime = datetime.datetime.utcfromtimestamp(time)
                time_in_utc = time_in_datetime.replace(tzinfo=timezone('UTC'))
                time_in_est = time_in_utc.astimezone(timezone('US/Eastern'))
                conversion[time] = time_in_est
            dataframe[key].replace(conversion, inplace=True)

        # Create a dataframe from the ride key
        ride_data = list(dataframe['ride'])
        ride_dataframe = self._convert_to_dataframe(ride_data)

        # Create a dataframe from the instructor key
        instructor_keys = list(ride_dataframe['instructor'][0].keys())
        instructor_data = list(ride_dataframe['instructor'])
        instructor_data_cleaned = []
        for item in instructor_data:
            if item is None:
                bogus_data = {}
                for key in instructor_keys:
                    bogus_data[key] = None
                instructor_data_cleaned.append(bogus_data)
            else:
                instructor_data_cleaned.append(item)
        instructor_dataframe = self._convert_to_dataframe(instructor_data_cleaned)

        # Combine the three dataframes into one big one
        final_dataframe = pd.concat([dataframe, ride_dataframe, instructor_dataframe], axis=1)

        return final_dataframe

    def _convert_to_dataframe(self, data_list):
        """Convert the given list of ``json`` objects to a ``pandas``
        dataframe

        Parameters
        ----------
        data_list : list
            A list of ``json`` objects

        Returns
        -------
        dataframe : obj
            A ``pandas`` ``dataframe`` object that corresponds to the
            given data_list
        """

        dataframe = pd.DataFrame.from_records(data_list)
        return dataframe


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

    api_handler = APIHandler()

    api_handler.login()
    workouts = api_handler.get_workouts()
    workout_metadata_list = api_handler.get_workout_metadata(workouts)
    workout_dataframe = api_handler._convert_to_dataframe(workout_metadata_list)
    workout_dataframe = api_handler._clean_data(workout_dataframe)

    print(workout_dataframe)
