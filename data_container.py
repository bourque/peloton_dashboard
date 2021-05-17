#! /usr/env/bin python

"""This module contains methods for gathering data through the Peloton
API.

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


class DataContainer():
    """Methods and attributes dealing with gathering, combining, and
    cleaning data from the Peloton API.

    Attributes
    ----------
    base_url : str
        The base URL for the peloton API
    session : obj
        A session to be used to conenct to the Peloton API

    Methods
    -------
    _convert_to_dataframe
        Converts a list of ``json`` objects to a ``pandas`` dataframe
    _get_url
        Sends a request to the Peloton API for a given url
    get_workout_metadata_dataframe
        Retrieves workout metadata from the Peloton API
    get_workouts_dataframe
        Retrieves a list of workouts for a user from the Peloton API
    get_user_dataframe
        Retrieves the user data from the Peloton API
    login()
        Authenticate with the peloton API
    """

    def __init__(self):
        """Initialize the ``Dashboard`` class."""

        self.base_url = "https://api.onepeloton.com"
        self.session = requests.Session()

    def _convert_to_dataframe(self, data_list):
        """Convert the given list of ``json`` objects to a ``pandas``
        dataframe

        Parameters
        ----------
        data_list : list
            A list of ``json`` objects

        Returns
        -------
        dataframe : ``pandas.core.frame.DataFrame``
            A ``pandas`` ``dataframe`` object that corresponds to the
            given data_list
        """

        dataframe = pd.DataFrame.from_records(data_list)

        # Convert any keys that have UTC times to datetime objects
        affected_keys = 'start_time', 'end_time', 'created_at', 'created', 'device_time_created_at', 'foo'
        for key in affected_keys:
            if key in dataframe:
                conversion = {}
                for time in dataframe[key]:
                    time_in_datetime = datetime.datetime.utcfromtimestamp(time)
                    time_in_utc = time_in_datetime.replace(tzinfo=timezone('UTC'))
                    time_in_est = time_in_utc.astimezone(timezone('US/Eastern'))
                    conversion[time] = time_in_est
                dataframe[key].replace(conversion, inplace=True)

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

    def get_combined_dataframe(self):
        """Return a dataframe that contains all workout (meta)data
        (i.e. each workout with corresponding ride, instructor, and
        performace data).

        Returns
        -------
        combined_dataframe : ``pandas.core.frame.DataFrame``
            The combined dataframe
        """

        # Get the individual workout (meta)data dataframes
        workouts_dataframe = data_container.get_workouts_dataframe()
        rides_dataframe = data_container.get_rides_dataframe()
        workout_metadata_dataframe = data_container.get_workout_metadata_dataframe()

        # Combine the three dataframes into one big one
        combined_dataframe = pd.concat([workouts_dataframe, rides_dataframe, workout_metadata_dataframe], axis=1)

        # Order the dataframe in increasing time
        combined_dataframe = combined_dataframe.sort_index(ascending=False).reset_index()

        return combined_dataframe

    def get_rides_dataframe(self):
        """Return workout data from the ``ride`` API URL

        Returns
        -------
        user_dataframe : ``pandas.core.frame.DataFrame``
            A dataframe of the user data
        """

        workouts_dataframe = self.get_workouts_dataframe()
        workout_ids = workouts_dataframe['id'].values
        for workout_id in workout_ids[0:20]:
            ride_url = f'{self.base_url}/api/workout/{workout_id}?joins=ride,ride.instructor'

    def get_workouts_dataframe(self):
        """Return workout data from the ``workouts`` API URL

        Returns
        -------
        workouts_dataframe : ``pandas.core.frame.DataFrame``
            A dataframe of the workout data
        """

        # Determine total number of workouts and number of pages
        user_dataframe = self.get_user_dataframe()
        total_num_workouts = user_dataframe['total_workouts'].values[0]
        page_limit = 100
        total_pages = total_num_workouts // page_limit
        remainder = total_num_workouts % page_limit

        # Iterate through pages, gather workouts into list
        workouts = []
        page_number = 0
        while page_number < total_pages:
            full_workout_url = f'{self.base_url}/api/user/{self.user_id}/workouts?sort_by=-created&page={page_number}&limit={page_limit}'
            workouts.extend(self._get_url(full_workout_url)['data'])
            page_number += 1
        if remainder != 0:
            full_workout_url = f'{self.base_url}/api/user/{self.user_id}/workouts?sort_by=-created&page={page_number}&limit={page_limit}'
            workouts.extend(self._get_url(full_workout_url)['data'])

        workouts_dataframe = self._convert_to_dataframe(workouts)

        return workouts_dataframe

    def get_workout_metadata_dataframe(self):
        """Return workout metadata (i.e. ride, instructor, performace) from
        the ``ride``, ``instructor``, and ``performace`` API URLs

        Returns
        -------
        workout_metadata_dataframe : ``pandas.core.frame.DataFrame``
            A dataframe of the workout metadata
        """

        workout_metadata_list = []
        workouts_dataframe = self.get_workouts_dataframe()
        workout_ids = workouts_dataframe['id'].values
        for workout_id in workout_ids:
            workout_url = f'{self.base_url}/api/workout/{workout_id}?joins=ride,ride.instructor'
            workout_url_performance = f'{self.base_url}/api/workout/{workout_id}/performance_graph?every_n=10000'
            print(f'\tGathering data for workout {workout_id}')
            workout_data = self._get_url(workout_url, verbose=False)
            performance_data = self._get_url(workout_url_performance, verbose=False)
            data = {**workout_data, **performance_data}
            workout_metadata_list.append(data)

        workout_metadata_dataframe = self._convert_to_dataframe(workout_metadata_list)
        for col in workout_metadata_dataframe.columns:
            print(col)

        return workout_metadata_dataframe

    def get_user_dataframe(self):
        """Return user data from the ``me`` API URL

        Returns
        -------
        user_dataframe : ``pandas.core.frame.DataFrame``
            A dataframe of the user data
        """

        user_url = f'{self.base_url}/api/me'
        user_data = self._get_url(user_url)
        user_dataframe = self._convert_to_dataframe([user_data])

        return user_dataframe


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
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "peloton_dashboard",
        }

        # Start session and send request
        try:
            response = self.session.post(
                login_url, json=auth_payload, headers=headers, timeout=30
            ).json()
            self.user_id = response["user_id"]
            print(f"\nLogin successful for user {self.user_id}\n")
        except KeyError:
            print("Login failed")

def get_peloton_data():
    """A wrapper around methods of the ``data_container`` class to
    retrieve a complete ``pandas`` ``dataframe`` of data

    Returns
    -------
    data : ``pandas.core.frame.DataFrame``
        A ``pandas`` ``dataframe`` object containing Peloton metadata
    """

    data_container = DataContainer()
    data_container.login()
    data = data_container.get_combined_dataframe()

    return data
