# -*- coding: utf-8 -*-
try:
    import ujson as json
except ImportError:
    import json  # NOQA
import unicodecsv

# from sylvadbclient import Graph, API

CONFIG_FILE = 'config.json'


class SylvaApp(object):

    def __init__(self, file_path):
        "We load the config.json file to set up variables"
        with open(CONFIG_FILE) as data_file:
            data = json.load(data_file)

        self._user = data['user']
        self._password = data['password']
        self._graph = data['graph']

        self._properties = []
        self._data = []

        # api = API(auth=(self._user, self._password),
        #           graph_slug=self._graph)

        self._file_path = file_path

    def format_data(self):
        csv_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")

        # The first line is the header, our properties
        self._properties = csv_reader.next()

        # The rest of the lines are data
        try:
            temp_data = csv_reader.next()
            while temp_data:
                self._data.append(temp_data)
                temp_data = csv_reader.next()
        except:
            pass

    def populate_data(self):
        # We check if we need to format the data yet
        no_properties = len(self._properties) == 0
        no_data = len(self._data) == 0

        if no_properties or no_data:
            self.format_data()

        # Now, we populate our graph with the data
        print self._properties
        for row in self._data:
            print row
