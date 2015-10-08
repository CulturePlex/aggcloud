# -*- coding: utf-8 -*-
try:
    import ujson as json
except ImportError:
    import json  # NOQA
import unicodecsv

from sylvadbclient import API

CONFIG_FILE = 'config.json'


class SylvaApp(object):

    def __init__(self, file_path):
        # We load the config.json file to set up variables
        with open(CONFIG_FILE) as data_file:
            data = json.load(data_file)

        # Graph and user settings
        self._token = data['graph_settings']['token']
        self._graph = data['graph_settings']['graph']

        # Schema settings
        self._nodetype = data['node_settings']['nodetype']

        self._relationships_columns = []
        self._relationshiptypes = {}
        temp_relationship_types = data['relationship_settings']

        for rel_type in temp_relationship_types:
            key = rel_type['relationship_label']
            value = rel_type['relationship_direction']

            self._relationships_columns.append(key)
            self._relationshiptypes[key] = value

        # Variables to format the data
        self._properties = []

        # Variables to populate the data
        self._nodes = []
        self._relationships = []

        self._file_path = file_path

        self._api = API(token=self._token, graph_slug=self._graph)

    def format_data(self):
        csv_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")

        # The first line is the header, our properties
        self._properties = csv_reader.next()

        # The rest of the lines are data.
        # Inside this data we have relationships too.
        # We need to discover them to organize the data correctly.
        try:
            temp_data = csv_reader.next()
            while temp_data:
                # We need to store the data in a dict
                temp_node = {}
                value_index = 0

                for prop in self._properties:
                    # Here, we need to differentiate between property
                    # columns and relationship columns
                    if prop in self._relationships_columns:
                        temp_relationship = {}
                        direction = self._relationshiptypes[prop]

                        # Here we need an API request
                        node_related_id = temp_data[value_index]

                        temp_relationship['node_id'] = ""
                        temp_relationship['node_related_id'] = node_related_id
                        temp_relationship['direction'] = direction
                        temp_relationship['label'] = prop

                        self._relationships.append(temp_relationship)
                    else:
                        temp_node[prop] = temp_data[value_index]
                    value_index = value_index + 1

                self._nodes.append(temp_node)
                temp_data = csv_reader.next()
        except:
            pass

    def populate_data(self):
        # We check if we need to format the data yet
        no_properties = len(self._properties) == 0
        no_data = len(self._nodes) == 0

        if no_properties or no_data:
            self.format_data()

        # Now, we populate our graph with the data
        # Populate nodes
        nodes_ids = self._api.post_nodes(self._nodetype, params=self._nodes)
        print "Nodes created!"

        relationships = {}
        for relationship_type in self._relationships_columns:
            relationships[relationship_type] = []

        row_index = 0
        for node_id in nodes_ids:
            relationship = {}
            try:
                temp_relationship = self._relationships[row_index]

                direction = temp_relationship['direction']
                label = temp_relationship['label']
                node_related_id = temp_relationship['node_related_id']

                # Fields needed for the relationship:
                # - source_id
                # - target_id
                # - label
                if direction == 'target':
                    relationship['source_id'] = node_related_id
                    relationship['target_id'] = node_id
                else:
                    relationship['source_id'] = node_id
                    relationship['target_id'] = node_related_id
                relationship['label'] = label

                relationships[label].append(relationship)
            except:
                pass
            row_index = row_index + 1

        for key, val in relationships.iteritems():
            relationship_type = key
            self._api.post_relationships(relationship_type, params=val)
            print "Relationships created!"
