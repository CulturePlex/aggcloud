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
        self._file_path = file_path
        # Nodes settings
        # Let's extract the types and the affected columns
        nodetypes = data['nodes']
        self._nodetypes = {}
        self._nodes_ids = {}
        self._nodetypes_correctly = {}
        for nodetype in nodetypes:
            type = nodetype['slug']
            id = nodetype['id']
            self._nodes_ids[type] = id
            self._nodetypes[type] = []
            for key, val in nodetype['properties'].iteritems():
                self._nodetypes[type].append(val)
                self._nodetypes_correctly[val] = key

        # Variables to format the data
        # Properties_index will contain the ids for the columns for each type
        self._properties_index = {}
        # Variables to populate the data
        self._nodes = {}
        self._relationships = {}

        self._api = API(token=self._token, graph_slug=self._graph)

    def format_data_columns(self):
        """
        We format the headers of the CSV to get the index to treat for
        each type
        """
        csv_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")

        # The first line is the header, our properties
        columns = csv_reader.next()
        column_index = 0
        for prop in columns:
            for key, val in self._nodetypes.iteritems():
                if prop in val:
                    try:
                        self._properties_index[key].append(
                            column_index)
                    except:
                        self._properties_index[key] = []
                        self._properties_index[key].append(
                            column_index)
            column_index = column_index + 1

    def format_data_nodes(self):
        csv_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
        # We read the header, to avoid the columns
        csv_reader.next()
        # The rest of the lines are data.
        # Inside this data we have relationships too.
        # We need to discover them to organize the data correctly.
        try:
            temp_data = csv_reader.next()
            while temp_data:
                # We need to store the data in a dict
                for key, val in self._properties_index.iteritems():
                    temp_node = []
                    for index in val:
                        temp_value = temp_data[index]
                        temp_node.append(temp_value)
                    try:
                        if temp_node not in self._nodes[key]:
                            self._nodes[key].append(temp_node)
                    except:
                        self._nodes[key] = []
                        if temp_node not in self._nodes[key]:
                            self._nodes[key].append(temp_node)
                temp_data = csv_reader.next()
        except:
            pass
        # Once we have our nodes mapped, let's build the temp csv files
        for key, val in self._nodes.iteritems():
            csv_file_path = './temp/' + key + '.csv'
            csv_file = open(csv_file_path, 'w')

            csv_headers_basics = ['id', 'type']
            # Let's get the headers correctly
            csv_headers = []
            bad_headers = self._nodetypes[key]
            for header in bad_headers:
                csv_header = self._nodetypes_correctly[header]
                csv_headers.append(csv_header)
            csv_headers_basics.extend(csv_headers)
            csv_headers = ",".join(csv_headers_basics)
            csv_file.write(csv_headers)
            csv_file.write("\n")

            nodes = self._nodes[key]
            node_id = 1
            for node in nodes:
                node_basics = [str(node_id), key]
                node_basics.extend(node)
                node = ",".join(node_basics)
                csv_file.write(node)
                csv_file.write("\n")
                node_id += 1

    def populate_nodes(self):
        for key, val in self._nodes_ids.iteritems():
            csv_file_path = './temp/' + key + '.csv'
            csv_file = open(csv_file_path, 'r')
            csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
            columns = csv_reader.next()
            # We need to check if we need to get or create the nodes
            if val == 'create':
                try:
                    nodes = []
                    nodetype = key
                    temp_node_data = csv_reader.next()
                    while temp_node_data:
                        # We need to store the data in a dict
                        column_index = 0
                        temp_node = {}
                        for elem in temp_node_data:
                            temp_node[columns[column_index]] = elem
                            column_index += 1
                        nodes.append(temp_node)
                        temp_node_data = csv_reader.next()
                except:
                    pass

                nodes_ids = self._api.post_nodes(nodetype, params=nodes)
                # Once we have the ids, we restart the csv files
                # TODO
            elif val == 'get_or_create':
                # We need to set up methods to filter by value
                # TODO
                pass

        print nodes_ids

    def populate_data(self):
        # We check if we need to format the data yet
        self.format_data_columns()
        self.format_data_nodes()
        self.populate_nodes()
