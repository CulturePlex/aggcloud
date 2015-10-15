# -*- coding: utf-8 -*-
try:
    import ujson as json
except ImportError:
    import json  # NOQA
import os
import unicodecsv

from sylvadbclient import API

TEMP_FOLDER = './temp/'
CONFIG_FILE = 'config.json'
CREATE = 'create'
GET_OR_CREATE = 'get_or_create'


class SylvaApp(object):

    def __init__(self, file_path):
        """
        Loading the rules into data structures for an easily treatment
        """
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
        self._rel_properties = {}
        self._nodes_ids = {}
        self._nodetypes_mapping = {}
        for nodetype in nodetypes:
            type = nodetype['slug']
            id = nodetype['id']
            rel_property = nodetype['rel_property']
            self._nodes_ids[type] = id
            self._rel_properties[type] = rel_property
            self._nodetypes[type] = []
            for key, val in nodetype['properties'].iteritems():
                self._nodetypes[type].append(val)
                self._nodetypes_mapping[val] = key

        # Relationships settings
        reltypes = data['relationships']
        self._reltypes = {}
        for reltype in reltypes:
            type = reltype['slug']
            source = reltype['source']
            target = reltype['target']

            relationship = {}
            relationship[source] = 'source'
            relationship[target] = 'target'
            self._reltypes[type] = relationship

        # Variables to format the data
        # Properties_index will contain the ids for the columns for each type
        self._properties_index = {}
        # Variable to store the indices to include in the relationships file
        self._relationships_headers = []
        self._relationships_index = {}
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
        """
        We format the nodes data into their respective csv files
        """
        csv_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
        # We read the header, to avoid the columns
        csv_reader.next()
        # The rest of the lines are data
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
                        self._nodes[key].append(temp_node)
                temp_data = csv_reader.next()
        except:
            pass
        # Once we have our nodes mapped, let's build the temp csv files
        for key, val in self._nodes.iteritems():
            csv_file_path = os.path.join(TEMP_FOLDER, key + '.csv')
            csv_file = open(csv_file_path, 'w')

            csv_headers_basics = ['id', 'type']
            # Let's get the headers correctly
            csv_headers = []
            bad_headers = self._nodetypes[key]
            for header in bad_headers:
                csv_header = self._nodetypes_mapping[header]
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
        """
        Populate the nodes data into SylvaDB
        """
        for key, val in self._nodes_ids.iteritems():
            csv_file_path = os.path.join(TEMP_FOLDER, key + '.csv')
            csv_file = open(csv_file_path, 'r')
            csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
            columns = csv_reader.next()

            try:
                # We are going to save the nodes in a list of dicts
                # to store the data
                nodes = []
                # And in a list of lists, to dump the data in the csv
                # TODO - PROBLEM WITH ORDER
                nodes_list = []
                nodetype = key
                temp_node_data = csv_reader.next()
                while temp_node_data:
                    # We need to store the data in a dict to post the data
                    column_index = 0
                    temp_node = {}
                    temp_node_list = []
                    for elem in temp_node_data:
                        temp_node[columns[column_index]] = elem
                        temp_node_list.append(elem)
                        column_index += 1
                    nodes.append(temp_node)
                    nodes_list.append(temp_node_list)
                    temp_node_data = csv_reader.next()
            except:
                pass

            # We restart the csv files adding the remote id
            csv_file_path = os.path.join(TEMP_FOLDER, key + '_new_ids.csv')
            csv_file = open(csv_file_path, 'w')
            columns.append("remote_id")
            columns_str = ",".join(columns)
            csv_file.write(columns_str)
            csv_file.write("\n")

            # We need to check if we need to get or create the nodes
            nodes_ids = []
            if val == GET_OR_CREATE:
                for node in nodes_list:
                    try:
                        property_type = self._rel_properties[nodetype]
                        # Let's get the value for the property
                        try:
                            property_index = columns.index(property_type)
                        except:
                            property_index = 0
                        results = self._api.filter_node(
                            nodetype, property_type, node[property_index])
                        remote_id = str(results['results'][0]['id'])
                        nodes_ids.append(remote_id)
                    except IndexError:
                        break
                nodes_ids = self._api.post_nodes(nodetype, params=nodes)
            if val == CREATE:
                nodes_ids = self._api.post_nodes(nodetype, params=nodes)

            id_index = 0
            for node in nodes_list:
                node_values = node
                property_type = self._rel_properties[nodetype]
                # Let's get the value for the property
                try:
                    property_index = columns.index(property_type)
                except:
                    property_index = 0
                remote_id = str(nodes_ids[id_index])
                self._relationships_index[node_values[property_index]] = (
                    remote_id)
                if nodetype not in self._relationships_headers:
                    self._relationships_headers.append(nodetype)
                node_values.append(remote_id)
                node_values = ",".join(node_values)
                csv_file.write(node_values)
                csv_file.write("\n")
                id_index += 1

    def format_relationships(self):
        csv_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
        # Headers useless read
        csv_reader.next()
        results = []
        try:
            temp_data = csv_reader.next()
            while temp_data:
                row = []
                for elem in temp_data:
                    try:
                        remote_id = self._relationships_index[elem]
                        row.append(remote_id)
                    except KeyError:
                        pass
                results.append(row)
                temp_data = csv_reader.next()
        except:
            pass

        csv_file_path = os.path.join(TEMP_FOLDER, '_relationships.csv')
        csv_file = open(csv_file_path, 'w')
        columns = self._relationships_headers
        columns_str = ",".join(columns)
        csv_file.write(columns_str)
        csv_file.write("\n")

        id_index = 0
        for row in results:
            node_values = ",".join(row)
            csv_file.write(node_values)
            csv_file.write("\n")
            id_index += 1

    def format_data_relationships(self):
        csv_file_path = os.path.join(TEMP_FOLDER, '_relationships.csv')
        csv_file = open(csv_file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")

        # First we get the index for each type (they are the headers)
        columns_indexes = {}
        columns = csv_reader.next()
        column_index = 0
        for prop in columns:
            columns_indexes[prop] = column_index
            column_index = column_index + 1

        # And next, we get the values to create the relationships
        relationships_indexes = {}
        try:
            temp_data = csv_reader.next()
            while temp_data:
                # We need to store the data in a dict
                for key, val in columns_indexes.iteritems():
                    index = val
                    temp_value = temp_data[index]
                    try:
                        relationships_indexes[key].append(temp_value)
                    except:
                        relationships_indexes[key] = []
                        relationships_indexes[key].append(temp_value)
                temp_data = csv_reader.next()
        except:
            pass

        # We create a dictionary containing the rows
        for key, val in self._reltypes.iteritems():
            csv_file_path = os.path.join(TEMP_FOLDER, key + '.csv')
            csv_file = open(csv_file_path, 'w')
            columns = ['source_id', 'target_id', 'type']
            columns_str = ",".join(columns)
            csv_file.write(columns_str)
            csv_file.write("\n")

            rows = {}
            for key_t, val_t in val.iteritems():
                # We get the elements for the type
                ids = relationships_indexes[key_t]
                # We get the direction of the type
                direction = val_t
                rows[direction] = ids

            try:
                number_rows = len(rows.values()[0])
            except:
                number_rows = 0

            row_index = 0
            while row_index < number_rows:
                source = rows['source'][row_index]
                target = rows['target'][row_index]
                type = key
                temp_row = [source, target, type]
                temp_row_str = ",".join(temp_row)
                csv_file.write(temp_row_str)
                csv_file.write("\n")
                row_index += 1

    def populate_relationships(self):
        pass

    def populate_data(self):
        # We check if we need to format the data yet
        # self.format_data_columns()
        # self.format_data_nodes()
        # self.populate_nodes()

        # self.format_relationships()
        # self.format_data_relationships()
        # self.populate_relationships()
        pass
