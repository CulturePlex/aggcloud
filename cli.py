# -*- coding: utf-8 -*-
from collections import namedtuple
from datetime import datetime
import argparse
import castings
import hashlib
import imp
try:
    import ujson as json
except ImportError:
    import json  # NOQA
import os
import shutil
import unicodecsv

from sylvadbclient import API


# IO constants
APP_ROOT = os.path.dirname(__file__)
HISTORY_PATH = os.path.join(APP_ROOT, "history")

RULES_PATH = os.environ.get("RULES_PATH",
                            os.path.join(APP_ROOT, "rules.py"))
rules = imp.load_source('rules', RULES_PATH)
LOG_FILENAME = 'app.log'

# Rules constants
CREATE = 'create'
GET_OR_CREATE = 'get_or_create'

# Status codes
_statuses = [
    "RULES_LOADING", "API_CONNECTING", "CHECKING_TOKEN", "CHECKING_SCHEMA",
    "CSV_COLUMNS_FORMATTING", "DATA_NODES_FORMATTING", "DATA_NODES_DUMPING",
    "RELATIONSHIPS_PREPARING", "DATA_RELATIONSHIPS_FORMATTING",
    "DATA_RELATIONSHIPS_DUMPING", "EXECUTION_COMPLETED", "RESUMING_LOAD",
]
STATUS = namedtuple("Status", _statuses)(**dict([(s, s) for s in _statuses]))

# Batch size
BATCH_SIZE = 3


class SylvaApp(object):

    def __init__(self, file_path):
        """
        Loading the rules into data structures for an easily treatment
        """
        print("Hashing contents of CSV file...")  # This doesn't to to status
        file_hash = self._hash(file_path)
        self._history_path = os.path.join(HISTORY_PATH, file_hash)
        self._log_file_path = os.path.join(self._history_path, LOG_FILENAME)
        self._file_path = os.path.join(self._history_path,
                                       os.path.basename(file_path))
        if os.path.exists(self._file_path):
            self._status(STATUS.RESUMING_LOAD, "Resuming previous load...")
            # TODO: Resume the loading using the log file
        else:
            os.makedirs(self._history_path)
            shutil.copy(file_path, self._file_path)
        # We load the config.json file to set up variables
        self._status(STATUS.RULES_LOADING, "Loading rules for the graph...")
        # Settings
        self._token = rules.GRAPH_SETTINGS['token']
        self._graph = rules.GRAPH_SETTINGS['graph']
        schema_json = rules.SCHEMA
        self._schema = hashlib.sha1(schema_json).hexdigest()
        self._nodetypes = {}
        self._rel_properties = {}
        self._nodes_ids = {}
        self._nodetypes_mapping = {}
        self._nodetypes_casting_elements = {}
        self._headers_indexes = {}
        # Variables to format the data
        # Properties_index will contain the ids for the columns for each type
        self._headers = []
        self._properties_index = {}
        # Variable to store the indices to include in the relationships file
        self._relationships_headers = []
        self._relationships_index = {}
        # Variables to populate the data
        self._setup_nodetypes()
        self._setup_reltypes()
        self._status(STATUS.API_CONNECTING,
                     "Connecting with the SylvaDB API...")
        self._api = API(token=self._token, graph_slug=self._graph)

    def _setup_nodetypes(self):
        nodetypes = rules.NODES
        for nodetype in nodetypes:
            type = nodetype['slug']
            id = nodetype['id']
            rel_property = nodetype['rel_property']
            self._nodes_ids[type] = id
            self._rel_properties[type] = rel_property
            self._nodetypes[type] = []
            for key, val in nodetype['properties'].iteritems():
                # We check if we have a casting function defined
                if isinstance(val, (tuple, list)):
                    func = val[0]
                    params = val[1:]
                    for param in params:
                        self._nodetypes[type].append(param)
                        try:
                            casting_elem = {}
                            casting_elem[func] = params
                            if(casting_elem not in
                               self._nodetypes_casting_elements[type]):
                                    (self._nodetypes_casting_elements[type]
                                        .append(casting_elem))
                        except:
                            self._nodetypes_casting_elements[type] = []
                            casting_elem = {}
                            casting_elem[func] = params
                            if(casting_elem not in
                               self._nodetypes_casting_elements[type]):
                                    (self._nodetypes_casting_elements[type]
                                        .append(casting_elem))
                        self._nodetypes_mapping[param] = param
                else:
                    self._nodetypes[type].append(val)
                    self._nodetypes_mapping[val] = key

    def _setup_reltypes(self):
        # Relationships settings
        reltypes = rules.RELATIONSHIPS
        self._reltypes = {}
        self._rel_ids = {}
        for reltype in reltypes:
            type = reltype['slug']
            id = reltype['id']
            source = reltype['source']
            target = reltype['target']
            relationship = {}
            relationship[source] = 'source'
            relationship[target] = 'target'
            self._reltypes[type] = relationship
            self._rel_ids[type] = id

    def _hash(self, filename, blocksize=65536):
        _hash = hashlib.sha256()
        with open(filename, "rb") as f:
            for block in iter(lambda: f.read(blocksize), b""):
                _hash.update(block)
        return _hash.hexdigest()

    def _status(self, code, msg):
        """
        Log function
        """
        log_file = open(self._log_file_path, 'a+')
        date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file.write(u"{}: {}\n".format(code, date_time))
        print(msg)

    def _dump_nodes(self, csv_file, type, mode, nodetype, columns, nodes,
                    nodes_list):
        nodes_ids = []
        if mode == GET_OR_CREATE:
            node_index = 0
            for node in nodes_list:
                try:
                    property_type = self._rel_properties[nodetype]
                    # Let's get the value for the property
                    try:
                        property_index = columns.index(property_type)
                    except:
                        property_index = 0
                    results = self._api.filter_nodes(
                        nodetype,
                        params={property_type: node[property_index]}
                    )
                    remote_id = str(results['nodes'][0]['id'])
                    nodes_ids.append(remote_id)
                except IndexError:
                    node_id = self._api.post_nodes(
                        nodetype, params=[nodes[node_index]])
                    nodes_ids.extend(node_id)
                node_index += 1
        if mode == CREATE:
            temp_nodes_ids = (
                self._api.post_nodes(nodetype, params=nodes))
            nodes_ids.extend(temp_nodes_ids)
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
                type, remote_id)
            if nodetype not in self._relationships_headers:
                self._relationships_headers.append(nodetype)
            node_values.append(remote_id)
            node_values = ",".join(node_values)
            csv_file.write(node_values)
            csv_file.write("\n")
            id_index += 1

    def check_token(self):
        """
        We check the schema to allow the entire execution.
        """
        self._status(STATUS.CHECKING_TOKEN,
                     "Checking API token...")
        try:
            self._api.get_graph()
        except:
            raise ValueError(
                "There are problems connecting with the API."
                "Maybe your token isn't correct. Please, check it and "
                "restart the execution."
                "If the problem persists, please contact us :)")

    def check_schema(self):
        """
        We check the schema to allow the entire execution.
        """
        self._status(STATUS.CHECKING_SCHEMA,
                     "Checking schema...")
        temp_schema = json.dumps(self._api.export_schema())
        schema_hash = hashlib.sha1(temp_schema).hexdigest()

        if self._schema == schema_hash:
            pass
        else:
            raise ValueError(
                "The schema isn't correct. Please, check it and restart"
                "the execution.")

    def format_data_columns(self):
        """
        We format the headers of the CSV to get the index to treat for
        each type
        """
        self._status(STATUS.CSV_COLUMNS_FORMATTING,
                     "Formatting the CSV columns...")
        csv_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
        # The first line is the header, our properties
        columns = csv_reader.next()
        column_index = 0
        for prop in columns:
            self._headers.append(prop)
            for key, val in self._nodetypes.iteritems():
                if prop in val:
                    try:
                        self._properties_index[key].append(
                            column_index)
                    except:
                        self._properties_index[key] = []
                        self._properties_index[key].append(column_index)
            self._headers_indexes[prop] = column_index
            column_index += 1

    def format_data_nodes(self):
        """
        We format the nodes data into their respective csv files
        """
        self._status(STATUS.DATA_NODES_FORMATTING,
                     "Formatting data for nodes...")
        csv_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
        # We read the header, to avoid the columns
        csv_reader.next()
        # The rest of the lines are data
        csv_files = {}
        csv_file_node_id = {}
        csv_nodes_treated = []
        try:
            temp_data = csv_reader.next()
            while temp_data:
                for key, val in self._properties_index.iteritems():
                    # Create the node
                    temp_node = []
                    for index in val:
                        temp_value = temp_data[index]
                        temp_node.append(temp_value)
                    # Check if we have casting function
                    try:
                        casting_functions = (
                            self._nodetypes_casting_elements[key])
                    except KeyError:
                        casting_functions = []
                    csv_headers_castings = []
                    for casting_function in casting_functions:
                        for func, params in casting_function.iteritems():
                            # Let's get the index to get the values for params
                            params_values = []
                            for param in params:
                                param_index = self._headers_indexes[param]
                                param_value = temp_data[param_index]
                                params_values.append(param_value)
                            cast_func = getattr(castings, func)
                            csv_header, result = cast_func(*params_values)
                            csv_headers_castings.append(csv_header)
                            temp_node.append(result)
                    # We check the csv file needed
                    try:
                        csv_file = csv_files[key]
                    except KeyError:
                        csv_file_path = os.path.join(self._history_path,
                                                     "{}.csv".format(key))
                        csv_file = open(csv_file_path, 'w+')
                        csv_files[key] = csv_file
                        csv_file_node_id[key] = 1
                        node_id = csv_file_node_id[key]
                        # Let's get the headers correctly
                        csv_headers_basics = ['id', 'type']
                        csv_headers = []
                        headers_indexes = self._properties_index[key]
                        bad_headers = [
                            self._headers[i] for i in headers_indexes]
                        for header in bad_headers:
                            csv_header = self._nodetypes_mapping[header]
                            csv_headers.append(csv_header)
                        csv_headers_basics.extend(csv_headers)
                        csv_headers_basics.extend(csv_headers_castings)
                        csv_headers = ",".join(csv_headers_basics)
                        csv_file.write(csv_headers)
                        csv_file.write("\n")
                    # Let's add our node
                    if temp_node not in csv_nodes_treated:
                        node_basics = [str(node_id), key]
                        node_basics.extend(temp_node)
                        node = ",".join(node_basics)
                        csv_file.write(node)
                        csv_file.write("\n")
                        csv_nodes_treated.append(temp_node)
                        csv_file_node_id[key] += 1
                temp_data = csv_reader.next()
        except StopIteration:
            pass
        # Once we have our nodes mapped, let's close our csv files
        for key in csv_files.keys():
            csv_files[key].close()

    def populate_nodes(self):
        """
        Populate the nodes data into SylvaDB
        """
        self._status(STATUS.DATA_NODES_DUMPING,
                     "Dumping the data for nodes into SylvaDB...")
        for key, val in self._nodes_ids.iteritems():
            # We open the files to read and write
            csv_file_path_root = os.path.join(self._history_path,
                                              "{}.csv".format(key))
            csv_file_path_new = os.path.join(self._history_path,
                                             "{}_new_ids.csv".format(key))
            csv_file_root = open(csv_file_path_root, 'r')
            csv_file_new = open(csv_file_path_new, 'w+')
            csv_reader_root = unicodecsv.reader(csv_file_root,
                                                encoding="utf-8")
            columns = csv_reader_root.next()
            columns.append("remote_id")
            columns_str = ",".join(columns)
            csv_file_new.write(columns_str)
            csv_file_new.write("\n")
            try:
                # We are going to save the nodes in a list of dicts
                # to store the data
                nodes = []
                nodes_batch_limit = 0
                nodes_list = []
                nodetype = key
                temp_node_data = csv_reader_root.next()
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
                    nodes_batch_limit += 1
                    if nodes_batch_limit == BATCH_SIZE:
                        print("Dumping {} nodes...".format(len(nodes_list)))
                        self._dump_nodes(csv_file_new, key, val,
                                         nodetype, columns, nodes, nodes_list)
                        # We reset the structures
                        nodes = []
                        nodes_batch_limit = 0
                        nodes_list = []
                    temp_node_data = csv_reader_root.next()
            except StopIteration:
                print("Dumping {} nodes...".format(len(nodes_list)))
                self._dump_nodes(csv_file_new, key, val, nodetype, columns,
                                 nodes, nodes_list)

    def preparing_relationships(self):
        """
        Create the _relationships.csv files where we map all the relationships
        neccesary ids.
        """
        self._status(STATUS.RELATIONSHIPS_PREPARING,
                     "Preparing data for relationships...")
        csv_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
        # Headers useless read
        csv_reader.next()
        columns = []
        rows = {}
        try:
            temp_data = csv_reader.next()
            while temp_data:
                for elem in temp_data:
                    try:
                        reltype = self._relationships_index[elem][0]
                        remote_id = self._relationships_index[elem][1]
                        try:
                            rows[reltype].append(remote_id)
                        except KeyError:
                            rows[reltype] = []
                            rows[reltype].append(remote_id)
                    except KeyError:
                        pass
                temp_data = csv_reader.next()
        except:
            pass

        csv_file_path = os.path.join(self._history_path, '_relationships.csv')
        csv_file = open(csv_file_path, 'w+')
        columns = rows.keys()
        columns_str = ",".join(columns)
        csv_file.write(columns_str)
        csv_file.write("\n")
        values = rows.values()
        try:
            number_rows = len(values[0])
            number_cols = len(values)
        except:
            number_rows = 0
            number_cols = 0

        row_index = 0
        while row_index < number_rows:
            temp_row = []
            col_index = 0
            while col_index < number_cols:
                elem = values[col_index][row_index]
                temp_row.append(elem)
                col_index += 1
            temp_row_str = ",".join(temp_row)
            csv_file.write(temp_row_str)
            csv_file.write("\n")
            row_index += 1

    def format_data_relationships(self):
        """
        Using the _relationships.csv file, we format the data for the
        relationships
        """
        self._status(STATUS.DATA_RELATIONSHIPS_FORMATTING,
                     "Formatting data for relationships...")
        csv_file_path = os.path.join(self._history_path, '_relationships.csv')
        csv_file = open(csv_file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")

        csv_files = {}
        # First we get the index for each type (they are the headers)
        columns_indexes = {}
        columns = csv_reader.next()
        column_index = 0
        for prop in columns:
            columns_indexes[prop] = column_index
            column_index = column_index + 1

        try:
            temp_data = csv_reader.next()
            while temp_data:
                for key, val in self._reltypes.iteritems():
                    try:
                        csv_file = csv_files[key]
                    except:
                        csv_file_path = os.path.join(self._history_path,
                                                     "{}.csv".format(key))
                        csv_file = open(csv_file_path, 'w+')
                        csv_files[key] = csv_file
                        columns = ['source_id', 'target_id', 'type']
                        columns_str = ",".join(columns)
                        csv_file.write(columns_str)
                        csv_file.write("\n")
                    source = ""
                    target = ""
                    type = key
                    for key_t, val_t in val.iteritems():
                        data_index = columns_indexes[key_t]
                        if val_t == 'source':
                            source = temp_data[data_index]
                        elif val_t == 'target':
                            target = temp_data[data_index]
                    temp_row = [source, target, type]
                    temp_row_str = ",".join(temp_row)
                    csv_files[key].write(temp_row_str)
                    csv_files[key].write("\n")
                temp_data = csv_reader.next()
        except StopIteration:
            pass
        # Once we have our nodes mapped, let's close our csv files
        for key in csv_files.keys():
            csv_files[key].close()

    def populate_relationships(self):
        """
        Populate the relationships data into SylvaDB
        """
        self._status(STATUS.DATA_RELATIONSHIPS_DUMPING,
                     "Dumping the data for relationships into SylvaDB...")
        for key, val in self._rel_ids.iteritems():
            csv_file_path = os.path.join(self._history_path,
                                         "{}.csv".format(key))
            csv_file = open(csv_file_path, 'r')
            csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
            columns = csv_reader.next()

            try:
                relationships = []
                relationships_batch_limit = 0
                reltype = key
                temp_rel_data = csv_reader.next()
                while temp_rel_data:
                    # We need to store the data in a dict to post the data
                    column_index = 0
                    temp_rel = {}
                    for elem in temp_rel_data:
                        temp_rel[columns[column_index]] = elem
                        column_index += 1
                    relationships.append(temp_rel)
                    relationships_batch_limit += 1
                    if relationships_batch_limit == BATCH_SIZE:
                        print("Dumping {} relationships...".format(
                            len(relationships)))
                        if val == CREATE:
                            self._api.post_relationships(reltype,
                                                         params=relationships)
                        # We reset the structures
                        relationships = []
                        relationships_batch_limit = 0
                    temp_rel_data = csv_reader.next()
            except StopIteration:
                print("Dumping {} relationships...".format(len(relationships)))
                if val == CREATE:
                    self._api.post_relationships(reltype,
                                                 params=relationships)

    def populate_data(self):
        """
        Execute all the functions
        """
        # We check if we need to format the data yet
        try:
            self.check_token()
            self.check_schema()
            self.format_data_columns()
            self.format_data_nodes()
            self.populate_nodes()
            self.preparing_relationships()
            self.format_data_relationships()
            self.populate_relationships()
            self._status(STATUS.EXECUTION_COMPLETED, "Execution completed! :)")
        except ValueError as e:
            print e.args


def main():
    """
    Options to execute the app using the command line
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'file', help='CSV file used to dump the data into SylvaDB')

    args = parser.parse_args()
    file_path = args.file

    app = SylvaApp(file_path)
    app.populate_data()


if __name__ == '__main__':
    main()
