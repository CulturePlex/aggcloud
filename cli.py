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
                            os.path.join(APP_ROOT,
                                         "rules.py"))
rules = imp.load_source('rules', RULES_PATH)
LOG_FILENAME = 'app.log'

# Rules constants
CREATE = 'create'
GET_OR_CREATE = 'get_or_create'
SOURCE = 'source'
TARGET = 'target'
DEFAULT_FUNC = 'default'  # default function defined in castings

# Status codes
_statuses = [
    "RULES_LOADING", "API_CONNECTING", "CHECKING_TOKEN", "CHECKING_SCHEMA",
    "CSV_COLUMNS_FORMATTING", "DATA_NODES_FORMATTING", "DATA_NODES_DUMPING",
    "RELATIONSHIPS_PREPARING", "DATA_RELATIONSHIPS_FORMATTING",
    "DATA_RELATIONSHIPS_DUMPING", "EXECUTION_COMPLETED", "RESUMING_LOAD"
]
STATUS = namedtuple("Status", _statuses)(**dict([(s, s) for s in _statuses]))

# Batch size
BATCH_SIZE = 500


class SylvaApp(object):

    def __init__(self, file_path, batch_size=None):
        """
        Loading the rules into data structures for an easily treatment
        """
        print("Hashing contents of CSV file...")  # This doesn't to to status
        self.batch_size = BATCH_SIZE
        if batch_size:
            self.batch_size = int(batch_size)
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
        self._token = rules.GRAPH_SETTINGS['token']
        self._graph = rules.GRAPH_SETTINGS['graph']
        # Variables to manage nodetypes
        self._nodetypes = []
        self._nodetypes_id_label = {}
        self._nodetypes_graph_names = {}
        self._nodetypes_graph_slugs = {}
        self._nodetypes_rules_slugs = {}
        self._nodetypes_graph_ids = {}
        self._nodetypes_mode = {}
        self._nodetypes_casting = {}
        self._nodes_ids_mapping = {}
        # Variables to manage reltypes
        self._reltypes_rules_slugs = {}
        # Variables to format the data
        # List to store the headers from the csv. Lists maintain the order.
        self._headers = []
        self._rules_headers = []
        # Checking the connection with the API
        try:
            self._status(STATUS.API_CONNECTING,
                         "Connecting to the API...")
            self._api = API(token=self._token, graph_slug=self._graph)
            # Settings
            self._schema = json.loads(rules.SCHEMA)
            self._schema_id = self._api.get_graph()['schema']
            self._csv_columns_indexes = {}
        except:
            raise ValueError(
                "There are problems connecting to the API. "
                "Maybe the server is not available at this moment. "
                "Please, restart the execution once it is running. "
                "If the problem persists, please contact us.")

    def _setup_nodetypes(self):
        nodetypes = rules.NODES
        # Let's extract the slug directly from the graph using the API
        graph_nodetypes_slugs = {}
        try:
            graph_nodetypes = self._api.get_nodetypes()
            # We create the dictionary to map the correct slug
            for graph_nodetype in graph_nodetypes:
                graph_nodetypes_slugs[graph_nodetype['name']] = (
                    graph_nodetype['slug'])
            for nodetype in nodetypes:
                type = nodetype['type']
                type_slug = graph_nodetypes_slugs[type]
                # These structures are useful to mapping the slug and the id
                # to build our relationships
                self._nodetypes.append(type_slug)
                self._nodetypes_graph_names[type_slug] = type
                self._nodetypes_graph_slugs[type] = type_slug
                self._nodetypes_rules_slugs[type_slug] = nodetype['slug']
                nodetype_id = self._api.get_nodetype_schema(type_slug)['id']
                self._nodetypes_graph_ids[type] = nodetype_id
                mode = nodetype['mode']
                self._nodetypes_mode[type_slug] = mode
                id_label = nodetype.get('id', None)
                self._nodetypes_id_label[type_slug] = id_label
                for key, val in nodetype['properties'].iteritems():
                    # We remove spaces from beginning and end
                    key = key.strip()
                    # We check if we have a casting function defined
                    # If not, we use the default function
                    if isinstance(val, (tuple, list)):
                        func = val[0]
                        params = val[1:]
                    else:
                        func = DEFAULT_FUNC
                        params = [val]
                    for param in params:
                        try:
                            casting_elem = (key, func, params)
                            if(casting_elem not in self._nodetypes_casting
                               [type_slug]):
                                    (self._nodetypes_casting
                                        [type_slug].append(casting_elem))
                        except:
                            self._nodetypes_casting[type_slug] = (
                                [])
                            casting_elem = (key, func, params)
                            if(casting_elem not in self._nodetypes_casting
                               [type_slug]):
                                    (self._nodetypes_casting
                                        [type_slug].append(casting_elem))
                        if param not in self._rules_headers:
                            self._rules_headers.append(param)
        except:
            raise ValueError(
                "There are problems handling the types. "
                "Maybe the schema is not valid. "
                "Please, check the schema and restart the execution. "
                "If the problem persists, please contact us.")

    def _setup_reltypes(self):
        # Relationships settings
        reltypes = rules.RELATIONSHIPS
        self._reltypes = {}
        self._rel_ids = {}
        # Let's extract the slug directly from the graph using the API
        graph_rels_slugs = {}
        try:
            graph_reltypes = self._api.get_relationshiptypes()
            # We create the dictionary to map the correct slug
            for graph_reltype in graph_reltypes:
                rel_name = graph_reltype['name']
                rel_schema = graph_reltype['schema']
                rel_source = graph_reltype['source']
                rel_target = graph_reltype['target']
                rel_key = (rel_name, rel_schema, rel_source, rel_target)
                graph_rels_slugs[rel_key] = graph_reltype['slug']
            for reltype in reltypes:
                type = reltype['type']
                source_id = self._nodetypes_graph_ids[reltype['source']]
                target_id = self._nodetypes_graph_ids[reltype['target']]
                reltype_key = (type, self._schema_id, source_id, target_id)
                type_slug = graph_rels_slugs[reltype_key]
                source = self._nodetypes_graph_slugs[reltype['source']]
                target = self._nodetypes_graph_slugs[reltype['target']]
                id = reltype['id']
                relationship = {}
                relationship[source] = 'source'
                relationship[target] = 'target'
                self._reltypes_rules_slugs[type_slug] = reltype['slug']
                self._reltypes[type_slug] = relationship
                self._rel_ids[type_slug] = id
        except:
            raise ValueError(
                "There are problems handling the allowed relationships. "
                "Maybe the schema is not correct. "
                "Please, check the schema and restart the execution. "
                "If the problem persists, please contact us.")

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

    def _check_correct_row(self, row, columns):
        csv_row_length = len(row) == len(columns)
        csv_row_not_empty = len(row) != 0
        correct_row = csv_row_length and csv_row_not_empty
        return correct_row

    def _dump_nodes(self, csv_writer, type, mode, nodetype, columns, nodes,
                    nodes_list):
        nodes_remote_id = []
        if mode == GET_OR_CREATE:
            node_index = 0
            for node in nodes_list:
                filtering_params = {}
                try:
                    filtering_values = self._nodetypes_id_label[nodetype]
                    # We use the filtering values or all the properties
                    if filtering_values:
                        for value in filtering_values:
                            value_index = columns.index(value)
                            param_value = node[value_index]
                            filtering_params[value] = param_value
                    else:
                        # In case that we dont have defined values to filter,
                        # we use all the values for the node.
                        node_params = nodes[node_index]
                        for prop, value in node_params.iteritems():
                            # We need to remove the id and type props
                            # and the props with empty values
                            param_value = value
                            correct_prop = (prop != 'id') and (prop != 'type')
                            not_empty_value = (
                                (value != '') and (value is not None))
                            if correct_prop and not_empty_value:
                                filtering_params[prop] = param_value
                    results = self._api.filter_nodes(nodetype,
                                                     params=filtering_params)
                    remote_id = str(results['nodes'][0]['id'])
                    node.append(remote_id)
                    nodes_remote_id.append(node)
                except:
                    remote_id = self._api.post_nodes(
                        nodetype, params=[nodes[node_index]])
                    node.extend(remote_id)
                    nodes_remote_id.append(node)
                node_index += 1
        if mode == CREATE:
            try:
                temp_nodes_ids = (
                    self._api.post_nodes(nodetype, params=nodes))
                node_index = 0
                for node in nodes_list:
                    remote_id = temp_nodes_ids[node_index]
                    node.append(remote_id)
                    nodes_remote_id.append(node)
                    node_index += 1
            except:
                pass
        # Once we have our ids, we write them into the new csv files
        for new_node in nodes_remote_id:
            # The remote id is the last element of the node
            remote_id = str(new_node[-1])
            local_id = str(new_node[0])
            try:
                self._nodes_ids_mapping[type][local_id] = remote_id
            except KeyError:
                self._nodes_ids_mapping[type] = {}
                self._nodes_ids_mapping[type][local_id] = remote_id
            csv_writer.writerow(new_node)

    def _dump_relationships(self, mode, reltype, relationships):
        if mode == GET_OR_CREATE:
            rel_index = 0
            for relationship in relationships:
                filtering_params = {}
                try:
                    # We filter by source_id and target_id
                    filtering_params['source_id'] = relationship['source_id']
                    filtering_params['target_id'] = relationship['target_id']
                    results = self._api.filter_relationships(
                        reltype, params=filtering_params)
                    # We access to the result to check if
                    # everything is ok
                    results['relationships'][0]['id']
                except:
                    self._api.post_relationships(
                        reltype, params=[relationships[rel_index]])
                rel_index += 1
        if mode == CREATE:
            self._api.post_relationships(reltype, params=relationships)

    def _check_token(self):
        """
        We check the schema to allow the entire execution.
        """
        self._status(STATUS.CHECKING_TOKEN,
                     "Verifying API token...")
        try:
            self._api.get_graph()
        except:
            raise ValueError(
                "There are problems connecting to the API. "
                "Maybe the token is not valid. "
                "Please, check that the token is correct and "
                "restart the execution. "
                "If the problem persists, please contact us.")

    def _check_schema(self):
        """
        We check the schema to allow the entire execution.
        """
        self._status(STATUS.CHECKING_SCHEMA,
                     "Verifying schema...")
        api_schema_hash = self._api.export_schema()
        app_schema_hash = self._schema
        if api_schema_hash == app_schema_hash:
            pass
        else:
            raise ValueError(
                "The schema is not valid. Please, check the schema "
                "and restart the execution.")

    def format_data_columns(self):
        """
        We format the headers of the CSV to get the index to treat for
        each type
        """
        self._status(STATUS.CSV_COLUMNS_FORMATTING,
                     "Formatting CSV columns...")
        csv_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
        # The first line are the headers, our schema properties
        csv_headers = csv_reader.next()
        column_index = 0
        for csv_header in csv_headers:
            # We remove spaces from beginning and end
            csv_header = csv_header.strip()
            self._headers.append(csv_header)
            self._csv_columns_indexes[csv_header] = column_index
            column_index += 1
        # Let's check if the CSV headers in the rules file are actually
        # in the CSV file
        for rule_header in self._rules_headers:
            if rule_header not in self._headers:
                raise ValueError(
                    "CSV file headers do not match those defined "
                    "in the rules file. "
                    "Please, check the headers and restart the execution."
                )
        # Let's check if all the properties in the rules file are actually
        # defined in the schema
        for type in self._nodetypes:
            # We need to check two cases: casting functions or directed mapping
            # Check if the type has casting functions defined
            try:
                casting_functions = self._nodetypes_casting[type]
                properties = [elems[0] for elems in casting_functions]
            except:
                properties = []
            # We get the properties for the type from the schema
            nodetype = self._nodetypes_graph_names[type]
            type_properties = self._schema['nodeTypes'][nodetype].keys()
            for prop in properties:
                if prop not in type_properties:
                    raise ValueError(
                        "Schema properties do not match those defined "
                        "in the rules file. Please, check the properties "
                        "and restart the execution."
                    )
        csv_file.close()

    def format_data_nodes(self):
        """
        We format the nodes data into their respective csv files
        """
        self._status(STATUS.DATA_NODES_FORMATTING,
                     "Formatting nodes data...")
        csv_root_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_root_file, encoding="utf-8")
        # We create the file to dump the relationships by row
        csv_relationships_path = os.path.join(
            self._history_path, "_{}.csv".format('relationships'))
        csv_relationships = open(csv_relationships_path, 'w+')
        csv_writer_rels = unicodecsv.writer(csv_relationships,
                                            encoding="utf-8")
        # We create a temp file to control the nodes
        csv_nodes_treated_path = os.path.join(
            self._history_path, "_{}.csv".format('nodes_treated'))
        csv_nodes_treated = open(csv_nodes_treated_path, 'a+', 0)
        csv_nodes_treated.close()
        csv_relationships_headers = []
        csv_rels_headers_written = False
        # The first line are the headers/columns values
        columns = csv_reader.next()
        # The rest of the lines are data
        csv_files = {}
        csv_writers = {}
        csv_file_node_id = {}
        try:
            csv_row = csv_reader.next()
            correct_row = self._check_correct_row(csv_row, columns)
            while csv_row:
                if correct_row:
                    relationships_node_ids = []
                    for type in self._nodetypes:
                        temp_node = []
                        try:
                            casting_functions = (self._nodetypes_casting[type])
                        except KeyError:
                            casting_functions = []
                        csv_headers_castings = []
                        for casting_function in casting_functions:
                            # Let's extract the elements from the tuple
                            csv_header = casting_function[0]
                            func = casting_function[1]
                            params = casting_function[2]
                            # Let's get the index to get the values for params
                            params_values = []
                            try:
                                for param in params:
                                    param_index = self._csv_columns_indexes[
                                        param]
                                    param_value = csv_row[param_index]
                                    params_values.append(param_value)
                                cast_func = getattr(castings, func,
                                                    lambda *x: u",".join(
                                                        map(repr, x)))
                                result = cast_func(*params_values)
                                csv_headers_castings.append(csv_header)
                                temp_node.append(result)
                            except KeyError:
                                raise ValueError(
                                    "There is something wrong with the CSV "
                                    "file or the rules file. "
                                    "Please, check both and restart the "
                                    "execution. If the problem persists, "
                                    "please contact us."
                                )
                        # We write the node values in the right csv file
                        try:
                            csv_writer = csv_writers[type]
                        except KeyError:
                            csv_name = self._nodetypes_rules_slugs[type]
                            csv_file_path = (os.path.join(
                                self._history_path, "{}.csv".format(csv_name)))
                            csv_file = open(csv_file_path, 'w+', 0)
                            csv_writer = unicodecsv.writer(csv_file,
                                                           encoding="utf-8")
                            csv_files[type] = csv_file
                            csv_writers[type] = csv_writer
                            csv_file_node_id[type] = 1
                            # Let's get the headers correctly
                            csv_headers_basics = ['id', 'type']
                            csv_headers_basics.extend(csv_headers_castings)
                            csv_writer.writerow(csv_headers_basics)
                        # We check if the node already exists
                        exists_node = False
                        try:
                            node_id = csv_file_node_id[type]
                            node_basics = [str(node_id), type]
                            node_basics.extend(temp_node)
                            # We use the temp file for the checking
                            csv_nodes_treated = open(csv_nodes_treated_path,
                                                     'r+')
                            csv_reader_type = unicodecsv.reader(
                                csv_nodes_treated, encoding="utf-8")
                            csv_reader_row = csv_reader_type.next()
                            while csv_reader_row:
                                # We omit the first two elements (id, type)
                                if csv_reader_row[2:] == temp_node:
                                    exists_node = True
                                    # The local id is in the 0 index
                                    rel_node_id = csv_reader_row[0]
                                    break
                                csv_reader_row = csv_reader_type.next()
                        except StopIteration:
                            pass
                        csv_nodes_treated.close()
                        if not exists_node:
                            # Let's add our node
                            rel_node_id = node_id
                            # We add the the node to our temp file
                            csv_nodes_treated = open(csv_nodes_treated_path,
                                                     'a+', 0)
                            csv_writer_nodes = unicodecsv.writer(
                                csv_nodes_treated, encoding="utf-8")
                            csv_writer_nodes.writerow(node_basics)
                            csv_nodes_treated.close()
                            # We add the the node to the type csv file
                            csv_writer.writerow(node_basics)
                            csv_file_node_id[type] += 1
                        relationships_node_ids.append(rel_node_id)
                        # Let's update our structures for the relationships
                        # file
                        if type not in csv_relationships_headers:
                            csv_relationships_headers.append(type)
                    # We dump the values for our relationships
                    if not csv_rels_headers_written:
                        csv_writer_rels.writerow(csv_relationships_headers)
                        csv_rels_headers_written = True
                    csv_writer_rels.writerow(relationships_node_ids)
                csv_row = csv_reader.next()
                correct_row = self._check_correct_row(csv_row, columns)
        except StopIteration:
            pass
        # We close the files
        csv_root_file.close()
        csv_relationships.close()
        nodes_dumped_file_name = csv_nodes_treated.name
        csv_nodes_treated.close()
        os.remove(nodes_dumped_file_name)
        for f in csv_files.values():
            f.close()

    def populate_nodes(self):
        """
        Populate the nodes data into SylvaDB
        """
        self._status(STATUS.DATA_NODES_DUMPING,
                     "Writing nodes to the server. This may take a while, "
                     "please, be patient...")
        for type, mode in self._nodetypes_mode.iteritems():
            # We open the files to read and write
            csv_name = self._nodetypes_rules_slugs[type]
            csv_file_path_type = os.path.join(
                self._history_path, "{}.csv".format(csv_name))
            csv_file_path_type_new = os.path.join(
                self._history_path, "{}_new_ids.csv".format(csv_name))
            csv_file_type = open(csv_file_path_type, 'r')
            csv_file_type_new = open(csv_file_path_type_new, 'w+')
            csv_reader = unicodecsv.reader(csv_file_type, encoding="utf-8")
            csv_writer = unicodecsv.writer(csv_file_type_new, encoding="utf-8")
            columns = csv_reader.next()
            columns.append("remote_id")
            csv_writer.writerow(columns)
            try:
                # We use a list of dicts to store the data nodes
                nodes = []
                nodes_list = []
                nodes_batch_limit = 0
                nodetype = type
                csv_type_row = csv_reader.next()
                while csv_type_row:
                    column_index = 0
                    temp_node = {}
                    for elem in csv_type_row:
                        prop_name = columns[column_index]
                        temp_node[prop_name] = elem
                        column_index += 1
                    nodes.append(temp_node)
                    nodes_list.append(csv_type_row)
                    nodes_batch_limit += 1
                    if nodes_batch_limit == self.batch_size:
                        print("Dumping {} nodes...".format(len(nodes_list)))
                        self._dump_nodes(csv_writer, type, mode,
                                         nodetype, columns, nodes, nodes_list)
                        # We reset the structures
                        nodes = []
                        nodes_list = []
                        nodes_batch_limit = 0
                    csv_type_row = csv_reader.next()
            except StopIteration:
                print("Dumping {} nodes...".format(len(nodes_list)))
                self._dump_nodes(csv_writer, type, mode, nodetype, columns,
                                 nodes, nodes_list)
            # We get the names for the files
            old_ids_file_name = csv_file_type.name
            new_ids_file_name = csv_file_type_new.name
            # We close the files
            csv_file_type.close()
            csv_file_type_new.close()
            # We remove the old csv and rename the new
            os.remove(old_ids_file_name)
            os.rename(new_ids_file_name, old_ids_file_name)

    def preparing_relationships(self):
        """
        Create the _relationships.csv files where we map all the relationships
        neccesary ids.
        """
        self._status(STATUS.RELATIONSHIPS_PREPARING,
                     "Preparing relationships...")
        csv_file_path = os.path.join(
            self._history_path, "_{}.csv".format('relationships'))
        csv_file = open(csv_file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
        csv_file_new_path = (
            os.path.join(
                self._history_path, "_{}_new_ids.csv".format('relationships')))
        csv_new_file = open(csv_file_new_path, 'w')
        csv_writer_new = unicodecsv.writer(csv_new_file, encoding="utf-8")
        headers = csv_reader.next()
        csv_writer_new.writerow(headers)
        try:
            csv_row = csv_reader.next()
            while csv_row:
                column_index = 0
                new_csv_row = []
                for header in headers:
                    type_row_id = csv_row[column_index]
                    remote_id = self._nodes_ids_mapping[header][type_row_id]
                    new_csv_row.append(remote_id)
                    column_index += 1
                csv_writer_new.writerow(new_csv_row)
                csv_row = csv_reader.next()
        except StopIteration:
            pass
        # We get the names for the files
        old_ids_file_name = csv_file.name
        new_ids_file_name = csv_new_file.name
        # We close the files
        csv_file.close()
        csv_new_file.close()
        # We remove the old csv and rename the new
        os.remove(old_ids_file_name)
        os.rename(new_ids_file_name, old_ids_file_name)

    def format_data_relationships(self):
        """
        Using the _relationships_new_ids.csv file, we format the data for the
        relationships
        """
        self._status(STATUS.DATA_RELATIONSHIPS_FORMATTING,
                     "Formatting relationships data...")
        csv_file_path = os.path.join(
            self._history_path, "_{}.csv".format('relationships'))
        csv_file_root = open(csv_file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file_root, encoding="utf-8")
        csv_files = {}
        csv_writers = {}
        # First we get the index for each type (they are the headers)
        columns_indexes = {}
        columns = csv_reader.next()
        column_index = 0
        for prop in columns:
            columns_indexes[prop] = column_index
            column_index = column_index + 1
        try:
            csv_row_data = csv_reader.next()
            while csv_row_data:
                for key, val in self._reltypes.iteritems():
                    try:
                        csv_writer = csv_writers[key]
                    except:
                        csv_name = self._reltypes_rules_slugs[key]
                        csv_file_path = os.path.join(self._history_path,
                                                     "{}.csv".format(csv_name))
                        csv_file = open(csv_file_path, 'w+')
                        csv_writer = unicodecsv.writer(csv_file,
                                                       encoding="utf-8")
                        csv_files[key] = csv_file
                        csv_writers[key] = csv_writer
                        columns = ['source_id', 'target_id', 'type']
                        csv_writer.writerow(columns)
                    source = ""
                    target = ""
                    type = key
                    for key_t, val_t in val.iteritems():
                        data_index = columns_indexes[key_t]
                        if val_t == SOURCE:
                            source = csv_row_data[data_index]
                        elif val_t == TARGET:
                            target = csv_row_data[data_index]
                    temp_row = [source, target, type]
                    csv_writers[key].writerow(temp_row)
                csv_row_data = csv_reader.next()
        except StopIteration:
            pass
        csv_file_root.close()
        for f in csv_files.values():
            f.close()

    def populate_relationships(self):
        """
        Populate the relationships data into SylvaDB
        """
        self._status(STATUS.DATA_RELATIONSHIPS_DUMPING,
                     "Writing relationships to the server. This may take a "
                     "while, please, be patient...")
        for key, val in self._rel_ids.iteritems():
            csv_name = self._reltypes_rules_slugs[key]
            csv_file_path = os.path.join(
                self._history_path, "{}.csv".format(csv_name))
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
                    if relationships_batch_limit == self.batch_size:
                        print("Dumping {} relationships...".format(
                            len(relationships)))
                        self._dump_relationships(val, reltype, relationships)
                        # We reset the structures
                        relationships = []
                        relationships_batch_limit = 0
                    temp_rel_data = csv_reader.next()
            except StopIteration:
                print("Dumping {} relationships...".format(len(relationships)))
                self._dump_relationships(val, reltype, relationships)
            csv_file.close()

    def populate_data(self):
        """
        Execute all the functions
        """
        # We check if we need to format the data yet
        try:
            self._check_token()
            self._check_schema()
            self._setup_nodetypes()
            self._setup_reltypes()
            self.format_data_columns()
            self.format_data_nodes()
            self.populate_nodes()
            self.preparing_relationships()
            self.format_data_relationships()
            self.populate_relationships()
            self._status(STATUS.EXECUTION_COMPLETED, "Execution completed!")
        except ValueError as e:
            print e.args


def main():
    """
    Options to execute the app using the command line
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'file', help='CSV file used to dump the data into SylvaDB')

    parser.add_argument(
        '--batch-size', help='Batch size used to dump the data into SylvaDB')
    args = parser.parse_args()
    file_path = args.file
    batch_size = args.batch_size
    app = SylvaApp(file_path, batch_size)
    app.populate_data()


if __name__ == '__main__':
    main()
