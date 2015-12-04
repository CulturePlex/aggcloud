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
        self._nodetypes = {}
        self._nodetypes_id_label = {}
        self._nodetypes_graph_names = {}
        self._nodetypes_graph_slugs = {}
        self._nodetypes_graph_ids = {}
        self._nodetypes_mode = {}
        self._nodetypes_headers_mapping = {}
        self._nodetypes_casting_elements = {}
        self._nodes_ids_mapping = {}
        # Variables to format the data
        # List to store the headers from the csv. Lists maintain the order.
        self._headers = []
        self._rules_headers = []
        # Dictionary to store the index of each column that belongs to a type
        self._types_properties_indexes = {}
        # Checking the connection with the API
        self._status(STATUS.API_CONNECTING,
                     "Connecting with the SylvaDB API...")
        self._api = API(token=self._token, graph_slug=self._graph)
        # Settings
        self._schema = json.loads(rules.SCHEMA)
        self._schema_id = self._api.get_graph()['schema']
        self._csv_columns_indexes = {}

    def _setup_nodetypes(self):
        nodetypes = rules.NODES
        for nodetype in nodetypes:
            # Let's extract the slug directly from the graph using the API
            graph_nodetypes = self._api.get_nodetypes()
            type = nodetype['type']
            for graph_nodetype in graph_nodetypes:
                nodetype_name = graph_nodetype['name']
                if type == nodetype_name:
                    type_slug = graph_nodetype['slug']
                    break
            # These structures are useful to mapping the slug and the id
            # to build our relationships
            self._nodetypes_graph_names[type_slug] = type
            self._nodetypes_graph_slugs[type] = type_slug
            nodetype_id = self._api.get_nodetype_schema(type_slug)['id']
            self._nodetypes_graph_ids[type] = nodetype_id

            mode = nodetype['mode']
            self._nodetypes_mode[type_slug] = mode
            id_label = nodetype.get('id', None)
            self._nodetypes_id_label[type_slug] = id_label
            self._nodetypes[type_slug] = []
            for key, val in nodetype['properties'].iteritems():
                # We remove spaces from beginning and end
                key = key.strip()
                # We check if we have a casting function defined
                if isinstance(val, (tuple, list)):
                    func = val[0]
                    params = val[1:]
                    for param in params:
                        try:
                            casting_elem = (key, func, params)
                            if(casting_elem not in
                               self._nodetypes_casting_elements[type_slug]):
                                    (self._nodetypes_casting_elements
                                        [type_slug].append(casting_elem))
                        except:
                            self._nodetypes_casting_elements[type_slug] = []
                            casting_elem = (key, func, params)
                            if(casting_elem not in
                               self._nodetypes_casting_elements[type_slug]):
                                    (self._nodetypes_casting_elements
                                        [type_slug].append(casting_elem))
                        if param not in self._rules_headers:
                            self._rules_headers.append(param)
                else:
                    if val not in self._nodetypes[type_slug]:
                        self._nodetypes[type_slug].append(val)
                    self._nodetypes_headers_mapping[val] = key
                    if val not in self._rules_headers:
                            self._rules_headers.append(val)

    def _setup_reltypes(self):
        # Relationships settings
        reltypes = rules.RELATIONSHIPS
        self._reltypes = {}
        self._rel_ids = {}
        for reltype in reltypes:
            source_id = self._nodetypes_graph_ids[reltype['source']]
            target_id = self._nodetypes_graph_ids[reltype['target']]
            # Let's extract the slug directly from the graph using the API
            graph_reltypes = self._api.get_relationshiptypes()
            type = reltype['type']
            for graph_reltype in graph_reltypes:
                rel_name = graph_reltype['name']
                rel_schema = graph_reltype['schema']
                rel_source = graph_reltype['source']
                rel_target = graph_reltype['target']
                if ((type == rel_name)
                    and (self._schema_id == rel_schema)
                    and (source_id == rel_source)
                        and (target_id == rel_target)):
                    type_slug = graph_reltype['slug']
                    break
            source = self._nodetypes_graph_slugs[reltype['source']]
            target = self._nodetypes_graph_slugs[reltype['target']]
            id = reltype['id']
            relationship = {}
            relationship[source] = 'source'
            relationship[target] = 'target'
            self._reltypes[type_slug] = relationship
            self._rel_ids[type_slug] = id

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
                            filtering_params[value] = node[value_index]
                    else:
                        # In case that we dont have defined values to filter,
                        # we use all the values for the node.
                        node_params = nodes[node_index]
                        for prop, value in node_params.iteritems():
                            # We need to remove the id and type props
                            # and the props with empty values
                            correct_prop = (prop != 'id') and (prop != 'type')
                            not_empty_value = (value != '') and (
                                value is not None)
                            if correct_prop and not_empty_value:
                                filtering_params[prop] = value
                    results = self._api.filter_nodes(
                        nodetype, params=filtering_params)
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

    def _check_token(self):
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

    def _check_schema(self):
        """
        We check the schema to allow the entire execution.
        """
        self._status(STATUS.CHECKING_SCHEMA,
                     "Checking schema...")
        api_schema_hash = self._api.export_schema()
        app_schema_hash = self._schema
        if api_schema_hash == app_schema_hash:
            pass
        else:
            raise ValueError(
                "The schema isn't correct. Please, check it and restart "
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
        # The first line are the headers, our schema properties
        csv_headers = csv_reader.next()
        column_index = 0
        for csv_header in csv_headers:
            # We remove spaces from beginning and end
            csv_header = csv_header.strip()
            self._headers.append(csv_header)
            for key, val in self._nodetypes.iteritems():
                if csv_header in val:
                    try:
                        self._types_properties_indexes[key].append(
                            column_index)
                    except:
                        self._types_properties_indexes[key] = []
                        self._types_properties_indexes[key].append(
                            column_index)
            self._csv_columns_indexes[csv_header] = column_index
            column_index += 1
        # Let's check if the CSV headers in the rules file are actually
        # in the CSV file
        for rule_header in self._rules_headers:
            if rule_header not in self._headers:
                raise ValueError(
                    "CSV file headers do not match the rules file headers. "
                    "Please, check it and restart the execution."
                )
        # Let's check if all the properties in the rules file are actually
        # defined in the schema
        for type, type_props in self._nodetypes.iteritems():
            # We need to check two cases: casting functions or directed mapping
            # Check if the type has casting functions defined
            try:
                casting_functions = self._nodetypes_casting_elements[type]
                properties = [elems[0] for elems in casting_functions]
            except:
                properties = []
            # We need to map the values from the nodetype
            values_mapped = [
                self._nodetypes_headers_mapping[val] for val in type_props]
            properties.extend(values_mapped)
            # We get the properties for the type from the schema
            nodetype = self._nodetypes_graph_names[type]
            type_properties = self._schema['nodeTypes'][nodetype].keys()
            for prop in properties:
                if prop not in type_properties:
                    raise ValueError(
                        "Schema properties do not match the rules properties. "
                        "Please, check it and restart the execution."
                    )
        csv_file.close()

    def format_data_nodes(self):
        """
        We format the nodes data into their respective csv files
        """
        self._status(STATUS.DATA_NODES_FORMATTING,
                     "Formatting data for nodes...")
        csv_root_file = open(self._file_path, 'r')
        csv_reader = unicodecsv.reader(csv_root_file, encoding="utf-8")
        # We create the file to dump the relationships by row
        csv_relationships_path = os.path.join(
            self._history_path, '_relationships.csv')
        csv_relationships = open(csv_relationships_path, 'w+')
        csv_writer_rels = unicodecsv.writer(
            csv_relationships, encoding="utf-8")
        csv_relationships_headers = []
        csv_rels_headers_written = False
        # We read the headers, to avoid the columns
        csv_reader.next()
        # The rest of the lines are data
        csv_files = {}
        csv_writers = {}
        csv_file_node_id = {}
        nodes_id = {}
        csv_nodes_treated = []
        try:
            csv_row = csv_reader.next()
            while csv_row:
                relationships_node_ids = []
                for type, prop_indexes in (
                        self._types_properties_indexes.iteritems()):
                    # Create the node in two steps
                    # First we get the values using the index of columns
                    # related to each type. Then, applying casting functions.
                    temp_node = []
                    for index in prop_indexes:
                        temp_value = csv_row[index]
                        temp_node.append(temp_value)
                    # Check if we have casting function
                    try:
                        casting_functions = (
                            self._nodetypes_casting_elements[type])
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
                                param_index = self._csv_columns_indexes[param]
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
                                "There is something wrong with the csv file "
                                "or with the rules file. "
                                "Please, check it and restart the execution. "
                                "If the problem persists, please contact us :)"
                            )
                    # We write the node values in the right csv file
                    try:
                        csv_writer = csv_writers[type]
                    except KeyError:
                        csv_file_path = os.path.join(self._history_path,
                                                     "{}.csv".format(type))
                        csv_file = open(csv_file_path, 'w+')
                        csv_writer = unicodecsv.writer(csv_file,
                                                       encoding="utf-8")
                        csv_files[type] = csv_file
                        csv_writers[type] = csv_writer
                        csv_file_node_id[type] = 1
                        # Let's get the headers correctly
                        csv_headers_basics = ['id', 'type']
                        csv_headers = []
                        headers_indexes = self._types_properties_indexes[type]
                        not_mapped_headers = [
                            self._headers[i] for i in headers_indexes]
                        for header in not_mapped_headers:
                            try:
                                csv_header = (
                                    self._nodetypes_headers_mapping[header])
                                csv_headers.append(csv_header)
                            except:
                                # This exception is produced by the mapping
                                # with the casting function
                                pass
                        csv_headers_basics.extend(csv_headers)
                        csv_headers_basics.extend(csv_headers_castings)
                        csv_writer.writerow(csv_headers_basics)
                    # Let's add our node
                    # We create a temp key to store the ids
                    nodes_ids_key = "_".join([str(elem) for elem in temp_node])
                    if temp_node not in csv_nodes_treated:
                        node_id = csv_file_node_id[type]
                        nodes_id[nodes_ids_key] = node_id
                        type_name = self._nodetypes_graph_names[type]
                        node_basics = [str(node_id), type_name]
                        node_basics.extend(temp_node)
                        csv_writer.writerow(node_basics)
                        csv_nodes_treated.append(temp_node)
                        csv_file_node_id[type] += 1
                    relationships_node_ids.append(nodes_id[nodes_ids_key])
                    # Let's update our structures for the relationships file
                    if type not in csv_relationships_headers:
                        csv_relationships_headers.append(type)
                # We dump the values for our relationships
                if not csv_rels_headers_written:
                    csv_writer_rels.writerow(csv_relationships_headers)
                    csv_rels_headers_written = True
                csv_writer_rels.writerow(relationships_node_ids)
                csv_row = csv_reader.next()
        except StopIteration:
            pass
        # We close the files
        csv_root_file.close()
        csv_relationships.close()
        for f in csv_files.values():
            f.close()

    def populate_nodes(self):
        """
        Populate the nodes data into SylvaDB
        """
        self._status(STATUS.DATA_NODES_DUMPING,
                     "Dumping the data for nodes into SylvaDB...")
        for type, mode in self._nodetypes_mode.iteritems():
            # We open the files to read and write
            csv_file_path_type = os.path.join(self._history_path,
                                              "{}.csv".format(type))
            csv_file_path_type_new = os.path.join(
                self._history_path, "{}_new_ids.csv".format(type))
            csv_file_type = open(csv_file_path_type, 'r')
            csv_file_type_new = open(csv_file_path_type_new, 'w+')
            csv_reader_type = unicodecsv.reader(csv_file_type,
                                                encoding="utf-8")
            csv_writer = unicodecsv.writer(csv_file_type_new, encoding="utf-8")
            columns = csv_reader_type.next()
            columns.append("remote_id")
            csv_writer.writerow(columns)
            try:
                # We are going to save the nodes in a list of dicts
                # to store the data
                nodes = []
                nodes_list = []
                nodes_batch_limit = 0
                nodetype = type
                csv_type_row = csv_reader_type.next()
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
                    csv_type_row = csv_reader_type.next()
            except StopIteration:
                print("Dumping {} nodes...".format(len(nodes_list)))
                self._dump_nodes(csv_writer, type, mode, nodetype, columns,
                                 nodes, nodes_list)
            csv_file_type.close()
            csv_file_type_new.close()

    def preparing_relationships(self):
        """
        Create the _relationships.csv files where we map all the relationships
        neccesary ids.
        """
        self._status(STATUS.RELATIONSHIPS_PREPARING,
                     "Preparing data for relationships...")
        csv_file_path = os.path.join(self._history_path, "_relationships.csv")
        csv_file = open(csv_file_path, 'r')
        csv_reader = unicodecsv.reader(csv_file, encoding="utf-8")
        csv_file_new_path = os.path.join(self._history_path,
                                         "_relationships_new_ids.csv")
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
        # We close the files
        csv_file.close()

    def format_data_relationships(self):
        """
        Using the _relationships_new_ids.csv file, we format the data for the
        relationships
        """
        self._status(STATUS.DATA_RELATIONSHIPS_FORMATTING,
                     "Formatting data for relationships...")
        csv_file_path = os.path.join(self._history_path,
                                     '_relationships_new_ids.csv')
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
                        csv_file_path = os.path.join(self._history_path,
                                                     "{}.csv".format(key))
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
                        if val_t == 'source':
                            source = csv_row_data[data_index]
                        elif val_t == 'target':
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
                    if relationships_batch_limit == self.batch_size:
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

    parser.add_argument(
        '--batch-size', help='Batch size used to dump the data into SylvaDB')
    args = parser.parse_args()
    file_path = args.file
    batch_size = args.batch_size
    app = SylvaApp(file_path, batch_size)
    app.populate_data()


if __name__ == '__main__':
    main()
