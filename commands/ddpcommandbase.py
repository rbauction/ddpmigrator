from datetime import datetime
from sfdclib import \
    SfdcBulkApi, \
    SfdcMetadataApi, \
    SfdcSession, \
    SfdcToolingApi

import fieldhandlers
import os


class DdpCommandBase:
    _LOOP_DIR_NAME = "loop"
    _DATA_DIR_NAME = "data"
    _DOCS_DIR_NAME = "documents"

    """ Base command class """
    def __init__(self, settings, **kwargs):
        self._kwargs = kwargs
        self._unique_key = settings['unique-key']
        self._update_batch_size = int(settings['update-batch-size'])
        self._table_settings = settings['tables']
        self._excluded_fields = settings['excluded-fields']
        self._source_dir = os.path.normpath(kwargs['source_dir'])
        # Instance of SfdcSession class
        self._session = None
        # Instance of SfdcToolingApi class
        self._tapi = None
        # Instance of SfdcBulkApi class
        self._bapi = None
        # Instance of SfdcMetadataApi class
        self._mapi = None
        # Resolve table dependencies
        self._forward = dict()
        self._reverse = dict()
        self._resolve_dependencies()

    @staticmethod
    def _locate_handler_class(class_name):
        return getattr(fieldhandlers, class_name)

    @staticmethod
    def _convert_sfdc_date_time(date_time):
        return datetime.strptime(date_time, '%Y-%m-%dT%H:%M:%S.%fZ')

    def _create_sfdc_session(self):
        kwargs = {
            'username': self._kwargs['username'],
            'password': self._kwargs['password'],
            'is_sandbox': self._kwargs['is_sandbox']
        }
        if 'token' in self._kwargs:
            kwargs['token'] = self._kwargs['token']
        if 'version' in self._kwargs:
            kwargs['api_version'] = self._kwargs['version']

        # Establish SFDC session
        self._session = SfdcSession(**kwargs)
        self._session.login()

        # Create an instance of Tooling API class
        self._tapi = SfdcToolingApi(self._session)

        # Create an instance of Bulk API class
        self._bapi = SfdcBulkApi(self._session)

        # Create an instance of Metadata API class
        self._mapi = SfdcMetadataApi(self._session)

    def _get_loop_dir(self):
        return os.path.join(self._source_dir, self._LOOP_DIR_NAME)

    def _get_data_dir(self):
        return os.path.join(self._get_loop_dir(), self._DATA_DIR_NAME)

    def _get_relative_loop_dir(self):
        """ Returns path to loop directory relative to source directory """
        return self._LOOP_DIR_NAME

    def _get_relative_data_dir(self):
        """ Returns path to data directory relative to source directory """
        return os.path.join(self._get_relative_loop_dir(), self._DATA_DIR_NAME)

    def _get_relative_docs_dir(self):
        """ Returns path to documents directory relative to source directory """
        return os.path.join(self._get_relative_loop_dir(), self._DOCS_DIR_NAME)

    def _get_relative_file_path(self, path):
        return os.path.relpath(path, self._get_relative_loop_dir())

    def _get_relative_data_path(self, path):
        return os.path.relpath(path, self._get_relative_data_dir())

    def _excluded_fields_for_soql(self):
        return "'" + "','".join(self._excluded_fields) + "'"

    def _lookup_table_name_by_alias(self, table_alias):
        for table_name in self._table_settings:
            if table_alias == self._table_settings[table_name]['alias']:
                return table_name

        raise Exception("Could not find table by alias {0}".format(table_alias))

    def _extract_table_name_from_path(self, file_name):
        # Extract table name
        rel_path = os.path.relpath(file_name, self._get_relative_data_dir())
        table_alias = rel_path.split('\\')[1]
        # Remove '.yaml' for parent table files
        if table_alias.endswith('.yaml'):
            table_alias = table_alias[:-5]
        return self._lookup_table_name_by_alias(table_alias)

    def _resolve_import_order(self, table_names):
        # Prepare list of tables in order of import
        ordered_import_list = list()
        for table_name in table_names:
            # Check if table has parents
            if table_name in self._reverse and len(self._reverse[table_name]) > 0:
                max_parent_index = -1
                for parent in self._reverse[table_name]:
                    if parent in ordered_import_list:
                        parent_index = ordered_import_list.index(parent)
                        if parent_index > max_parent_index:
                            max_parent_index = parent_index

                ordered_import_list.insert(max_parent_index + 1, table_name)
            else:
                ordered_import_list.insert(0, table_name)

        return ordered_import_list

    def _resolve_dependencies(self):
        for table_name in self._table_settings:
            if table_name not in self._reverse:
                self._reverse[table_name] = []
            table_settings = self._table_settings[table_name]
            if 'parent-relationship' in table_settings:
                dep = table_settings['parent-relationship']['parent-table']
                if dep not in self._forward:
                    self._forward[dep] = []
                self._forward[dep].append(table_name)
                self._reverse[table_name].append(dep)

    @staticmethod
    def _parse_table_name(table_name):
        if '.' in table_name:
            return table_name.split('.')
        else:
            return '', table_name

    def _retrieve_fields(self, table_name):
        fields = []
        namespace, dev_name = self._parse_table_name(table_name)
        query = \
            """SELECT QualifiedApiName FROM FieldDefinition
            WHERE EntityDefinition.NamespacePrefix = '{0}'
                AND EntityDefinition.QualifiedApiName='{1}'
                AND IsCalculated = false
                AND QualifiedApiName NOT IN ({2})""".\
            format(namespace, dev_name, self._excluded_fields_for_soql())
        res = self._tapi.anon_query(query)
        for field in res['records']:
            fields.append(field['QualifiedApiName'])

        # Sort fields by name, otherwise two subsequent exports won't be identical
        fields.sort()

        return fields

    def _retrieve_data(self, dev_name, query):
        return self._bapi.export_object(dev_name, query)

    def _upsert_data(self, dev_name, csv_data, external_id_field):
        return self._bapi.upsert_object(dev_name, csv_data, external_id_field)

    def _update_data(self, dev_name, csv_data):
        return self._bapi.update_object(dev_name, csv_data)

    def do(self):
        raise NotImplementedError("Method do() is not overridden")
