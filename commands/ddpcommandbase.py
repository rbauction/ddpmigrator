from datetime import datetime
from sfdclib import \
    SfdcBulkApi, \
    SfdcMetadataApi, \
    SfdcSession, \
    SfdcToolingApi

import fieldhandlers
import logging
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
        # Logger
        self._logger = logging.getLogger('root')

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

    def _get_docs_dir(self):
        return os.path.join(self._get_loop_dir(), self._DOCS_DIR_NAME)

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
        # noinspection PyTypeChecker
        if table_alias.endswith('.yaml'):
            table_alias = table_alias[:-5]
        return self._lookup_table_name_by_alias(table_alias)

    def _resolve_import_order(self, table_names):
        # Prepare ordered list of tables using import-order from settings.yaml
        ordered_import_list = list()
        for table_name in table_names:
            unordered_element_import_order = self._table_settings[table_name]['import-order']
            if ordered_import_list.__len__() > 1:
                for ordered_element in ordered_import_list:
                    ordered_element_import_order = self._table_settings[ordered_element]['import-order']
                    # if ordered element is the first element
                    if ordered_import_list.index(ordered_element) == 0:
                        # and if ordered element has a lower order index
                        if unordered_element_import_order < ordered_element_import_order:
                            ordered_import_list.insert(0, table_name)
                            break
                    # if ordered element is the last element
                    elif ordered_import_list.index(ordered_element) == (ordered_import_list.__len__() - 1):
                        # and if ordered element has a higher order index
                        if unordered_element_import_order > ordered_element_import_order:
                            ordered_import_list.insert(ordered_import_list.__len__(), table_name)
                            break
                    # if ordered element has a higher import order index
                    elif ordered_element_import_order > unordered_element_import_order:
                        ordered_import_list.insert(ordered_import_list.index(ordered_element), table_name)
                        break
            if ordered_import_list.__len__() == 1:
                # get the import order of the first element
                ordered_element_import_order = self._table_settings[ordered_import_list[0]]['import-order']
                # since theres only one element we only need to determine if this goes before or after
                if unordered_element_import_order > ordered_element_import_order:
                    ordered_import_list.insert(1, table_name)
                else:
                    ordered_import_list.insert(0, table_name)
            # empty list? just drop it in
            if ordered_import_list.__len__() == 0:
                ordered_import_list.insert(0, table_name)

        return ordered_import_list

    def _resolve_dependencies(self):
        for table_name in self._table_settings:
            if table_name not in self._reverse:
                self._reverse[table_name] = list()
            table_settings = self._table_settings[table_name]
            if 'parent-relationship' in table_settings:
                dep = table_settings['parent-relationship']['parent-table']
                if dep not in self._forward:
                    self._forward[dep] = list()
                self._forward[dep].append(table_name)
                self._reverse[table_name].append(dep)

    @staticmethod
    def _parse_table_name(table_name):
        if '.' in table_name:
            return table_name.split('.')
        else:
            return '', table_name

    def _retrieve_fields(self, table_name):
        fields = list()
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
        return self._bapi.export(dev_name, query)

    def _upsert_data(self, dev_name, csv_data, external_id_field):
        return self._bapi.upsert(dev_name, csv_data, external_id_field)

    def _update_data(self, dev_name, csv_data):
        return self._bapi.update(dev_name, csv_data)

    def _delete_data(self, dev_name, csv_data):
        return self._bapi.delete(dev_name, csv_data)

    def do(self):
        raise NotImplementedError("Method do() is not overridden")
