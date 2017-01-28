from collections import OrderedDict
from commands.ddpcommandbase import DdpCommandBase
from datetime import datetime
from helpers import csvhelper

import io
import os
import shutil
import time
import zipfile
import yaml


def str_presenter(dumper, data):
    if len(data.splitlines()) > 1:  # check for multi-line string
        # PyYAML doesn't like carriage return, remove it
        return dumper.represent_scalar('tag:yaml.org,2002:str', data.replace('\r\n', '\n'), style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


def ordered_dict_presenter(dumper, data):
    return dumper.represent_dict(data.items())


class DdpExport(DdpCommandBase):
    """ Class handling export command """
    def __init__(self, settings, **kwargs):
        super().__init__(settings, **kwargs)
        self._data = {}
        self._ddp_ids = None

    def _export_table(self, table_name):
        fields = self._retrieve_fields(table_name)
        namespace, dev_name = self._parse_table_name(table_name)
        query = "SELECT {0} FROM {1}".format(",".join(fields), dev_name)
        if self._ddp_ids:
            if 'parent-relationship' in self._table_settings[table_name]:
                field_name = self._table_settings[table_name]['parent-relationship']['field']
            else:
                field_name = 'Id'
            query += " WHERE {0} IN ({1})".format(field_name, "'" + "','".join(self._ddp_ids) + "'")

        query += " ORDER BY {0}".format(self._unique_key)
        data = self._retrieve_data(dev_name, query)
        return {
            'fields': fields,
            'raw_data': data
        }

    @staticmethod
    def _persist_row(file_name, header, row):
        obj = OrderedDict()
        column_index = 0
        while column_index < len(row):
            obj[header[column_index]] = row[column_index]
            column_index += 1

        dir_name = os.path.dirname(file_name)
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
        with open(file_name, 'w') as file:
            file.write(yaml.dump(obj, default_flow_style=False, explicit_start=False, width=1024))

    def _get_ddp_data_dir_name(self, ddp_name):
        """ loop/data/<DDP-Name> """
        encoded_ddp_name = ddp_name.replace('/', '%2f')
        return os.path.join(self._get_data_dir(), encoded_ddp_name)

    def _get_parent_data_file_name(self, ddp_name, table_name):
        """ <DDP-Directory>/<Table-Alias>.yaml """
        return os.path.join(
            self._get_ddp_data_dir_name(ddp_name),
            "{0}.yaml".format(self._table_settings[table_name]['alias'])
        )

    def _get_child_data_file_name(self, table_name, parent_id, entity_name):
        """ <DDP-Directory>/<Table-Alias>/<Record-Name(s)>.yaml """
        encoded_parent_name = self._get_parent_record_name(table_name, parent_id).replace('/', '%2f')
        encoded_entity_name = entity_name.replace('/', '%2f')
        return os.path.join(
            self._get_data_dir(), *[
                encoded_parent_name,
                self._table_settings[table_name]['alias'],
                "{0}.yaml".format(encoded_entity_name)])

    def _get_parent_record_name(self, table_name, parent_id):
        parent_table_name = self._table_settings[table_name]['parent-relationship']['parent-table']
        header = self._data[parent_table_name]['header']
        name_fields = self._table_settings[parent_table_name]['name']
        if isinstance(name_fields, str):
            name_fields = [name_fields]
        name_field_indexes = list()
        for name_field in name_fields:
            name_field_indexes.append(header.index(name_field))
        names = list()
        for name_field_index in name_field_indexes:
            names.append(self._data[parent_table_name]['rows'][parent_id][name_field_index])
        return '-'.join(names)

    def _save_parent_data(self, table_name):
        header = self._data[table_name]['header']
        rows = self._data[table_name]['rows']
        name_fields = self._table_settings[table_name]['name']
        if isinstance(name_fields, str):
            name_fields = [name_fields]
        name_field_indexes = list()
        for name_field in name_fields:
            name_field_indexes.append(header.index(name_field))
        for row_index in rows:
            row = rows[row_index]
            names = list()
            for name_field_index in name_field_indexes:
                names.append(row[name_field_index])
            self._persist_row(
                self._get_parent_data_file_name('-'.join(names), table_name),
                header,
                row
            )

    def _save_child_data(self, table_name):
        header = self._data[table_name]['header']
        rows = self._data[table_name]['rows']
        if len(rows) == 0:
            return
        name_fields = self._table_settings[table_name]['name']
        if isinstance(name_fields, str):
            name_fields = [name_fields]
        name_field_indexes = list()
        for name_field in name_fields:
            name_field_indexes.append(header.index(name_field))
        parent_id_index = header.index(self._table_settings[table_name]['parent-relationship']['field'])
        for row_index in rows:
            row = rows[row_index]
            names = list()
            for name_field_index in name_field_indexes:
                names.append(row[name_field_index])
            self._persist_row(
                self._get_child_data_file_name(table_name, row[parent_id_index], '-'.join(names)),
                header,
                row
            )

    def _save_data(self):
        """ Save resulting data """
        yaml.add_representer(str, str_presenter)
        yaml.add_representer(OrderedDict, ordered_dict_presenter)

        table_settings = self._table_settings
        for table_name in table_settings:
            if 'parent-relationship' in table_settings[table_name]:
                self._save_child_data(table_name)
            else:
                self._save_parent_data(table_name)

    def _retrieve_ddp_ids(self, retrieve_all=False):
        key_field = 'Id'
        name_field = 'Name'
        date_field = 'LastModifiedDate'
        parent_table_name = 'Loop__DDP__c'
        query = "SELECT {0},{1},{2} FROM {3}".format(key_field, name_field, date_field, parent_table_name)
        if not retrieve_all:
            query += " WHERE Name IN ({0})".format("'" + "','".join(self._kwargs['ddp']) + "'")
        raw_data = self._retrieve_data(parent_table_name, query)
        header, rows = csvhelper.load_csv_with_one_id_key(raw_data, key_field)
        name_index = header.index(name_field)
        date_index = header.index(date_field)

        # Find the latest modified DDPs
        names = dict()
        for row_id in rows:
            row = rows[row_id]
            name = row[name_index]
            last_modified_date = datetime.strptime(row[date_index], '%Y-%m-%dT%H:%M:%S.%fZ')
            if name in names:
                if last_modified_date > names[name]['last_modified_date']:
                    names[name] = {'last_modified_date': last_modified_date, 'id': row_id}
            else:
                names[name] = {'last_modified_date': last_modified_date, 'id': row_id}

        # Extract Ids of the DDPs found above
        self._ddp_ids = list()
        for name in names:
            self._ddp_ids.append(names[name]['id'])

    def _export_data(self):
        # Check whether we need to export just a few DDPs
        if 'ddp' in self._kwargs:
            self._retrieve_ddp_ids()
        else:
            self._retrieve_ddp_ids(retrieve_all=True)

        # Export all tables first using Bulk API
        for table_name in self._table_settings:
            self._logger.info("  Exporting table: {0} ...".format(table_name))
            self._data[table_name] = self._export_table(table_name)

        # Load CSV from string and create maps ID -> Unique key and Unique key -> ID in memory
        for table_name in self._table_settings:
            self._data[table_name]['IdToUk'], \
                self._data[table_name]['UkToId'], \
                self._data[table_name]['header'], \
                self._data[table_name]['rows'] = csvhelper.load_csv_with_two_id_keys(
                    self._data[table_name]['raw_data'], 'Id', self._unique_key)

        # Replace IDs with external IDs (unique-key) and handle special fields
        for table_name in self._table_settings:
            table_settings = self._table_settings[table_name]
            self._logger.info("  Translating IDs in table {0} ...".format(table_name))

            # Replace ID values of lookup fields with unique key values
            if 'parent-relationship' in table_settings:
                self._replace_lookup_id_with_uk(table_name)

            # Execute special field handlers
            if 'field-handlers' in table_settings:
                for field_name in table_settings['field-handlers']:
                    self._logger.info("    Field: {0}".format(field_name))
                    self._decode_field(table_name, field_name)

        # Save data
        self._save_data()

    def _replace_lookup_id_with_uk(self, table_name):
        table_settings = self._table_settings[table_name]
        lookup_field = table_settings['parent-relationship']['field']
        self._logger.info("    Field: {0}".format(lookup_field))
        parent_table = table_settings['parent-relationship']['parent-table']
        id_to_uk = self._data[parent_table]['IdToUk']

        rows = self._data[table_name]['rows']
        if len(rows) == 0:
            return
        header = self._data[table_name]['header']
        lookup_field_index = header.index(lookup_field)
        for row_id in rows:
            row = rows[row_id]
            row[lookup_field_index] = id_to_uk[row[lookup_field_index]]

    def _decode_field(self, table_name, field_name):
        # Find handler class and create an instance of it
        handler_class_name = self._table_settings[table_name]['field-handlers'][field_name]['class']
        handler_class = self._locate_handler_class(handler_class_name)
        handler = handler_class(self._data[table_name], field_name)
        # Check whether handler class needs any other tables to be loaded
        tables_to_load = handler.tables_required_by_decode()
        required_data = {}
        for table_to_load in tables_to_load:
            raw_data = self._retrieve_data(table_to_load, tables_to_load[table_to_load]['query'])
            header, rows = csvhelper.load_csv_with_one_id_key(raw_data, tables_to_load[table_to_load]['id'])
            required_data[table_to_load] = {
                'header': header,
                'rows': rows
            }
        handler.set_required_data(required_data)
        # Convert values
        handler.decode()

    def _get_list_of_ddp_files(self):
        """ Get list of DDP files """
        rows = self._data['Loop.Loop__DDP_File__c']['rows']
        header = self._data['Loop.Loop__DDP_File__c']['header']
        document_index = header.index('Loop__Document_ID__c')

        documents = []
        for row_id in rows:
            documents.append(rows[row_id][document_index])

        return documents

    def _retrieve_files(self):
        """ Retrieves DDP files using Metadata API """
        options = {
            'single_package': 'true',
            'unpackaged': {
                'Document': self._get_list_of_ddp_files()
            }
        }
        async_process_id, state = self._mapi.retrieve(options)
        state, error_message, messages = self._mapi.check_retrieve_status(async_process_id)
        while state in ['InProgress', 'Pending']:
            time.sleep(5)
            state, error_message, messages = self._mapi.check_retrieve_status(async_process_id)

        # Print out any warnings
        for message in messages:
            self._logger.info("File: {0} Message: {1}".format(messages[message]['file'], messages[message]['message']))

        if state == "Succeeded":
            state, error_message, messages, zip_bytes = self._mapi.retrieve_zip(async_process_id)
            with io.BytesIO(zip_bytes) as zip_file:
                with zipfile.ZipFile(zip_file, 'r') as zip_file_obj:
                    zip_file_obj.extractall(path=self._get_loop_dir())
            os.remove(os.path.join(self._get_loop_dir(), "package.xml"))
        else:
            raise Exception("Could not retrieve DDP files: {0}".format(error_message))

    def _create_directories(self):
        loop_dir = self._get_loop_dir()
        data_dir = self._get_data_dir()

        # Delete DDP directories (so deleted records don't persist after export)
        if os.path.exists(data_dir) and os.path.isdir(data_dir):
            if 'ddp' in self._kwargs:
                # Delete DDP directories listed in the command line
                for ddp_name in self._kwargs['ddp']:
                    ddp_dir_name = self._get_ddp_data_dir_name(ddp_name)
                    if os.path.exists(ddp_dir_name) and os.path.isdir(ddp_dir_name):
                        shutil.rmtree(ddp_dir_name)
            else:
                # Delete data directory if we pull down all DDPs
                shutil.rmtree(data_dir)

        if not os.path.exists(loop_dir):
            self._logger.info("  Creating new source directory {0} ...".format(loop_dir))
            os.mkdir(loop_dir)

        if not os.path.exists(data_dir):
            self._logger.info("  Creating data directory {0} ...".format(data_dir))
            os.mkdir(data_dir)

    def do(self):
        self._logger.info("==> Creating directories ...")
        self._create_directories()

        self._logger.info("==> Connecting to Salesforce using {0} account ...".format(self._kwargs['username']))
        self._create_sfdc_session()

        self._logger.info("==> Exporting data ...")
        self._export_data()

        self._logger.info("==> Retrieving DDP files ...")
        self._retrieve_files()
