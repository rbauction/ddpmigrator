from commands.ddpcommandbase import DdpCommandBase
from helpers import csvhelper, yamlhelper

import copy
import glob
import os
import shutil
import subprocess
import tempfile
import time
import zipfile


class DdpImport(DdpCommandBase):
    """ Import command handler class """
    _PACKAGE_XML_START = """<?xml version="1.0" encoding="UTF-8"?>
<Package xmlns="http://soap.sforce.com/2006/04/metadata">
"""

    _PACKAGE_XML_END = """    <version>{apiVersion}</version>
</Package>
"""

    """ Class handling import command """
    def __init__(self, settings, **kwargs):
        super().__init__(settings, **kwargs)
        self._data = dict()
        self._data_tempdir = None
        self._file_tempdir = None
        self._import_tempdir = None
        self._data_changes = {'modified': list(), 'added': list()}
        self._file_changes = {'modified': list(), 'added': list()}
        self._changed_tables = dict()
        self._loaded_tables = list()

    def _find_new_and_changed_files(self):
        # Find added or modified files using git diff
        args = [
            "git",
            "diff",
            "--no-renames",
            "--name-status",
            "--diff-filter=AM",
            "{0}..HEAD".format(self._kwargs['baseline']),
            "."
        ]
        output_bytes = subprocess.check_output(args, cwd=self._get_loop_dir())
        self._process_git_diff_output(output_bytes)

    def _process_git_diff_output(self, output_bytes):
        # Convert escaped unicode characters to utf-8, then to latin_1, then to utf-8 again
        # b'Ench\\303\\250res' -> u'Ench\xc3\xa8res' -> b'Ench\xc3\xa8res' -> u'Enchères'
        output = output_bytes.decode('unicode_escape').encode('latin_1').decode('utf-8')

        # Parse the output to extract file names and their statuses
        for line in output.split('\n'):
            # Skip empty lines
            if line.strip() == '':
                continue
            # Extract letter and file name
            mod, filename = line.split('\t')
            if mod not in ['A', 'M']:
                raise Exception("Unexpected status: {0}".format(mod))
            # Trim double-quotes
            if filename[0] == '"':
                filename = filename[1:-1]
            full_filename = os.path.normpath(filename)
            # Add file to corresponding change group
            if full_filename.startswith(self._get_relative_data_dir()):
                if mod == 'M':
                    self._data_changes['modified'].append(full_filename)
                else:
                    self._data_changes['added'].append(full_filename)
            else:
                if mod == 'M':
                    self._file_changes['modified'].append(full_filename)
                else:
                    self._file_changes['added'].append(full_filename)

    def _fake_new_files_for_ddp_list(self):
        # Add data files to the list of changed/new files first
        temp_dir = tempfile.TemporaryDirectory()
        for ddp in self._kwargs['ddp']:
            # Find added files using git diff
            args = [
                "git",
                "diff",
                "--no-index",
                "--name-status",
                "--diff-filter=AM",
                temp_dir.name,
                "{0}\\{1}".format(self._get_relative_data_dir(), ddp)
            ]
            try:
                # We expect this command to fail (exit code 1). This is how git diff works
                subprocess.check_output(args, cwd=self._source_dir)
            except subprocess.CalledProcessError as ex:
                self._process_git_diff_output(ex.output)
        temp_dir.cleanup()
        # Find new files in Files directories, extract Loop__Document_ID__c field and add it to changed files
        for data_file in self._data_changes['added']:
            if '\\Files\\' in data_file:
                ddp_file = self._extract_ddp_file_name(data_file)
                wildcard = os.path.join(self._source_dir, *[self._get_relative_docs_dir(), "{0}*".format(ddp_file)])
                for file_name in glob.glob(wildcard):
                    full_filename = os.path.relpath(os.path.normpath(file_name), self._source_dir)
                    self._file_changes['added'].append(full_filename)

    def _extract_ddp_file_name(self, file_name):
        obj = yamlhelper.load_one_yaml(os.path.join(self._source_dir, file_name))
        return obj['Loop__Document_ID__c']

    def _copy_file_to_temp(self, filename):
        rel_path = self._get_relative_file_path(filename)
        temp_file = os.path.join(self._file_tempdir.name, rel_path)
        temp_file_dir = os.path.dirname(temp_file)
        if not os.path.exists(temp_file_dir):
            os.makedirs(temp_file_dir)
        # Copy file
        shutil.copyfile(os.path.join(self._source_dir, filename), temp_file)
        # Copy file's meta.xml
        shutil.copyfile(
            "{0}-meta.xml".format(os.path.join(self._source_dir, filename)),
            "{0}-meta.xml".format(temp_file)
        )

    def _generate_file_delta(self):
        # Process new files
        for filename in self._file_changes['added']:
            # Skip meta files
            if filename.endswith('-meta.xml'):
                continue
            self._logger.info("  Added: {0}".format(filename))
            self._copy_file_to_temp(filename)
        # Process changed files
        for filename in self._file_changes['modified']:
            # Skip meta files
            if filename.endswith('-meta.xml'):
                continue
            self._logger.info("  Modified: {0}".format(filename))
            self._copy_file_to_temp(filename)

    def _create_temp_dirs(self):
        self._data_tempdir = tempfile.TemporaryDirectory()
        self._import_tempdir = os.path.join(self._data_tempdir.name, "import")
        os.mkdir(self._import_tempdir)
        self._file_tempdir = tempfile.TemporaryDirectory()

    def _calculate_delta(self):
        self._create_temp_dirs()
        if 'ddp' in self._kwargs:
            self._fake_new_files_for_ddp_list()
        else:
            self._find_new_and_changed_files()
        self._find_changed_tables()
        self._generate_file_delta()

    def _find_changed_tables(self):
        file_names = self._data_changes['added'] + self._data_changes['modified']
        for file_name in file_names:
            self._logger.info("Processing: {0}".format(file_name))
            tail = os.path.basename(file_name)
            if not tail.endswith('.yaml'):
                raise Exception(".yaml extension was expected")

            table_name = self._extract_table_name_from_path(file_name)

            if table_name not in self._changed_tables and table_name not in self._table_settings:
                raise Exception("Settings for table {0} are missing".format(table_name))

            if table_name not in self._changed_tables:
                self._changed_tables[table_name] = list()
            self._changed_tables[table_name].append(file_name)

    def _load_changed_rows(self, changed_rows):
        """ Loads YAML from temporary data/delta directory and converts it to CSV """
        header, rows = yamlhelper.load_multiple_yaml(changed_rows, self._unique_key, self._source_dir)
        return {
            'header': header,
            'rows': rows
        }

    def _export_parent_table_ids(self, table_name):
        parent_namespace, parent_object = \
            self._table_settings[table_name]['parent-relationship']['parent-table'].split('.')
        parent_unique_key = self._table_settings[table_name]['parent-relationship']['parent-field']
        query = 'SELECT Id,{0} FROM {1}'.format(parent_unique_key, parent_object)
        self._logger.info("      Exporting {0} object from Salesforce ...".format(parent_object))
        parent_raw_data = self._retrieve_data(parent_object, query)
        return csvhelper.load_csv_with_one_id_key(parent_raw_data, parent_unique_key)

    def _update_lookup_field_values(self, table_name, lookup_field):
        parent_header, parent_rows = self._export_parent_table_ids(table_name)
        lookup_field_index = self._data[table_name]['header'].index(lookup_field)
        rows = self._data[table_name]['rows']
        for row_id in rows:
            row_list = list(rows[row_id])
            id_value = parent_rows[row_list[lookup_field_index]][0]
            row_list[lookup_field_index] = id_value
            rows[row_id] = tuple(row_list)

    def _load_table(self, table_name):
        """ Loads table and its parents if applicable and converts values """
        # Skip already loaded tables
        if table_name in self._loaded_tables:
            return

        # Load parents
        for parent in self._reverse[table_name]:
            self._load_table(parent)
        self._logger.info("Loading {0} table ...".format(table_name))

        # Load table into memory
        self._data[table_name] = self._load_changed_rows(self._changed_tables[table_name])
        self._loaded_tables.append(table_name)

    def _encode_field(self, table_name, field_name):
        # Find handler class and create an instance of it
        handler_class_name = self._table_settings[table_name]['field-handlers'][field_name]['class']
        is_retry_failed = False
        if 'retry-failed' in self._table_settings[table_name]['field-handlers'][field_name]:
            is_retry_failed = bool(self._table_settings[table_name]['field-handlers'][field_name]['retry-failed'])
        handler_class = self._locate_handler_class(handler_class_name)
        handler = handler_class(self._data[table_name], field_name, is_retry_failed)
        # Check whether handler class needs any other tables to be loaded
        tables_to_load = handler.tables_required_by_encode()
        required_data = dict()
        for table_to_load in tables_to_load:
            self._logger.info("      Exporting {0} object from Salesforce ...".format(table_to_load))
            raw_data = self._retrieve_data(table_to_load, tables_to_load[table_to_load]['query'])
            header, rows = csvhelper.load_csv_with_one_id_key(raw_data, tables_to_load[table_to_load]['id'])
            required_data[table_to_load] = {
                'header': header,
                'rows': rows
            }
        handler.set_required_data(required_data)
        # Convert values
        handler.encode()

        return handler.get_encoded_rows()

    def _save_table_as_csv(self, table_name, encoded_rows):
        csvhelper.save_csv(
            os.path.join(self._import_tempdir, table_name),
            self._data[table_name]['header'],
            encoded_rows)

    def _convert_field_values(self, table_name):
        is_retry_failed = False
        table_settings = self._table_settings[table_name]
        # Replace IDs with external IDs (lookup fields) and handle special fields
        self._logger.info("  Translating IDs in table {0} ...".format(table_name))

        # Convert lookup field values
        if 'parent-relationship' in table_settings:
            lookup_field = table_settings['parent-relationship']['field']
            self._logger.info("    Field: {0}".format(lookup_field))
            self._update_lookup_field_values(table_name, lookup_field)

        encoded_rows = dict()
        # Execute special field handlers
        if 'field-handlers' in table_settings:
            for field_name in table_settings['field-handlers']:
                self._logger.info("    Field: {0}".format(field_name))
                if 'retry-failed' in table_settings['field-handlers'][field_name]:
                    is_retry_failed |= bool(table_settings['field-handlers'][field_name]['retry-failed'])
                encoded_rows = self._encode_field(table_name, field_name)

        return is_retry_failed, encoded_rows

    def _import_data(self):
        """ Loads data, converts values and imports it into Salesforce """
        # Load data
        for table_name in self._changed_tables:
            self._load_table(table_name)

        ordered_import_list = self._resolve_import_order(self._changed_tables)

        # Convert values and import data. Data should be imported in the right order
        import_order = 0
        while import_order < len(ordered_import_list):
            table_name = ordered_import_list[import_order]
            row_count = len(self._data[table_name]['rows'])
            self._logger.info("Importing {0} table ({1} row(s))...".format(table_name, row_count))
            if self._table_settings[table_name]['recreate-on-import']:
                self._delete_records(table_name)
            should_retry = True
            last_count = -1
            while should_retry:
                rows_backup = copy.deepcopy(self._data[table_name]['rows'])
                is_retry_failed, encoded_rows = self._convert_field_values(table_name)
                self._save_table_as_csv(table_name, encoded_rows)
                encoded_row_count = len(encoded_rows)
                self._logger.info("  Upserting {0} row(s) ...".format(encoded_row_count))
                self._import_table(table_name)
                if last_count == encoded_row_count:
                    raise Exception("Unable to import {0} row(s)".format(row_count - encoded_row_count))
                if not is_retry_failed or encoded_row_count == row_count:
                    should_retry = False
                else:
                    self._logger.info("  Could not import {0} row(s). Will retry ...".format(row_count - encoded_row_count))
                    self._data[table_name]['rows'] = rows_backup
                    last_count = encoded_row_count
            import_order += 1

    def _delete_records(self, table_name):
        """ Deletes records from specified table based on parent relationship """
        # Find unique parent IDs
        parent_ids = list()
        parent_guids = list()
        parent_key_field = self._table_settings[table_name]['parent-relationship']['field']
        parent_key_field_index = self._data[table_name]['header'].index(parent_key_field)
        for row_id, row in self._data[table_name]['rows'].items():
            if row[parent_key_field_index] not in parent_guids:
                parent_guids.append(row[parent_key_field_index])
        # Convert parent GUIDs to record IDs
        parent_header, parent_rows = self._export_parent_table_ids(table_name)
        for guid in parent_guids:
            if guid in parent_rows.keys():
                parent_ids.append(parent_rows[guid][0])
        # Abort deletion if there is nothing to delete
        if len(parent_ids) == 0:
            return
        # Retrieve IDs of children
        dev_namespace, dev_name = table_name.split('.')
        query = "SELECT Id FROM {0} WHERE {1} IN ('{2}')".format(dev_name, parent_key_field, "','".join(parent_ids))
        self._logger.info("  Retrieving IDs of old records ...")
        ids_to_delete = self._retrieve_data(dev_name, query)
        # Abort deletion if there is nothing to delete
        if len(ids_to_delete) == 0:
            return
        # Delete records by IDs
        self._logger.info("  Deleting old records ...")
        self._delete_data(dev_name, ids_to_delete)

    def _import_table(self, table_name):
        with open(os.path.join(self._import_tempdir, table_name), mode='r', encoding='utf-8') as csv_file:
            csv_data = csv_file.read()
            csv_file.close()
        namespace, dev_name = table_name.split('.')
        status = self._upsert_data(dev_name, csv_data, self._unique_key)
        if int(status['failed']) > 0:
            raise Exception("   Could not upsert {0} row(s)\n{1}".format(status['failed'], status['results']))

    def _create_package_xml(self):
        package_xml = self._PACKAGE_XML_START
        # Create package.xml
        package_xml += "    <types>\n"
        for filename in self._file_changes['added'] + self._file_changes['modified']:
            document_path = os.path.relpath(filename, os.path.join(self._get_relative_loop_dir(), 'documents'))
            package_xml += "        <members>{0}</members>\n".format(document_path.replace("\\", "/"))

        package_xml += "        <name>Document</name>\n    </types>\n"
        package_xml += self._PACKAGE_XML_END.format(**{'apiVersion': self._kwargs['api_version']})
        # Save package.xml in the files' temp directory
        package_xml_file = os.path.join(self._file_tempdir.name, "package.xml")
        with open(package_xml_file, mode='w', encoding='utf-8') as file:
            file.write(package_xml)
            file.close()

    def _create_deploy_zip_file(self):
        zip_temp_file = tempfile.TemporaryFile()
        zip_file = zipfile.ZipFile(zip_temp_file, mode='w', compression=zipfile.ZIP_DEFLATED)
        for root, subdirs, files in os.walk(self._file_tempdir.name):
            # Skip empty directories
            if len(files) == 0:
                continue
            for file in files:
                file_rel_name = os.path.join(os.path.relpath(root, self._file_tempdir.name), file)
                zip_file.write(os.path.join(root, file), arcname=file_rel_name)
        zip_file.close()
        return zip_temp_file

    def _import_files(self):
        # Create package.xml file
        self._create_package_xml()
        # Create zip file
        zip_file = self._create_deploy_zip_file()
        # Deploy zip file
        options = {
            'checkonly': 'false',
            'testlevel': 'NoTestRun'
        }
        deployment_id, deployment_state = self._mapi.deploy(zip_file, options)
        self._logger.info("  State: {0}".format(deployment_state))
        while deployment_state in ['Queued', 'Pending', 'InProgress']:
            time.sleep(5)
            deployment_state, state_detail, deployment_detail, unit_test_detail = \
                self._mapi.check_deploy_status(deployment_id)
            if state_detail is None:
                self._logger.info("  State: {0}".format(deployment_state))
            else:
                if int(deployment_detail['deployed_count']) + \
                        int(deployment_detail['failed_count']) < \
                        int(deployment_detail['total_count']):
                    progress = "(%s/%s) " % (
                        deployment_detail['deployed_count'] +
                        deployment_detail['failed_count'],
                        deployment_detail['total_count']
                    )
                else:
                    progress = "(%s/%s) " % (
                        unit_test_detail['completed_count'],
                        unit_test_detail['total_count']
                    )

                self._logger.info("  State: {0} - {1}{2}".format(deployment_state, progress, state_detail))

        if deployment_state != 'Succeeded':
            raise Exception('Deployment of documents failed')

    def do(self):
        self._logger.info("==> Calculating delta ...")
        self._calculate_delta()

        self._logger.info("==> Connecting to Salesforce using {0} account ...".format(self._kwargs['username']))
        self._create_sfdc_session()

        # Documents need to be imported first so we can reference Document Id in Loop__DDP_File__c
        self._logger.info("==> Importing documents ...")
        self._import_files()

        self._logger.info("==> Importing data ...")
        self._import_data()
