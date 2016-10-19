from commands.ddpcommandbase import DdpCommandBase
from helpers import csvhelper, yamlhelper

import os
import subprocess
import tempfile


class DdpPushIds(DdpCommandBase):
    """ Class handling export command """
    def __init__(self, settings, **kwargs):
        super().__init__(settings, **kwargs)
        self._data = {}
        self._ddp_ids = None

    def _select_latest_rows(self, table_name, header, rows, names, select_empty_ext_id_only):
        names_to_ids = dict()
        latest_rows = dict()
        name_indexes = list()

        last_modified_index = header.index('LastModifiedDate')
        for name in names:
            name_indexes.append(header.index(name))

        # Append parent's unique key for child tables
        if 'parent-relationship' in self._table_settings[table_name]:
            name_indexes.append(header.index(self._table_settings[table_name]['parent-relationship']['field']))

        for row_id in rows:
            row = rows[row_id]

            keys = list()
            for name_index in name_indexes:
                keys.append(row[name_index])

            key_tuple = tuple(keys)
            if key_tuple in latest_rows:
                if DdpCommandBase._convert_sfdc_date_time(row[last_modified_index]) < \
                   DdpCommandBase._convert_sfdc_date_time(latest_rows[key_tuple][last_modified_index]):
                    continue
            latest_rows[key_tuple] = row
            names_to_ids[key_tuple] = row_id

        # If overwrite option is not set. Do not update records that have GUID field already populated
        if select_empty_ext_id_only:
            key_tuples_to_remove = list()
            guid_index = header.index(self._unique_key)
            for key_tuple in latest_rows:
                if latest_rows[key_tuple][guid_index] != '':
                    key_tuples_to_remove.append(key_tuple)
            for key_tuple in key_tuples_to_remove:
                del latest_rows[key_tuple]
                del names_to_ids[key_tuple]

        # Create an index Id -> Composite name key
        ids_to_names = dict()
        for key_tuple in names_to_ids:
            ids_to_names[names_to_ids[key_tuple]] = key_tuple

        # Return table and additional index
        return latest_rows, ids_to_names, names_to_ids

    def _replace_parent_ids(self, table_name, header, rows):
        parent_table_name = self._table_settings[table_name]['parent-relationship']['parent-table']
        parent_field_name = self._table_settings[table_name]['parent-relationship']['parent-field']

        parent_header = self._data[parent_table_name]['header']
        parent_rows = self._data[parent_table_name]['rows']
        parent_ids_to_name = self._data[parent_table_name]['ids-to-names']
        parent_field_index = parent_header.index(parent_field_name)

        field_name = self._table_settings[table_name]['parent-relationship']['field']
        field_index = header.index(field_name)

        missing_row_ids = list()
        for row_id in rows:
            row = rows[row_id]
            parent_id = row[field_index]
            if parent_id not in parent_ids_to_name:
                missing_row_ids.append(row_id)
                continue
            name_tuple = parent_ids_to_name[parent_id]
            row[field_index] = parent_rows[name_tuple][parent_field_index]

        # Remove rows that don't have parent records
        for row_id in missing_row_ids:
            del rows[row_id]

    def _get_parent_ids(self, table_name):
        parent_table_name = self._table_settings[table_name]['parent-relationship']['parent-table']
        parent_ids_to_name = self._data[parent_table_name]['ids-to-names']
        return list(parent_ids_to_name.keys())

    def _export_unique_keys(self, table_name, select_empty_ext_id_only):
        table_settings = self._table_settings[table_name]
        fields = ['Id', self._unique_key, 'LastModifiedDate']
        if isinstance(table_settings['name'], str):
            names = [table_settings['name']]
        else:
            names = table_settings['name']
        for field_name in names:
            fields.append(field_name)
        if 'parent-relationship' in table_settings:
            fields.append(table_settings['parent-relationship']['field'])
        namespace, dev_name = self._parse_table_name(table_name)
        query = "SELECT {0} FROM {1}".format(','.join(fields), dev_name)
        if 'parent-relationship' in table_settings:
            parent_ids = self._get_parent_ids(table_name)
            query += " WHERE {0} IN ({1})".format(
                table_settings['parent-relationship']['field'],
                "'" + "','".join(parent_ids) + "'"
            )
        raw_data = self._retrieve_data(dev_name, query)
        header, all_rows = csvhelper.load_csv_with_one_id_key(raw_data, 'Id')
        if 'parent-relationship' in table_settings:
            self._replace_parent_ids(table_name, header, all_rows)
        rows, ids_to_names, names_to_ids = self._select_latest_rows(table_name, header, all_rows, names, select_empty_ext_id_only)
        return {
            'header': header,
            'rows': rows,
            'ids-to-names': ids_to_names,
            'names-to-ids': names_to_ids
        }

    def _load_data(self):
        """ Loads data from YAML files from the source directory """
        files = dict()
        for table_name in self._table_settings:
            files[table_name] = list()

        # List all yaml files tracked by Git
        args = [
            "git",
            "ls-files",
            self._get_relative_data_dir()
        ]
        output_bytes = subprocess.check_output(args, cwd=self._source_dir)
        # Convert escaped unicode characters to utf-8, then to latin_1, then to utf-8 again
        # b'Ench\\303\\250res' -> u'Ench\xc3\xa8res' -> b'Ench\xc3\xa8res' -> u'Enchères'
        output = output_bytes.decode('unicode_escape').encode('latin_1').decode('utf-8')

        # Parse the output to extract file names and corresponding table names
        for line in output.split('\n'):
            # Skip empty lines
            if line.strip() == '':
                continue
            # Trim double-quotes
            if line[0] == '"':
                filename = line[1:-1]
            else:
                filename = line
            full_filename = os.path.normpath(filename)
            table_name = self._extract_table_name_from_path(full_filename)
            files[table_name].append(full_filename)

        # Load data
        for table_name in self._table_settings:
            print("  Loading {0} table ...".format(table_name))
            header, rows = yamlhelper.load_multiple_yaml(files[table_name], self._unique_key, self._source_dir)
            self._data[table_name] = {
                'src_header': header,
                'src_rows': rows
            }

    def _lookup_record_by_name_tuple(self, table_name, name_tuple):
        if name_tuple in self._data[table_name]['rows']:
            return name_tuple

        return None

    def _replace_id_values(self, table_name):
        src_header = self._data[table_name]['src_header']
        src_rows = self._data[table_name]['src_rows']

        table_settings = self._table_settings[table_name]
        # Find indexes of name columns
        names = table_settings['name']
        if isinstance(names, str):
            names = [names]
        name_indexes = list()
        for name in names:
            name_indexes.append(src_header.index(name))
        if 'parent-relationship' in table_settings:
            name_indexes.append(src_header.index(table_settings['parent-relationship']['field']))

        header = self._data[table_name]['header']
        rows = self._data[table_name]['rows']
        ext_id_index = header.index(self._unique_key)

        for ext_id in src_rows:
            src_row = src_rows[ext_id]
            # Compose name tuple
            keys = list()
            for name_index in name_indexes:
                keys.append(src_row[name_index])

            row_id = self._lookup_record_by_name_tuple(table_name, tuple(keys))
            if row_id is None:
                continue

            row = rows[row_id]
            row[ext_id_index] = ext_id

    def _remove_columns(self, table_name, field_names):
        header = self._data[table_name]['header']
        field_indexes = list()
        # Find indexes of fields to be removed
        for field_name in field_names:
            field_indexes.append(header.index(field_name))

        # Remove fields from header
        for field_name in field_names:
            header.remove(field_name)

        # Remove fields from data
        rows = self._data[table_name]['rows']
        for row_id in rows:
            row = rows[row_id]
            new_row = list()
            index = 0
            while index < len(row):
                if index not in field_indexes:
                    new_row.append(row[index])
                index += 1
            rows[row_id] = new_row

    def _update_batch(self, table_name, rows):
        temp_file = tempfile.TemporaryFile(mode='w+', encoding='utf-8', newline='')
        # Save resulting table as CSV
        csvhelper.save_csv_with_ids(
            temp_file.file,
            self._data[table_name]['header'],
            rows,
            self._data[table_name]['names-to-ids']
        )
        # Load CSV as text
        namespace, dev_name = table_name.split('.')
        temp_file.file.seek(0)
        csv_data = temp_file.read()
        # Update data in Salesforce
        print("  Updating data in {0} table ...".format(table_name))
        status = self._update_data(dev_name, csv_data)
        if int(status['failed']) > 0:
            raise Exception("    Could not update {0} row(s)\n{1}".format(status['failed'], status['results']))

    def _update_table(self, table_name):
        # Remove all columns but Id and external Id
        field_names = list()
        if isinstance(self._table_settings[table_name]['name'], str):
            field_names.append(self._table_settings[table_name]['name'])
        else:
            field_names = field_names + self._table_settings[table_name]['name']
        field_names.append('LastModifiedDate')
        # Append parent's unique key for child tables
        if 'parent-relationship' in self._table_settings[table_name]:
            field_names.append(self._table_settings[table_name]['parent-relationship']['field'])
        self._remove_columns(table_name, field_names)
        rows = self._data[table_name]['rows']
        row_count = 0
        batch = dict()
        for row_id in rows:
            batch[row_id] = rows[row_id]
            row_count += 1
            if row_count % self._update_batch_size == 0:
                self._update_batch(table_name, batch)
                batch = dict()

        if len(batch) > 0:
            self._update_batch(table_name, batch)

    def _update_ids(self):
        # Update external IDs
        ordered_import_list = self._resolve_import_order(list(self._table_settings.keys()))

        # Update external IDs starting from the parent table
        import_order = 0
        while import_order < len(ordered_import_list):
            table_name = ordered_import_list[import_order]
            print("  Exporting {0} table ...".format(table_name))
            self._data[table_name].update(self._export_unique_keys(table_name, not self._kwargs['overwrite']))

            if len(self._data[table_name]['rows']) == 0:
                print("  No rows to update")
            else:
                # Replace Ids with GUIDs from source directory
                self._replace_id_values(table_name)
                # Update data in Salesforce
                self._update_table(table_name)

            # Download data again so we have GUIDs in memory
            self._data[table_name].update(self._export_unique_keys(table_name, False))
            import_order += 1

    def do(self):
        print("==> Loading data from source directory ...")
        self._load_data()

        print("==> Connecting to Salesforce using {0} account ...".format(self._kwargs['username']))
        self._create_sfdc_session()

        print("==> Updating IDs ...")
        self._update_ids()
