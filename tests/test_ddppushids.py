from commands.ddppushids import DdpPushIds
import unittest


class DdpPushIdsTest(unittest.TestCase):
    @staticmethod
    def _setup_overwrite(overwrite):
        settings = {
            'excluded-fields': [],
            'unique-key': 'DDP_Migrator_Id__c',
            'update-batch-size': '100',
            'tables': {
                'Loop.Loop__DDP__c': {
                    'name': 'Name',
                    'alias': 'DDP',
                    'field-handlers': {
                        'RecordTypeId': {'class': 'RecordTypeIdHandler'},
                        'Loop__Security__c': {'class': 'LoopSecurityHandler'}
                    }
                }
            }
        }
        kwargs = {
            'source_dir': '..',
            'overwrite': overwrite
        }
        return DdpPushIds(settings, **kwargs)

    def test_select_latest_rows_no_overwrite_empty_guid(self):
        header = ['DDP_Migrator_Id__c', 'LastModifiedDate', 'Name']
        all_rows = {
            'a1t7A000003SnrZQAS': ['', '2016-10-13T20:39:50.000Z', 'DDP Name'],
            'a1t7A000003SnqkQAC': ['', '2016-10-15T20:41:07.000Z', 'DDP Name'],
            'a1t7A000003SnskQAC': ['', '2016-10-12T20:38:17.000Z', 'DDP Name']
        }

        command = self._setup_overwrite(False)
        rows, ids_to_names, names_to_ids = \
            command._select_latest_rows('Loop.Loop__DDP__c', header, all_rows, ['Name'], True)

        self.assertEqual({tuple(['DDP Name']): ['', '2016-10-15T20:41:07.000Z', 'DDP Name']}, rows)
        self.assertEqual({'a1t7A000003SnqkQAC': tuple(['DDP Name'])}, ids_to_names)

    def test_select_latest_rows_no_overwrite_guid_already_set(self):
        header = ['DDP_Migrator_Id__c', 'LastModifiedDate', 'Name']
        all_rows = {
            'a1t7A000003SnrZQAS': ['', '2016-10-13T20:39:50.000Z', 'DDP Name'],
            'a1t7A000003SnqkQAC': ['F6B2A31C-8AF9-CD20-AF30-7191D98CEE85', '2016-10-15T20:41:07.000Z', 'DDP Name'],
            'a1t7A000003SnskQAC': ['', '2016-10-12T20:38:17.000Z', 'DDP Name']
        }

        command = self._setup_overwrite(False)
        rows, ids_to_names, names_to_ids = \
            command._select_latest_rows('Loop.Loop__DDP__c', header, all_rows, ['Name'], True)

        self.assertEqual({}, rows)
        self.assertEqual({}, ids_to_names)

    def test_select_latest_rows_overwrite_empty_guid(self):
        header = ['DDP_Migrator_Id__c', 'LastModifiedDate', 'Name']
        all_rows = {
            'a1t7A000003SnrZQAS': ['', '2016-10-13T20:39:50.000Z', 'DDP Name'],
            'a1t7A000003SnqkQAC': ['', '2016-10-15T20:41:07.000Z', 'DDP Name'],
            'a1t7A000003SnskQAC': ['', '2016-10-12T20:38:17.000Z', 'DDP Name']
        }

        command = self._setup_overwrite(True)
        rows, ids_to_names, names_to_ids = \
            command._select_latest_rows('Loop.Loop__DDP__c', header, all_rows, ['Name'], False)

        self.assertEqual({tuple(['DDP Name']): ['', '2016-10-15T20:41:07.000Z', 'DDP Name']}, rows)
        self.assertEqual({'a1t7A000003SnqkQAC': tuple(['DDP Name'])}, ids_to_names)

    def test_select_latest_rows_overwrite_guid_already_set(self):
        header = ['DDP_Migrator_Id__c', 'LastModifiedDate', 'Name']
        all_rows = {
            'a1t7A000003SnrZQAS': ['', '2016-10-13T20:39:50.000Z', 'DDP Name'],
            'a1t7A000003SnqkQAC': ['F6B2A31C-8AF9-CD20-AF30-7191D98CEE85', '2016-10-15T20:41:07.000Z', 'DDP Name'],
            'a1t7A000003SnskQAC': ['', '2016-10-12T20:38:17.000Z', 'DDP Name']
        }

        command = self._setup_overwrite(True)
        rows, ids_to_names, names_to_ids = \
            command._select_latest_rows('Loop.Loop__DDP__c', header, all_rows, ['Name'], False)

        self.assertEqual(
            {tuple(['DDP Name']): ['F6B2A31C-8AF9-CD20-AF30-7191D98CEE85', '2016-10-15T20:41:07.000Z', 'DDP Name']},
            rows)
        self.assertEqual({'a1t7A000003SnqkQAC': tuple(['DDP Name'])}, ids_to_names)

if __name__ == '__main__':
    unittest.main()
