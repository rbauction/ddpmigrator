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
                },
                'Loop.Loop__Related_Object__c': {
                    'name': 'Name',
                    'alias': 'Relationships',
                    'parent-relationship': {
                        'field': 'Loop__DDP__c',
                        'parent-table': 'Loop.Loop__DDP__c',
                        'parent-field': 'DDP_Migrator_Id__c'
                    },
                    'field-handlers': {
                        'RecordTypeId': {
                            'class': 'LoopParentObjectHandler',
                            'retry-failed': 'true'
                        },
                        'Loop__Security__c': {
                            'class': 'LoopParentObjectHandler',
                            'retry-failed': 'true'
                        }
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

    def test_replace_parent_ids(self):
        command = self._setup_overwrite(False)
        command._data['Loop.Loop__DDP__c'] = {
            'header': ['DDP_Migrator_Id__c', 'LastModifiedDate', 'Name'],
            'rows': {
                ('DDP Three',): ['FE9C85E3-CA64-EC16-85C1-FE9EFAF15489', '2016-11-24T00:38:03.000Z', 'DDP Three'],
                ('DDP Two',): ['190A17CB-FA65-FC07-86B4-0B71417026C6', '2016-11-24T00:37:50.000Z', 'DDP Two'],
                ('DDP One',): ['8ECD16D7-D5DC-41D8-DE5B-2423E21004CB', '2016-11-24T00:39:25.000Z', 'DDP One']
            },
            'ids-to-names': {
                'a1tW0000001fkWkIAI': ('DDP One',),
                'a1tW0000001fkWrIAI': ('DDP Two',),
                'a1tW0000001fkWSIAY': ('DDP Three',)
            }
        }
        header = ['DDP_Migrator_Id__c', 'LastModifiedDate', 'Loop__Index__c', 'Name', 'Loop__DDP__c']
        rows = {
            'a22W0000006hPKoIAM': [
                'B6D0F499-5CC7-DB80-89BC-A643CDCBCE28',
                '2016-11-24T00:38:03.000Z',
                '10.0',
                'Object_One__c',
                'a1tW0000001fkWSIAY'
            ],
            'a22W0000006hPSeIAM': [
                '2615B1DF-7A92-ED68-DEB2-D7A25AB1622D',
                '2016-11-24T00:36:57.000Z',
                '7.0',
                'Object_Two__c',
                'a1tW0000001fkWkIAI'
            ],
            'a22W0000006hPfsIAE': [
                '99F88EB6-388F-20CB-D942-A2EA30A4CCC5',
                '2016-11-24T00:37:50.000Z',
                '18.0',
                'Object_Three__c',
                'a1tW0000001fkWrIAI'
            ],
            'a22W0000007hfPsIAM': [
                '00F88EB6-388F-20CB-D942-A2EA30A4CC11',
                '2016-11-24T00:37:50.000Z',
                '18.0',
                'Missing_Object__c',
                'a1tW1234561fkWrIAI'
            ]
        }
        command._replace_parent_ids('Loop.Loop__Related_Object__c', header, rows)
        self.assertEqual({
            'a22W0000006hPKoIAM': [
                'B6D0F499-5CC7-DB80-89BC-A643CDCBCE28',
                '2016-11-24T00:38:03.000Z',
                '10.0',
                'Object_One__c',
                'FE9C85E3-CA64-EC16-85C1-FE9EFAF15489'
            ],
            'a22W0000006hPSeIAM': [
                '2615B1DF-7A92-ED68-DEB2-D7A25AB1622D',
                '2016-11-24T00:36:57.000Z',
                '7.0',
                'Object_Two__c',
                '8ECD16D7-D5DC-41D8-DE5B-2423E21004CB'
            ],
            'a22W0000006hPfsIAE': [
                '99F88EB6-388F-20CB-D942-A2EA30A4CCC5',
                '2016-11-24T00:37:50.000Z',
                '18.0',
                'Object_Three__c',
                '190A17CB-FA65-FC07-86B4-0B71417026C6'
            ]
        }, rows)


if __name__ == '__main__':
    unittest.main()
