""" Handler class for Loop__DDP_Files__c field """
from fieldhandlers.field_handler_base import FieldHandlerBase


class LoopDDPFilesHandler(FieldHandlerBase):
    """ Handler class for Loop__DDP_Files__c field """

    _TABLES_REQUIRED_BY_DECODE = {
        'Loop__DDP_File__c': {
            'id': 'Id',
            'query': 'SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP_File__c'
        }
    }

    _TABLES_REQUIRED_BY_ENCODE = {
        'Loop__DDP_File__c': {
            'id': 'Id',
            'query': 'SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP_File__c'
        }
    }

    def _decode_one_value(self, value):
        """ Converts semi-colon separated list of Loop DDP File IDs into list of DDP File Migrator IDs """
        header = self._required_data['Loop__DDP_File__c']['header']
        mig_id_index = header.index('DDP_Migrator_Id__c')
        ddpfile_migratorids = list()
        for ddpfile_id in value.split(';'):
            if self._is_id_in_list(ddpfile_id, 'Loop__DDP_File__c'):
                table_name = 'Loop__DDP_File__c'
            else:
                raise Exception(
                    "Could not find {0} id in Loop__DDP_File__c tables".format(ddpfile_id))

            row = self._get_value_by_id(ddpfile_id, table_name)
            ddpfile_migratorids.append("{0}".format(row[mig_id_index]))
        return ";".join(ddpfile_migratorids)

    def _lookup_id_by_name(self, ddpFilesGuid):
        header = self._required_data['Loop__DDP_File__c']['header']
        rows = self._required_data['Loop__DDP_File__c']['rows']
        mig_id_index = header.index('DDP_Migrator_Id__c')
        for row_id, row in rows.items():
            if ddpFilesGuid == row[mig_id_index]:
                return row_id
        raise Exception("Could not convert value [{0}]".format(ddpFilesGuid))

    def _encode_one_value(self, value, value_row_id):
        ddpfiles_GUIDnames = list()
        for ddpfile in value.split(';'):
            ddpfiles_GUIDnames.append(ddpfile)

        ddpfiles_ids = list()
        # Convert each value
        for ddpFilesGuid in ddpfiles_GUIDnames:
            ddpfiles_ids.append(self._lookup_id_by_name(ddpFilesGuid))

        return ";".join(ddpfiles_ids)
