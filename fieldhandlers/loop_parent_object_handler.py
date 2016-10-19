""" Handler class for Loop__Parent_Object__c field """
from fieldhandlers.field_handler_base import FieldHandlerBase


class LoopParentObjectHandler(FieldHandlerBase):
    """ Handler class for Loop__Parent_Object__c field """

    _TABLES_REQUIRED_BY_DECODE = {
        'Loop__Related_Object__c': {
            'id': 'Id',
            'query': 'SELECT Id,DDP_Migrator_Id__c FROM Loop__Related_Object__c'
        }
    }

    _TABLES_REQUIRED_BY_ENCODE = {
        'Loop__Related_Object__c': {
            'id': 'Id',
            'query': 'SELECT Id,DDP_Migrator_Id__c FROM Loop__Related_Object__c'
        }
    }

    def _decode_one_value(self, value):
        if value not in self._required_data['Loop__Related_Object__c']['rows']:
            return value
        header = self._required_data['Loop__Related_Object__c']['header']
        row = self._required_data['Loop__Related_Object__c']['rows'][value]
        unique_index = header.index('DDP_Migrator_Id__c')
        return row[unique_index]

    def _encode_one_value(self, value, value_row_id):
        # Do not convert non-GUID values
        if '-' not in value:
            return value
        header = self._required_data['Loop__Related_Object__c']['header']
        rows = self._required_data['Loop__Related_Object__c']['rows']
        unique_index = header.index('DDP_Migrator_Id__c')
        for row_id in rows:
            if rows[row_id][unique_index] == value:
                return row_id

        if self._is_retry_failed:
            print("        Missing value: {0}".format(value))
            if value_row_id not in self._retry_row_ids:
                self._retry_row_ids.append(value_row_id)
            return value

        raise Exception("Could not convert value [{0}]".format(value))
