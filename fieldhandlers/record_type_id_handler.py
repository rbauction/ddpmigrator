""" Handler class for RecordTypeId field """
from fieldhandlers.field_handler_base import FieldHandlerBase


class RecordTypeIdHandler(FieldHandlerBase):
    """ Handler class for RecordTypeId field """

    _TABLES_REQUIRED_BY_DECODE = {
        'RecordType': {
            'id': 'Id',
            'query': 'SELECT Id,NamespacePrefix,SobjectType,DeveloperName FROM RecordType'
        }
    }

    _TABLES_REQUIRED_BY_ENCODE = {
        'RecordType': {
            'id': 'Id',
            'query': 'SELECT Id,NamespacePrefix,SobjectType,DeveloperName FROM RecordType'
        }
    }

    def _decode_one_value(self, value):
        header = self._required_data['RecordType']['header']
        row = self._required_data['RecordType']['rows'][value]
        namespace_index = header.index('NamespacePrefix')
        s_obj_type_index = header.index('SobjectType')
        dev_name_index = header.index('DeveloperName')
        return "{0}.{1}.{2}".format(row[namespace_index], row[s_obj_type_index], row[dev_name_index])

    def _encode_one_value(self, value, value_row_id):
        header = self._required_data['RecordType']['header']
        rows = self._required_data['RecordType']['rows']
        namespace_index = header.index('NamespacePrefix')
        s_obj_type_index = header.index('SobjectType')
        dev_name_index = header.index('DeveloperName')
        namespace, s_obj_type, dev_name = value.split('.')
        for row_id in rows:
            row = rows[row_id]
            if namespace == row[namespace_index] \
               and s_obj_type == row[s_obj_type_index] \
               and dev_name == row[dev_name_index]:
                return row_id

        raise Exception("Could not convert value [{0}]".format(value))
