""" Handler class for Loop_Security__c field """
from fieldhandlers.field_handler_base import FieldHandlerBase


class LoopSecurityHandler(FieldHandlerBase):
    """ Handler class for Loop_Security__c field """

    _TABLES_REQUIRED_BY_DECODE = {
        'Profile': {
            'id': 'Id',
            'query': 'SELECT Id,Name FROM Profile'
        },
        'PermissionSet': {
            'id': 'Id',
            'query': 'SELECT Id,Name FROM PermissionSet'
        },
        'Group': {
            'id': 'Id',
            'query': "SELECT Id,Name FROM Group WHERE Type = 'Regular'"
        },
        'UserRole': {
            'id': 'Id',
            'query': 'SELECT Id,Name FROM UserRole'
        }
    }

    _TABLES_REQUIRED_BY_ENCODE = {
        'Profile': {
            'id': 'Id',
            'query': 'SELECT Id,Name FROM Profile'
        },
        'PermissionSet': {
            'id': 'Id',
            'query': 'SELECT Id,Name FROM PermissionSet'
        },
        'Group': {
            'id': 'Id',
            'query': "SELECT Id,Name FROM Group WHERE Type = 'Regular'"
        },
        'UserRole': {
            'id': 'Id',
            'query': 'SELECT Id,Name FROM UserRole'
        }
    }

    def _decode_one_value(self, value):
        """ Converts comma-separated list of profile IDs into comma-separated list of profile names """
        header = self._required_data['Profile']['header']
        name_index = header.index('Name')
        security_names = list()
        for quoted_profile_id in value.split(','):
            security_id = quoted_profile_id.replace("'", "")
            if self._is_id_in_list(security_id, 'Profile'):
                table_name = 'Profile'
            elif self._is_id_in_list(security_id, 'PermissionSet'):
                table_name = 'PermissionSet'
            elif self._is_id_in_list(security_id, 'Group'):
                table_name = 'Group'
            elif self._is_id_in_list(security_id, 'UserRole'):
                table_name = 'UserRole'
            else:
                raise Exception(
                    "Could not find {0} id in Profile, PermissionSet, Group and UserRole tables".format(security_id))

            row = self._get_value_by_id(security_id, table_name)
            security_names.append("{0}.{1}".format(table_name, row[name_index]))
        security_names.sort()
        return "\n".join(security_names)

    def _lookup_id_by_name(self, table_name, security_name):
        header = self._required_data[table_name]['header']
        rows = self._required_data[table_name]['rows']
        name_index = header.index('Name')
        for row_id, row in rows.items():
            if security_name == row[name_index]:
                return row_id
        raise Exception("Could not convert value [{0}]".format(security_name))

    def _encode_one_value(self, values, value_row_id):
        security_names = list()
        for value in values.splitlines():
            security_names.append(value.split('.'))

        security_ids = list()
        # Convert each value
        for security_type, security_value in security_names:
            security_ids.append(self._lookup_id_by_name(security_type, security_value))

        return "'" + "','".join(security_ids) + "'"
