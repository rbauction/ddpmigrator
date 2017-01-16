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
        security_names = []
        for quoted_profile_id in value.split(','):
            security_id = quoted_profile_id.replace("'", "")
            if security_id in self._required_data['Profile']['rows']:
                table_name = 'Profile'
            elif security_id in self._required_data['PermissionSet']['rows']:
                table_name = 'PermissionSet'
            elif security_id in self._required_data['Group']['rows']:
                table_name = 'Group'
            elif security_id in self._required_data['UserRole']['rows']:
                table_name = 'UserRole'
            else:
                raise Exception(
                    "Could not find {0} id in Profile, PermissionSet, Group and UserRole tables".format(security_id))

            row = self._required_data[table_name]['rows'][security_id]
            security_names.append("{0}.{1}".format(table_name, row[name_index]))
        security_names.sort()
        return "\n".join(security_names)

    def _lookup_id_by_name(self, table_name, security_name):
        header = self._required_data[table_name]['header']
        rows = self._required_data[table_name]['rows']
        name_index = header.index('Name')
        for row_id in rows:
            if security_name == rows[row_id][name_index]:
                return row_id
        raise Exception("Could not convert value [{0}]".format(security_name))

    def _encode_one_value(self, values, value_row_id):
        security_names = []
        for value in values.splitlines():
            security_names.append(value.split('.'))

        security_ids = []
        # Convert each value
        for security_type, security_value in security_names:
            security_ids.append(self._lookup_id_by_name(security_type, security_value))

        return "'" + "','".join(security_ids) + "'"
