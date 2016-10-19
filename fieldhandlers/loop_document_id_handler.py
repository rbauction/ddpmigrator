""" Handler class for Loop__Document_Id__c field """
from fieldhandlers.field_handler_base import FieldHandlerBase


class LoopDocumentIdHandler(FieldHandlerBase):
    """ Handler class for Loop__Document_Id__c field """

    _TABLES_REQUIRED_BY_DECODE = {
        'Document': {
            'id': 'Id',
            'query': 'SELECT Id,DeveloperName,FolderId FROM Document'
        },
        'Folder': {
            'id': 'Id',
            'query': 'SELECT Id,NamespacePrefix,DeveloperName FROM Folder WHERE Type=\'Document\''
        }
    }

    _TABLES_REQUIRED_BY_ENCODE = {
        'Document': {
            'id': 'Id',
            'query': 'SELECT Id,DeveloperName,FolderId FROM Document'
        },
        'Folder': {
            'id': 'Id',
            'query': 'SELECT Id,NamespacePrefix,DeveloperName FROM Folder WHERE Type=\'Document\''
        }
    }

    def _decode_one_value(self, value):
        # Document table
        doc_header = self._required_data['Document']['header']
        doc_row = self._required_data['Document']['rows'][value]
        doc_dev_name_index = doc_header.index('DeveloperName')
        doc_folder_id_index = doc_header.index('FolderId')
        folder_id = doc_row[doc_folder_id_index]
        # Folder table
        fol_header = self._required_data['Folder']['header']
        fol_row = self._required_data['Folder']['rows'][folder_id]
        fol_namespace_index = fol_header.index('NamespacePrefix')
        fol_dev_name_index = fol_header.index('DeveloperName')
        fol_namespace = fol_row[fol_namespace_index]

        if fol_namespace == "":
            folder_name = fol_row[fol_dev_name_index]
        else:
            folder_name = "{0}__{1}".format(fol_namespace, fol_row[fol_dev_name_index])

        return "{0}/{1}".format(folder_name, doc_row[doc_dev_name_index])

    def _lookup_folder_id_by_name(self, folder_name):
        if '__' in folder_name:
            namespace, dev_name = folder_name.split('__')
        else:
            namespace, dev_name = '', folder_name

        # Folder table
        header = self._required_data['Folder']['header']
        rows = self._required_data['Folder']['rows']
        namespace_index = header.index('NamespacePrefix')
        dev_name_index = header.index('DeveloperName')

        for row_id in rows:
            row = rows[row_id]
            if row[namespace_index] == namespace and row[dev_name_index] == dev_name:
                return row_id

        raise Exception("Could not find folder [{0}]".format(folder_name))

    def _lookup_document_id_by_name(self, folder_id, dev_name):
        header = self._required_data['Document']['header']
        rows = self._required_data['Document']['rows']
        folder_id_index = header.index('FolderId')
        dev_name_index = header.index('DeveloperName')

        for row_id in rows:
            row = rows[row_id]
            if row[folder_id_index] == folder_id and row[dev_name_index] == dev_name:
                return row_id

        raise Exception("Could not find document [{0}] in folder [{1}]".format(dev_name, folder_id))

    def _encode_one_value(self, value, value_row_id):

        folder_name, doc_dev_name = value.split('/')
        folder_id = self._lookup_folder_id_by_name(folder_name)
        return self._lookup_document_id_by_name(folder_id, doc_dev_name)
