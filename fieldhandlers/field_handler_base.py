""" Base field handler class """


class FieldHandlerBase:
    """ Base field handler class """

    # Hash-tables in 'Table name': 'query' format
    _TABLES_REQUIRED_BY_DECODE = {}
    _TABLES_REQUIRED_BY_ENCODE = {}

    def __init__(self, table_data, field_name, is_retry_failed=False):
        self._table_data = table_data
        self._field_name = field_name
        self._required_data = None
        self._is_retry_failed = is_retry_failed
        self._retry_row_ids = list()

    def _decode_one_value(self, value):
        raise NotImplementedError("_decode_one_value method has not been overridden")

    def decode(self):
        """ Converts sandbox specific data into sandbox-independent data (export) """
        rows = self._table_data['rows']
        if len(rows) == 0:
            return
        header = self._table_data['header']
        id_index = header.index(self._field_name)
        for row_id in rows:
            row = rows[row_id]
            decoded_value = self._decode_one_value(row[id_index])
            row[id_index] = decoded_value

    def _encode_one_value(self, value, value_row_id):
        raise NotImplementedError("_encode_one_value method has not been overridden")

    def encode(self):
        """ Converts sandbox-independent data into sandbox specific data (import) """
        rows = self._table_data['rows']
        self._retry_row_ids = list()
        header = self._table_data['header']
        id_index = header.index(self._field_name)
        for row_id in rows:
            row = rows[row_id]
            encoded_value = self._encode_one_value(row[id_index], row_id)
            # Tuple can't be updated so create a list, update it and then convert it to tuple
            row_list = list(row)
            row_list[id_index] = encoded_value
            rows[row_id] = tuple(row_list)

    def tables_required_by_decode(self):
        return self._TABLES_REQUIRED_BY_DECODE

    def tables_required_by_encode(self):
        return self._TABLES_REQUIRED_BY_ENCODE

    def set_required_data(self, required_data):
        self._required_data = required_data

    def get_encoded_rows(self):
        rows = self._table_data['rows']
        if not self._is_retry_failed:
            return rows
        encoded_rows = dict()
        for row_id in rows:
            if row_id not in self._retry_row_ids:
                encoded_rows[row_id] = rows[row_id]
        return encoded_rows

    def should_retry(self):
        if self._is_retry_failed and len(self._retry_row_ids) > 0:
            return True
        return False
