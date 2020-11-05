from collections import defaultdict
from typing import Any, Dict, List, Type
from src import db_api
import os
import shelve


def set_compare_operator(item):
    if item.operator == '=':
        item.operator = '=='

    return item


class DBField(db_api.DBField):
    name: str
    type: Type

    def __init__(self, name, type_):
        self.name = name
        self.type = type_


class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any

    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value


class DBTable(db_api.DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str
    indexes: Dict[str, str]

    def __init__(self, name, fields, key_field_name, indexes=None):
        if indexes is None:
            indexes = defaultdict()

        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        self.indexes = indexes

    def delete_key_from_index(self, db, key, field) -> None:
        path = os.path.join(db_api.DB_ROOT, self.indexes[field])
        with shelve.open(path, writeback=True) as index:
            ids_of_value = index[str(db[str(key)][field])]

            if len(ids_of_value) > 1:
                ids_of_value.remove(key)

            else:
                del index[str(db[str(key)][field])]

    @staticmethod
    def is_meets_criterion(criterion, field_value) -> bool:
        criterion = set_compare_operator(criterion)

        if isinstance(field_value, str):
            field_value = "'" + field_value + "'"
            item_value = "'" + criterion.value + "'"

        else:
            item_value = criterion.value

        if not eval(str(field_value) + criterion.operator + str(item_value)):
            return False

        return True

    def is_meets_criteria(self, criteria: List[SelectionCriteria], value: Any) -> bool:
        for criterion in criteria:
            if not value.get(criterion.field_name):
                raise NameError

            if not self.is_meets_criterion(criterion, value[criterion.field_name]):
                return False

        return True

    def insert_to_index(self, field, values) -> None:
        if self.indexes.get(field):
            path = os.path.join(db_api.DB_ROOT, self.indexes[field])
            with shelve.open(path, writeback=True) as index:
                if not index.get(str(values[field])):
                    index[str(values[field])] = list(values[self.key_field_name])

                else:
                    index[str(values[field])].append(values[self.key_field_name])

    def count(self) -> int:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            return len(db)

    def insert_record(self, values: Dict[str, Any]) -> None:
        try:
            for field in self.fields:
                if field.name not in values.keys():
                    raise ValueError("invalid fields")

        except ValueError as exp:
            print(exp.args)

        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            if db.get(str(values[self.key_field_name])):
                raise ValueError

            db[str(values[self.key_field_name])] = values

        for field in values.keys():
            self.insert_to_index(field, values)

    def delete_also_from_index(self, db, key) -> None:
        key_str = str(key)

        for field in db[key_str].keys():
            if self.indexes.get(field):
                self.delete_key_from_index(db, key, field)

        del db[key_str]

    def delete_record(self, key: Any) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:

            if not db.get(str(key)):
                raise ValueError

            self.delete_also_from_index(db, key)

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            for criterion in criteria:
                if self.key_field_name == criterion.field_name and criterion.operator == '=':
                    return self.del_record_by_key_index(criteria, criterion, db)

            for criterion in criteria:
                if self.indexes.get(criterion.field_name) and criterion.operator == '=':
                    return self.del_record_by_index(criteria, criterion, db)

            self.del_record_without_index(criteria, db)

    def del_record_without_index(self, criteria, db):
        for record in db.values():
            if self.is_meets_criteria(criteria, record):
                self.delete_also_from_index(db, record[self.key_field_name])

    def del_record_by_index(self, criteria, criterion, db):
        path = os.path.join(db_api.DB_ROOT, self.indexes[criterion.field_name])
        with shelve.open(path, writeback=True) as index:
            list_keys_match = index.get(str(criterion.value))

            if not list_keys_match:
                return

            for key in list_keys_match:
                if self.is_meets_criteria(criteria, db[str(key)]):
                    self.delete_also_from_index(db, key)

    def del_record_by_key_index(self, criteria, criterion, db):
        record = db.get(str(criterion.value))
        if not record:
            return

        if self.is_meets_criteria(criteria, record):
            self.delete_also_from_index(db, record[self.key_field_name])

    def get_record(self, key: Any) -> Dict[str, Any]:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            if not db.get(str(key)):
                raise ValueError

            return db[str(key)]

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:

        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            key_str = str(key)
            if db.get(key_str):
                for field in db[key_str].keys():
                    if not values.get(field):
                        continue

                    if db[key_str][field] != values[field]:
                        if self.indexes.get(field):
                            self.delete_key_from_index(db, key, field)

                values[self.key_field_name] = key
                for field in values.keys():
                    self.insert_to_index(field, values)

                db[key_str].update(values)

    def get_record_without_index(self, criteria, db) -> List[Dict[str, Any]]:
        query_list = []

        for record in db.values():
            if self.is_meets_criteria(criteria, record):
                query_list.append(record)

        return query_list

    def get_record_by_index(self, criteria, criterion, db) -> List[Dict[str, Any]]:
        query_list = []
        path = os.path.join(db_api.DB_ROOT, self.indexes[criterion.field_name])
        with shelve.open(path, writeback=True) as index:
            list_keys_match = index.get(str(criterion.value))

            if not list_keys_match:
                return query_list

            for key in list_keys_match:
                if self.is_meets_criteria(criteria, db[str(key)]):
                    query_list.append(db[str(key)])
        return query_list

    def get_record_by_key_index(self, criteria, criterion, db) -> List[Dict[str, Any]]:
        query_list = []
        record = db.get(str(criterion.value))
        if not record:
            return query_list

        if self.is_meets_criteria(criteria, record):
            query_list.append(record)

        return query_list

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            for criterion in criteria:
                if self.key_field_name == criterion.field_name and criterion.operator == '=':
                    return self.get_record_by_key_index(criteria, criterion, db)

            for criterion in criteria:
                if self.indexes.get(criterion.field_name) and criterion.operator == '=':
                    return self.get_record_by_index(criteria, criterion, db)

            return self.get_record_without_index(criteria, db)

    def create_index(self, field_to_index: str) -> None:
        self.indexes[field_to_index] = field_to_index + "_" + self.name + '.db'

        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            with shelve.open(os.path.join(db_api.DB_ROOT, self.indexes[field_to_index])) as index:
                for record in db.values():
                    key_index = record.get(field_to_index, None)

                    if key_index is None:
                        continue

                    key_index = str(key_index)

                    if key_index in index.keys():
                        index[key_index] += [record[self.key_field_name]]

                    else:
                        index[key_index] = [record[self.key_field_name]]

        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            db[self.name][2] = self.indexes


class DataBase(db_api.DataBase):
    tables: Dict[str, DBTable]
    __tables__ = defaultdict(DBTable)

    def __init__(self):
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            for key in db:
                DataBase.__tables__[key] = DBTable(key, db[key][0], db[key][1], db[key][2])

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        db = shelve.open(os.path.join(db_api.DB_ROOT, table_name))
        db.close()
        if DataBase.__tables__.get(table_name):
            raise ValueError
        flag = 0

        for field_name in fields:
            if key_field_name == field_name.name:
                flag = 1
                break

        if not flag:
            raise ValueError

        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            db[table_name] = [fields, key_field_name, {}]

        table = DBTable(table_name, fields, key_field_name)
        DataBase.__tables__[table_name] = table

        return table

    def num_tables(self) -> int:
        return len(DataBase.__tables__)

    def get_table(self, table_name: str) -> DBTable:
        if not DataBase.__tables__.get(table_name):
            raise ValueError
        return DataBase.__tables__[table_name]

    @staticmethod
    def delete_shelve_file(table_name):
        db = (os.path.join('db_files', table_name + ".bak"))
        os.remove(db)
        db = (os.path.join('db_files', table_name + ".dat"))
        os.remove(db)
        db = (os.path.join('db_files', table_name + ".dir"))
        os.remove(db)

    def delete_table(self, table_name: str) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            indexes = db[table_name][2]
            for value in indexes.values():
                self.delete_shelve_file(value)

            del db[table_name]
        self.delete_shelve_file(table_name)
        del DataBase.__tables__[table_name]

    def get_tables_names(self) -> List[Any]:
        return list(DataBase.__tables__.keys())

