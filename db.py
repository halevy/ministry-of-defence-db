from collections import defaultdict
from typing import Any, Dict, List, Type
import db_api
import os
import shelve


def set_compare_operator(item):
    if item.operator == '=':
        item.operator = '=='

    return item


class DBField(db_api.DBField):
    name: str
    type: Type

    def __init__(self, name, type):
        self.name = name
        self.type = type


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
    indexes: Dict[str, str]

    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        self.indexes = defaultdict()

    def delete_key_from_index(self, db, key, field) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.indexes[field]), writeback=True) as index:
            ids_of_value = index[str(db[str(key)][field])]

            if len(ids_of_value) > 1:
                ids_of_value.remove(key)

            else:
                del index[str(db[str(key)][field])]

    def delete_from_index(self, db, key) -> None:
        key_str = str(key)

        for field in db[key_str].keys():
            if self.indexes.get(field):
                self.delete_key_from_index(db, key, field)

        del db[key_str]

    def is_meets_criterion(self, criteria: List[SelectionCriteria], key: Any, value: Any) -> bool:
        for item in criteria:
            item = set_compare_operator(item)

            if item.field_name == self.key_field_name:
                field_value = key
                item_value = item.value

            elif not value.get(item.field_name):
                raise NameError

            elif isinstance(value[item.field_name], str):
                field_value = "'" + value[item.field_name] + "'"
                item_value = "'" + item.value + "'"

            else:
                field_value = value[item.field_name]
                item_value = item.value

            if not eval(str(field_value) + item.operator + str(item_value)):
                return True

        return False

    def insert_to_index(self, field, values) -> None:
        if self.indexes.get(field):
            with shelve.open(os.path.join(db_api.DB_ROOT, self.indexes[field]), writeback=True) as index:
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

    def delete_record(self, key: Any) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            key_str = str(key)

            if not db.get(key_str):
                raise ValueError

            self.delete_from_index(db, key)

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            for key, value in db.items():

                if not self.is_meets_criterion(criteria, key, value):
                    self.delete_from_index(db, key)

    def get_record(self, key: Any) -> Dict[str, Any]:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            if not db.get(str(key)):
                raise ValueError

            return db[str(key)]

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            key = str(key)

            if db.get(key):
                for field in db[key].keys():
                    if (field not in values.keys() or db[key][field] != values[field]) and self.indexes.get(field):
                        self.delete_key_from_index(db, key, field)

                for field in values.keys():
                    if field not in db[key].keys():
                        self.insert_to_index(field, values)

                db[key] = values

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        query_list = []

        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            for key, value in db.items():
                if not self.is_meets_criterion(criteria, key, value):
                    query_list.append(value)

        return query_list

    def create_index(self, field_to_index: str) -> None:
        db = shelve.open(os.path.join(db_api.DB_ROOT, field_to_index + '.db'))
        db.close()
        self.indexes[field_to_index] = field_to_index + "_" + self.name + '.db'


class DataBase(db_api.DataBase):
    tables: Dict[str, DBTable]
    __tables__ = defaultdict(DBTable)

    def __init__(self):
        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            for key in db:
                DataBase.__tables__[key] = DBTable(key, db[key][0], db[key][1])

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
            db[table_name] = [fields, key_field_name]
        table = DBTable(table_name, fields, key_field_name)
        DataBase.__tables__[table_name] = table
        return table

    def num_tables(self) -> int:
        return len(DataBase.__tables__)

    def get_table(self, table_name: str) -> DBTable:
        if not DataBase.__tables__.get(table_name):
            raise ValueError
        return DataBase.__tables__[table_name]

    def delete_table(self, table_name: str) -> None:
        db = (os.path.join('db_files', table_name + ".bak"))
        os.remove(db)
        db = (os.path.join('db_files', table_name + ".dat"))
        os.remove(db)
        db = (os.path.join('db_files', table_name + ".dir"))
        os.remove(db)

        with shelve.open(os.path.join(db_api.DB_ROOT, "DB.db"), writeback=True) as db:
            del db[table_name]

        del DataBase.__tables__[table_name]

    def get_tables_names(self) -> List[Any]:
        return list(DataBase.__tables__.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        pass






