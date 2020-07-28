from collections import defaultdict
from typing import Any, Dict, List, Type
import db_api
import os
import shelve


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
    key_field_name: str

    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

    def count(self) -> int:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            return len(db)

    def insert_record(self, values: Dict[str, Any]) -> None:
        try:
            for field in self.fields:
                if field.name not in values.keys():
                    raise ValueError("invalid field")
        except ValueError as exp:
            print(exp.args)
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            if db.get(str(values[self.key_field_name])):
                raise ValueError
            db[str(values[self.key_field_name])] = values

    def delete_record(self, key: Any) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            if not db.get(str(key)):
                raise ValueError
            del db[str(key)]

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        flag = 0
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            for key, value in db.items():
                for item in criteria:
                    if item.operator == '=':
                        item.operator = '=='
                    if not value.get(item.field_name):
                        raise NameError
                    if not eval(str(value[item.field_name]) + item.operator + str(item.value)):
                        flag = 1
                        break
                if not flag:
                    del db[key]
                flag = 0

    def get_record(self, key: Any) -> Dict[str, Any]:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            if not db.get(str(key)):
                raise ValueError
            return db[str(key)]

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=True) as db:
            db[str(key)] = values

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        query_list = []
        flag = 0
        with shelve.open(os.path.join(db_api.DB_ROOT, self.name), writeback=False) as db:
            for key, value in db.items():
                for item in criteria:
                    if item.operator == '=':
                        item.operator = '=='
                    if not value.get(item.field_name):
                        raise NameError
                    if not eval(str(value[item.field_name]) + item.operator + str(item.value)):
                        flag = 1
                        break
                if not flag:
                    query_list.append(value)
                flag = 0
        return query_list

    def create_index(self, field_to_index: str) -> None:
        pass

class DataBase(db_api.DataBase):

    tables: Dict[str, DBTable]

    def __init__(self):
        self.tables = defaultdict(DBTable)

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        db = shelve.open(os.path.join(db_api.DB_ROOT, table_name))
        db.close()
        table = DBTable(table_name, fields, key_field_name)
        if not self.tables.get(table_name):
            raise ValueError
        self.tables[table_name] = table
        return table
    def num_tables(self) -> int:
        return len(self.tables)

    def get_table(self, table_name: str) -> DBTable:
        if not self.tables.get(table_name):
            raise ValueError
        return self.tables[table_name]



    def delete_table(self, table_name: str) -> None:
        db = (os.path.join('db_files', table_name + ".db.bak"))
        os.remove(db)
        db = (os.path.join('db_files', table_name + ".db.dat"))
        os.remove(db)
        db = (os.path.join('db_files', table_name + ".db.dir"))
        os.remove(db)
        del self.tables[table_name]

    def get_tables_names(self) -> List[Any]:
        return list(self.tables.keys())


    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        pass
