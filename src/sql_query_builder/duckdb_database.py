#   Skadi - A visual modelling tool for constructing and executing directed graphs.
#
#    Copyright (C) 2022-2024 Visual Topology Ltd
#
#     Licensed under the MIT License
#
import datetime

from .sql_query import Database

class DuckDBDatabase(Database):

    def __init__(self,conn):
        self.conn = conn

    def check_schema(self,sql):
        print(sql)
        rs = self.conn.execute("DESCRIBE " + sql)
        schema = []
        for (name, type, _, _, _, _) in rs.fetchall():
            schema.append((name, type.upper()))
        return schema

    def get_sql(self, query):
        return query.get_sql(self)

    def run_query(self, sql, convert_datetimes=False):
        rs = self.conn.sql(sql)
        rs = {"columns": rs.columns, "data": [list(t) for t in rs.fetchall()], "column_types": [t[1].upper() for t in rs.description]}

        if convert_datetimes:
            epoch_time = datetime.datetime(1970, 1, 1)
            for row_idx in range(len(rs["data"])):
                row = rs["data"][row_idx]
                for col_idx in range(len(row)):
                    ctype = rs["column_types"][col_idx]
                    if ctype == "DATE" or ctype == "DATETIME":
                        v = row[col_idx]
                        if isinstance(v,datetime.datetime):
                            row[col_idx] = (v - epoch_time).total_seconds()
                        elif isinstance(v,datetime.date):
                            v = datetime.datetime.combine(v, datetime.time())
                        else:
                            continue
                        row[col_idx] = (v - epoch_time).total_seconds()

        return rs


    def get_types(self):
        return ["VARCHAR","TIMESTAMP","HUGEINT","BIGINT","INTEGER","SMALLINT","TINYINT","BOOLEAN","DOUBLE","FLOAT"]
