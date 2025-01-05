#   Skadi - A visual modelling tool for constructing and executing directed graphs.
#
#    Copyright (C) 2022-2024 Visual Topology Ltd
#
#     Licensed under the MIT License
#

import unittest

from .sql_query import BaseTable, JoinTable, ConditionalExpression, BinaryExpression, ColumnExpression, LiteralExpression
import os
import duckdb
from .duckdb_database import DuckDBDatabase as DuckDB

class SimpleTest(unittest.TestCase):

    def setUp(self):

        folder = os.path.split(__file__)[0]
        if folder:
            folder += "/"
        self.con = duckdb.connect(":memory:")
        csv_path = f'{folder}../../data/iris.csv'
        rel = self.con.read_csv(csv_path)
        rel.create("iris")
        print(rel.alias)

    def test_runSql(self):
        db = DuckDB(self.con)
        rs = db.run_query("SELECT * FROM iris")
        print(rs)

    def testDeriveColumns(self):

        db = DuckDB(self.con)

        bt = BaseTable("iris")
        print(bt.get_sql(db))
        print(bt.get_schema(db))
        # print(bt.execute(con))
        bq = bt.add_where_clause(ConditionalExpression("id", ">", "100"))
        print(bt.get_schema(db))
        print(bq.get_sql(db))

        # bq = bq.with_aliases({"id": "id_plus"})
        bq = bq.add_derived_column("id plus", BinaryExpression(ColumnExpression("id"),"+",LiteralExpression(100)))
        print(bq.get_sql(db))
        print(bq.get_schema(db))
        print(db.run_query(bq.get_sql(db)))

    def testSampleRows(self):
        db = DuckDB(self.con)
        bt = BaseTable("iris")
        bq = bt.add_sample_rows(10)
        print(bq.get_sql(db))
        print(db.run_query(bq.get_sql(db)))

    def testSelectColumns(self):
        db = DuckDB(self.con)
        bt = BaseTable("iris")
        bq = bt.select_columns(["SepalWidthCm","SepalLengthCm"])
        print(bq.get_sql(db))
        print(db.run_query(bq.get_sql(db)))

    def testJoin(self):
        db = DuckDB(self.con)
        bt2 = BaseTable("iris")
        bt3 = BaseTable("iris")
        jt = JoinTable(bt2, bt3, [["id", "=", "id"]])
        print(jt.get_sql(db))
        print(db.run_query(jt.get_sql(db)))

    def testAggregate(self):
        db = DuckDB(self.con)
        bt4 = BaseTable("iris")
        bt4 = bt4.summarise(["Species"],[
            ("COUNT", ColumnExpression("*"), "COUNT"),
            ("MEAN", ColumnExpression("SepalLengthCm"), "MeanSepalLengthCm"),
            ("SUM", ColumnExpression("SepalLengthCm"), "SumSepalLengthCm")
        ])
        print(bt4.get_sql(db))
        print(db.run_query(bt4.get_sql(db)))

    def testAlias(self):
        db = DuckDB(self.con)
        bt5 = BaseTable("iris")
        bt5 = bt5.with_aliases([(ColumnExpression("id"),"new_id")])
        print(bt5.get_sql(db))
        print(db.run_query(bt5.get_sql(db)))
