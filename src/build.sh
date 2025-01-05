#!/bin/bash

cd `dirname $0`
zip sql_query_builder.zip sql_query_builder/sql_query.py sql_query_builder/duckdb_database.py sql_query_builder/__init__.py
mv sql_query_builder.zip ../packages/dataframe-explorer

