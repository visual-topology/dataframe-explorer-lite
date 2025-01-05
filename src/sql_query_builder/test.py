import duckdb

con = duckdb.connect(":memory:")
con.execute("CREATE TABLE tbl AS SELECT 42 a")
rs = con.sql("SELECT * FROM tbl")
print(rs)
con.execute("CREATE VIEW v AS SELECT 42 a")
rs = con.sql("DESCRIBE v;")
print(rs)