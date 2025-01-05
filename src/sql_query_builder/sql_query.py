#   Skadi - A visual modelling tool for constructing and executing directed graphs.
#
#    Copyright (C) 2022-2024 Visual Topology Ltd
#
#     Licensed under the MIT License
#


import copy
import json

class Database:

    def check_schema(self,sql):
        raise NotImplemented("check_schema")

    def get_sql(self, query):
        raise NotImplemented("get_sql")

    def run_query(self, query):
        raise NotImplemented("run_query")

class Expression:

    def get_sql(self):
        pass

    def __repr__(self):
        return self.get_sql()

class ColumnExpression(Expression):

    def __init__(self, column_name, table_alias=None):
        self.column_name = column_name
        self.table_alias = table_alias

    def get_sql(self):
        if self.column_name == "*":
            return "*"
        if self.table_alias:
            return f"{self.table_alias}.{self.column_name}"
        else:
            return self.column_name

class LiteralExpression(Expression):

    def __init__(self, value):
        self.value = value

    def get_sql(self):
        if isinstance(self.value,str):
            return "'"+self.value+"'"
        elif isinstance(self.value,int) or isinstance(self.value,float):
            return str(self.value)
        else:
            return str(self.value)

class ConditionalExpression(Expression):

    def __init__(self, lhs, op, rhs):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def get_sql(self):
        return "( %s %s %s )" % (str(self.lhs), self.op, str(self.rhs))

class BinaryExpression(Expression):

    def __init__(self, lhs, op, rhs):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def get_sql(self):
        return "( %s %s %s )" % (str(self.lhs), self.op, str(self.rhs))

class UnaryExpression(Expression):

    def __init__(self, op, expr):
        self.op = op
        self.expr = expr

    def get_sql(self):
        return "( %s %s )" % (self.op, str(self.expr))

class FunctionExpression(Expression):

    def __init__(self, function_name, *function_arguments):
        self.function_name = function_name
        self.function_arguments = function_arguments

    def get_sql(self):
        s = self.function_name
        s += "("
        for idx in range(len(self.function_arguments)):
            if idx:
                s += ", "
            s += str(self.function_arguments[idx])
        s += ")"
        return s

class CastExpression(Expression):

    def __init__(self, expr, type_name):
        self.expr = expr
        self.type_name = type_name

    def get_sql(self):
        return "TRY_CAST( %s AS %s )" % (str(self.expr), self.type_name)

class Query:

    def get_schema(self, db):
        sql = self.get_sql(db,outer=True)
        return db.check_schema(sql)

    def generate_expression(self,parsed_expr):
        if "operator" in parsed_expr or "function" in parsed_expr:
            arg_e = [];
            for idx in range(len(parsed_expr["args"])):
                arg_e.append(self.generate_expression(parsed_expr["args"][idx]));
            nargs =  len(arg_e)
            if "operator" in parsed_expr:
                if nargs == 1:
                    return UnaryExpression(parsed_expr["operator"],arg_e[0])
                elif nargs == 2:
                    return BinaryExpression(arg_e[0],parsed_expr["operator"], arg_e[1])
            else:
                return FunctionExpression(parsed_expr["function"], *arg_e)
        elif "literal" in parsed_expr:
            return LiteralExpression(parsed_expr["literal"])
        elif "name" in parsed_expr:
            return ColumnExpression(parsed_expr["name"])
        else:
            raise Exception("generate_expression")

    def get_sql(self, db, outer=True, nest=0):
        pass

    def select_columns(self, column_names):
        pass

    def with_aliases(self, new_aliases):
        pass

    def add_where_clause(self, conditional_expression):
        pass

    def add_derived_column(self, new_column_name, expression):
        pass

    def add_sample_rows(self, nr_rows):
        pass

    def summarise(self, key_columns, summary_functions):
        pass

def format_name(name):
    return f'"{name}"'


class JoinTable(Query):

    def __init__(self,query1,query2,join_criteria):
        self.query1 = query1
        self.query2 = query2
        self.join_criteria = join_criteria
        self.where_clauses = []
        for (col1,comparison_op,col2) in self.join_criteria:
            self.where_clauses.append(ConditionalExpression("T1.\"%s\""%col1,comparison_op,"T2.\"%s\""%col2))
        self.drop_query2_columns = set()
        for (col1,op,col2) in self.join_criteria:
            if col1 == col2 and op == "=":
                self.drop_query2_columns.add(col2)

    def select_columns(self, column_names):
        q = BaseQuery(self)
        return q.select_columns(column_names)

    def with_aliases(self, new_aliases):
        return BaseQuery(self, where_clauses=[], column_aliases=new_aliases, include_all_columns=False)

    def add_where_clause(self,conditional_expression):
        if conditional_expression:
            if isinstance(conditional_expression,str):
                conditional_expression = self.generate_expression(json.loads(conditional_expression))
            return BaseQuery(self, where_clauses=[conditional_expression])
        else:
            return self

    def add_derived_column(self, new_column_name, expression):
        if new_column_name and expression:
            if isinstance(expression,str):
                expression = self.generate_expression(json.loads(expression))
            return BaseQuery(self, derived_columns=[(new_column_name,expression)])
        else:
            return self

    def summarise(self, key_columns, summary_functions):
        bq = BaseQuery(self)
        return bq.summarise(key_columns=key_columns,summary_functions=summary_functions)

    def get_sql(self, db, outer=True, nest=0):
        q1_cols = self.query1.get_schema(db)
        q2_cols = self.query2.get_schema(db)
        indent = "    "*nest
        sql = f"\n{indent}SELECT "
        sql += ", ".join([f"T1.{format_name(name)}" for (name,_) in q1_cols])
        sql += ", "
        sql += ", ".join([f"T2.{format_name(name)}" for (name, _) in q2_cols if name not in self.drop_query2_columns])
        sql += f"\n{indent}FROM "
        sql += self.query1.get_sql(db, outer=False, nest=nest+1) + " T1"
        sql += ", "
        sql += self.query2.get_sql(db, outer=False, nest=nest+1) + " T2"
        if len(self.where_clauses):
            sql += f"\n{indent}WHERE "
            sql += " AND ".join(map(lambda wc:wc.get_sql(),self.where_clauses))

        if outer:
            return sql
        else:
            return "( " + sql + ") "

class BaseQuery(Query):

    def __init__(self, query, where_clauses = [], column_aliases={}, derived_columns=[], group_by_columns=[],
                 include_all_columns=True, sample_nr_rows=None, summarised=False):
        self.query = query
        self.where_clauses = where_clauses[:]
        self.column_aliases = copy.deepcopy(column_aliases)
        self.derived_columns = derived_columns[:]
        self.group_by_columns = group_by_columns[:]
        self.include_all_columns = include_all_columns
        self.sample_nr_rows = sample_nr_rows
        self.summarised = summarised

    def __clone(self):
        return BaseQuery(self.query,where_clauses=self.where_clauses,
                         column_aliases=self.column_aliases,
                         derived_columns=self.derived_columns,
                         group_by_columns=self.group_by_columns,
                         include_all_columns=self.include_all_columns,
                         sample_nr_rows=self.sample_nr_rows,
                         summarised=self.summarised)

    def __nest(self):
        return BaseQuery(self)

    def select_columns(self, column_names):
        aliases = []
        for name in column_names:
            aliases.append((ColumnExpression(name),None))
        return self.with_aliases(aliases)

    def with_aliases(self, new_aliases):
        if not new_aliases:
            return self
        if self.derived_columns:
            bq = self.__nest()
        else:
            bq = self.__clone()
        bq.include_all_columns = False
        bq.column_aliases=copy.deepcopy(new_aliases)
        return bq

    def add_derived_column(self, new_column_name, expression):
        if new_column_name and expression:
            bq = self.__clone()
            if isinstance(expression,str):
                expression = self.generate_expression(json.loads(expression))
            bq.derived_columns.append((new_column_name,expression))
            return bq
        else:
            return self

    def add_where_clause(self,conditional_expression):
        if conditional_expression:
            bq = self.__nest() if self.sample_nr_rows is not None or self.summarised else self.__clone()
            if isinstance(conditional_expression,str):
                conditional_expression = self.generate_expression(json.loads(conditional_expression))
            bq.where_clauses.append(conditional_expression)
            return bq
        else:
            return self

    def add_sample_rows(self, nr_rows):
        if self.sample_nr_rows is None:
            q = self.__clone()
            q.sample_nr_rows = nr_rows
        else:
            q = self.__nest()
            q.sample_nr_rows = nr_rows
        return q

    def summarise(self, key_columns, summary_functions, nest=True):
        bq = self.__nest() if nest else self.__clone()
        bq.group_by_columns = key_columns[:]
        for (op,col,output_name) in summary_functions:
            bq.derived_columns.append((output_name,"%s(%s)"%(op,col)))
        bq.column_aliases = [(ColumnExpression(name),name) for name in key_columns]
        bq.include_all_columns = False
        bq.summarised = True
        return bq

    def get_sql(self, db, outer=True, nest=0):

        indent = "    "*nest
        sql = f"\n{indent}SELECT "

        columns = []

        if self.include_all_columns:
            columns.append("*")

        if self.column_aliases:
            for (colref,newname) in self.column_aliases:
                if newname is not None:
                    columns.append(f"{colref} AS {format_name(newname)}")
                else:
                    columns.append(f"{colref}")

        if self.derived_columns:
            for (name, expression) in self.derived_columns:
                columns.append("%s AS %s" % (expression, format_name(name)))

        sql += ", ".join(columns)

        sql += f"\n{indent}FROM "
        sql += self.query.get_sql(db, outer=False, nest=nest+1)
        if self.where_clauses:
            sql += f"\n{indent}WHERE "
            sql += " AND ".join(map(lambda wc:wc.get_sql(),self.where_clauses))
        if not outer:
            sql = "( " + sql + " )"
        if self.group_by_columns:
            group_bys = []
            for name in self.group_by_columns:
                group_bys.append("\"%s\"" % (name))
            sql += f"\n{indent}GROUP BY "
            sql += ", ".join(group_bys)
        if self.sample_nr_rows is not None:
            sql += f"\n{indent}USING SAMPLE {self.sample_nr_rows} ROWS"
        print(sql)
        return sql

class BaseTable(Query):

    def __init__(self, table_name):
        self.table_name = table_name

    def select_columns(self, column_names):
        bq = BaseQuery(self)
        bq = bq.select_columns(column_names)
        return bq

    def with_aliases(self, new_aliases):
        return BaseQuery(self, where_clauses=[], column_aliases = new_aliases, include_all_columns=False)

    def add_where_clause(self, conditional_expression):
        if conditional_expression:
            if isinstance(conditional_expression,str):
                conditional_expression = self.generate_expression(json.loads(conditional_expression))
            return BaseQuery(self, where_clauses=[conditional_expression], column_aliases={})
        else:
            return self

    def add_derived_column(self, new_column_name, expression):
        if new_column_name and expression:
            if isinstance(expression,str):
                expression = self.generate_expression(json.loads(expression))
            return BaseQuery(self, derived_columns=[(new_column_name,expression)])
        else:
            return self

    def add_sample_rows(self, nr_rows):
        return BaseQuery(self,sample_nr_rows=nr_rows)

    def summarise(self, key_columns, summary_functions):
        bq = BaseQuery(self)
        return bq.summarise(key_columns=key_columns, summary_functions=summary_functions,nest=False)

    def get_sql(self, db, outer=True, nest=0):
        if outer:
            indent = "    "*nest
            return f"{indent}SELECT * FROM %s" % self.table_name
        else:
            return self.table_name


