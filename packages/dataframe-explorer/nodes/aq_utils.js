/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.AqUtils = class {

    constructor(table) {
        this.table = table;
        this.operator_map = {
            "and": "&&",
            "or": "||"
        }
    }

    preprocess_expression(expr) {
        let parser = new skadi.ExpressionParser();
        parser.add_binary_operator("and",4);
        parser.add_binary_operator("or",4);
        parser.add_binary_operator("+",4);
        parser.add_binary_operator("-",4);
        parser.add_binary_operator("*",5);
        parser.add_binary_operator("/",5);
        parser.add_binary_operator("<",6);
        parser.add_binary_operator(">",6);
        parser.add_binary_operator("<=",6);
        parser.add_binary_operator(">=",6);
        parser.add_binary_operator("==",6);
        let tree = parser.parse(expr);
        if (tree.error) {
            throw new Error(tree.error + " at position "+tree.error_pos);
        }
        return this.apply_tree(tree);
    }

    apply_tree(tree) {

        if (tree.literal !== undefined) {
            return JSON.stringify(tree.literal);
        }
        if (tree.name) {
            return "d."+tree.name;
        }

        if (tree.function) {
            let s = tree.function+"(";
            for(let i=0; i<tree.args.length; i++) {
                if (i>0) {
                    s += ",";
                }
                s += this.apply_tree(tree.args[i])
            }
            s += ")";
            return s;
        }
        if (tree.operator) {
            let s = "(";
            let index = 0;
            if (tree.args.length == 2) {
                s += this.apply_tree(tree.args[index]);
                index += 1;
            }
            let op = tree.operator;
            if (op in this.operator_map) {
                op = this.operator_map[op];
            }
            s += " " + op + " ";
            s += this.apply_tree(tree.args[index]);
            s += ")";
            return s;
        }
    }

    get_column_type(column_name) {
        let all_values = this.table.values(column_name);
        for (const value of all_values) {
            if (typeof value === 'number') {
                return "number";
            } else if (typeof value === 'string') {
                return "string";
            } else if (value instanceof Date) {
                return "date";
            }
        }
        return "unknown";
    }

    analyse(column_name, value_count_limit) {
        let type = this.get_column_type(column_name);
        let all_values = this.table.values(column_name);
        if (type === "string") {
            let uncounted = 0;
            let value_counts = {};
            for (const value of all_values) {
                if (typeof value === "string" && !(value in value_counts)) {
                    if (value_count_limit === undefined || value_count_limit > 0) {
                        value_counts[value] = 1;
                        if (value_count_limit) {
                            value_count_limit -= 1;
                        }
                    } else {
                        uncounted += 1;
                    }
                } else {
                    value_counts[value] = value_counts[value]+1;
                }
            }
            return {
                "value_counts": value_counts,
                "uncounted": uncounted
            }
        } else if (type === "number") {
            let numeric_min = null;
            let numeric_max = null;
            for (const value of all_values) {
                if (typeof value === 'number') {
                    if (numeric_min == null || value < numeric_min) {
                        numeric_min = value;
                    }
                    if (numeric_max == null || value > numeric_max) {
                        numeric_max = value;
                    }
                }
            }
            return {
                "range": {
                    "min": numeric_min,
                    "max": numeric_max
                }
            }
        } else if (type === "date") {
            let date_min = null;
            let date_max = null;
            for (const value of all_values) {
                if (value instanceof Date) {
                    if (numeric_date == null || value < date_min) {
                        date_min = value;
                    }
                    if (numeric_date == null || value > date_max) {
                        date_max = value;
                    }
                }
            }
            return {
                "range": {
                    "min": date_min.toISOString(),
                    "max": date_max.toISOString()
                }
            }
        }
    }
}

function parse_custom_date(s, fmt) {
    try {
        let year = null;
        let month = 1;
        let day = 1;
        for(let idx=0; idx < fmt.length; idx++) {
            if (fmt.slice(idx,idx+4) == 'YYYY') {
                year = Number.parseInt(s.slice(idx,idx+4));
                idx += 4;
            } else if (fmt.slice(idx,idx+2) == 'MM') {
                month = Number.parseInt(s.slice(idx,idx+2));
                idx += 2;
            } else if (fmt.slice(idx,idx+2) == 'DD') {
                day = Number.parseInt(s.slice(idx,idx+2));
                idx += 1;
            } else if (fmt.charAt(idx) === s.charAt(idx)) {
                idx += 1;
            } else {
                return null;
            }
        }
        if (year && month && day) {
            return new Date(year,month-1,day);
        }
    } catch(ex) {
        return null;
    }
}

aq.addFunction("parse_date_custom",(s,fmt) => parse_custom_date(s,fmt));
