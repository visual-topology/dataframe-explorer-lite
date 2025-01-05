/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.CreateColumnNode = class {

    constructor(node_service) {
        this.node_service = node_service;
        this.parser_error = "";
        this.parser_error_pos = 0;
        this.sql_expr = "";
        this.ep = new skadi.ExpressionParser();
    }

    async load() {
        let url = this.node_service.resolve_resource("nodes/expression_parser.json");
        let r = await fetch(url);
        let o = await r.json();
        if (o["binary_operators"]) {
            for (let op in o["binary_operators"]) {
                let precedence = o["binary_operators"][op];
                this.ep.add_binary_operator(op, precedence);
            }
        }

        if (o["unary_operators"]) {
            for (let op in o["unary_operators"]) {
                this.ep.add_unary_operator(op);
            }
        }

        if (this.column_expression) {
            this.parse_expression(this.column_expression);
        }

        this.update_status();
    }

    get column_name() { return this.node_service.get_property("column_name",""); }
    set column_name(v) { this.node_service.set_property("column_name",v); }

    get column_expression() { return this.node_service.get_property("column_expression",""); }
    set column_expression(v) { this.node_service.set_property("column_expression",v); }

    update_status() {
        if (this.column_name !== "" && this.column_expression !== "") {
            if (this.parser_error) {
                this.node_service.set_status_error(this.parser_error);
            } else {
                this.node_service.set_status_info("" + this.column_name);
            }
        } else {
            this.node_service.set_status_warning("Configure Settings");
        }
    }

    open_client(page_id, client_options, client_service) {
        client_service.set_attributes("column_name",{"value":this.column_name});

        client_service.set_attributes("column_expression",{"value":this.column_expression});

        client_service.add_event_handler("column_name","change", v => {
            this.column_name = v;
            this.update_status();
            this.node_service.request_run();
        });

        client_service.add_event_handler("column_expression","change", v => {
            this.column_expression = v;
            this.parse_expression(this.column_expression);
            this.update_status();
            this.node_service.request_run();
        });
    }

    parse_expression(expr_s) {
        this.parser_error = "";
        this.parser_error_pos = 0;
        this.sql_expr = "";
        let o = this.ep.parse(expr_s);
        if (o.error) {
            this.parser_error = o.error;
            this.parser_error_pos = o.error_pos;
        } else {
            this.sql_expr = o;
        }
        console.log(JSON.stringify(o));
    }

    async run(inputs) {
        if (inputs["data_in"] && this.column_name && this.column_expression && this.sql_expr) {
            let input_query = inputs["data_in"][0];
            let pyodide_config = this.node_service.get_configuration("visualtopology.pyodide");
            let pyodide = await pyodide_config.get_pyodide();
            let config = this.node_service.get_configuration();
            let con = await config.get_duckdb_connection();
            let my_namespace = pyodide.toPy({
                con: con,
                column_name: this.column_name,
                sql_expr: JSON.stringify(this.sql_expr),
                input_query: input_query });
            let q = await pyodide.runPythonAsync(`
                    q = input_query.add_derived_column(column_name, sql_expr)
                    q
            `,{globals:my_namespace});

            return {
                    "data_out": q
            }
        } else {
            if (this.parser_error) {
                throw Error(this.parser_error);
            } else {
                return {};
            }
        }
    }
}
