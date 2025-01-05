/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.JoinRowsNode = class {

    constructor(node_service) {
        this.node_service = node_service;
        this.dataset1 = null;
        this.dataset2 = null;
        this.input_column_names1 = [];
        this.input_column_names2 = [];
        this.common_columns = [];
        this.client_service = null;
    }

    get join_column_names() { return this.node_service.get_property("join_column_names",[]); }
    set join_column_names(v) { this.node_service.set_property("join_column_names",v); }

    async input_changed(input_query1, input_query2) {
        let pyodide_config = this.node_service.get_configuration("visualtopology.pyodide");
        let pyodide = await pyodide_config.get_pyodide();
        let config = this.node_service.get_configuration();
        let db = await config.get_duckdb_database();
        let my_namespace = pyodide.toPy({ db: db, input_query: input_query1 });
        let r = await pyodide.runPythonAsync(`      
            sql = input_query.get_sql(db)
            schema = db.check_schema(sql)
            r = {"column_names":[name for (name,_) in schema]}
            r`,{globals:my_namespace});
        this.input_column_names1 = r.get("column_names").toJs();
        my_namespace = pyodide.toPy({ db: db, input_query: input_query2 });
        r = await pyodide.runPythonAsync(`      
            sql = input_query.get_sql(db)
            schema = db.check_schema(sql)
            r = {"column_names":[name for (name,_) in schema]}
            r`,{globals:my_namespace});
        this.input_column_names2 = r.get("column_names").toJs();
        this.refresh_controls();
        this.update_status();
    }

    valid() {
        return (this.valid_join_columns().length > 0);
    }

    update_status() {
        if (this.valid()) {
            this.node_service.set_status_info(this.valid_join_columns().join(","));
        } else {
            this.node_service.set_status_error("No valid join columns");
        }
    }

    refresh_controls() {
        this.common_columns = [];
        this.input_column_names1.forEach(name => {
            if (this.input_column_names2.includes(name)) {
                this.common_columns.push(name);
            }
        });

        if (this.client_service) {
            let options = [];
            this.common_columns.forEach(name => options.push([name,name]));
            const s = JSON.stringify(options);
            this.client_service.set_attributes("join_column_names",{"options":s,"value":JSON.stringify(this.join_column_names)});
        }
    }

    valid_join_columns() {
        let valid_columns = [];
        this.join_column_names.forEach(name => {
            if (this.common_columns.includes(name)) {
                valid_columns.push(name);
            }
        });
        return valid_columns;
    }

    open_client(page_id, client_options, client_service) {
        this.client_service = client_service;
        this.client_service.set_attributes("join_column_names",{"value":JSON.stringify(this.join_column_names)});
        this.client_service.add_event_handler("join_column_names","change", v => {
            this.join_column_names = JSON.parse(v);
            this.update_status();
            this.node_service.request_run();
        });
        this.refresh_controls();
    }

    close_client(page_id) {
        this.client_service = null;
    }

    async run(inputs) {
        this.dataset1 = null;
        this.dataset2 = null;
        if (inputs["data_in1"] && inputs["data_in2"]) {
            let input_query1 = inputs["data_in1"][0];
            let input_query2 = inputs["data_in2"][0];
            await this.input_changed(input_query1,input_query2);
            let join_cols = this.valid_join_columns();
            if (join_cols.length) {
                let pyodide_config = this.node_service.get_configuration("visualtopology.pyodide");
                let pyodide = await pyodide_config.get_pyodide();
                let my_namespace = pyodide.toPy({ join_cols: join_cols, input_query1: input_query1, input_query2: input_query2 });
                let q = await pyodide.runPythonAsync(` 
                    from sql_query_builder.sql_query import JoinTable     
                    q = JoinTable(input_query1, input_query2, [[name,"=",name] for name in join_cols])
                    q
                `,{globals:my_namespace});
                return {"data_out": q};
            }
        }
        return undefined;
    }
}

