/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.SelectColumnsNode = class {

    constructor(node_service) {
        this.node_service = node_service;
        this.elt = null;
        this.dataset = null;
        this.input_column_names = [];
        this.client_service = null;
        this.update_status();
    }

    get column_names() { return this.node_service.get_property("column_names",[]); }
    set column_names(v) { this.node_service.set_property("column_names",v); }

    update_status() {
        if (this.valid()) {
            this.node_service.set_status_info(this.column_names.length+" Columns");
        } else {
            this.node_service.set_status_error("Select Columns...");
        }
    }

    async input_changed(input_query) {
        let pyodide_config = this.node_service.get_configuration("visualtopology.pyodide");
        let pyodide = await pyodide_config.get_pyodide();
        let config = this.node_service.get_configuration();
        let db = await config.get_duckdb_database();
        let my_namespace = pyodide.toPy({ db: db, input_query: input_query });
        let r = await pyodide.runPythonAsync(`      
                sql = input_query.get_sql(db)
                schema = db.check_schema(sql)
                r = {"column_names":[name for (name,_) in schema]}
                r
        `,{globals:my_namespace});
        this.input_column_names = r.get("column_names").toJs();
        console.log(JSON.stringify(this.input_column_names));
        this.refresh_controls();
    }

    refresh_controls() {
        let options = [];
        this.input_column_names.forEach(name => options.push([name,name]));
        if (this.client_service) {
            const s = JSON.stringify(options);
            this.client_service.set_attributes("column_names", {
                "options": s,
                "value": JSON.stringify(this.column_names)
            });
        }
    }

    valid() {
        return (this.column_names.length > 0);
    }

    open_client(page_id, client_options, client_service) {
        this.client_service = client_service;
        this.refresh_controls();
        this.client_service.add_event_handler("column_names","change", v => {
            this.column_names = JSON.parse(v);
            this.update_status();
            this.node_service.request_run();
        });
    }

    close_client(page_id) {
        this.client_service = null;
    }

    async run(inputs) {
        this.input_column_names = [];
        if (inputs["data_in"]) {
            let input_query = inputs["data_in"][0];
            await this.input_changed(input_query);
            if (this.valid()) {
                let pyodide_config = this.node_service.get_configuration("visualtopology.pyodide");
                let pyodide = await pyodide_config.get_pyodide();
                let my_namespace = pyodide.toPy({ column_names: this.column_names, input_query: input_query });
                let q = await pyodide.runPythonAsync(`      
                    q = input_query.select_columns(column_names)
                    q
                `,{globals:my_namespace});
                return {"data_out": q};
            }
        } else {
            return {};
        }
    }
}

