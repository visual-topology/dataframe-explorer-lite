/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.TableDisplayNode = class {

    constructor(node_service) {
        this.node_service = node_service;
        this.dataset = null;
        this.client_service = null;
        this.refresh();
    }

    refresh() {
        if (this.dataset) {
            this.node_service.set_status_info(""+this.dataset.data.length+" Rows");
            if (this.client_service) {
                this.client_service.send_message(this.dataset);
            }
        } else {
            if (this.client_service) {
                this.client_service.send_message({});
            }
            this.node_service.set_status_warning("Waiting for input data");
        }
    }

    open_client(page_id, client_options, client_service) {
        this.client_service = client_service;
        this.refresh();
    }

    close_client(page_id) {
        this.client_service = null;
    }

    reset_run() {
        this.dataset = null;
        this.refresh();
    }

    async run(inputs) {
        if (inputs["data_in"]) {
            this.dataset = null;
            let pyodide_config = this.node_service.get_configuration("visualtopology.pyodide");
            let pyodide = await pyodide_config.get_pyodide();
            let config = this.node_service.get_configuration();
            let db = await config.get_duckdb_database();
            let query = inputs["data_in"][0];

            let my_namespace = pyodide.toPy({ db: db, query: query });
            let dataset_proxy = await pyodide.runPythonAsync(`      
                    sql = query.get_sql(db)
                    rs = db.run_query(sql,convert_datetimes=True)
                    rs
            `,{globals:my_namespace});
            this.dataset = { "data": dataset_proxy.get("data").toJs(), "columns": dataset_proxy.get("columns").toJs(),
                "column_types": dataset_proxy.get("column_types").toJs()};
            config.postprocess_dataset(this.dataset);
        } else {
            this.dataset = null;
        }

        console.log(this.dataset["data"].slice(0,20));
        this.refresh();
        if (this.dataset) {
            return {};
        }
        return undefined;
    }

}

