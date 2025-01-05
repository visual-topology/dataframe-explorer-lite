/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.Configuration = class {

    constructor(configuration_services) {
        this.services = configuration_services;
        this.update_callbacks = [];
        this.con = null;
    }

    async load() {
        await this.get_duckdb_connection();
    }

    async get_duckdb_connection() {
        if (this.con) {
            return this.con;
        }
        let pyodide_config = this.services.get_configuration("visualtopology.pyodide");


        // await pyodide_config.mount_filesystem("/files");
        let pyodide = await pyodide_config.get_pyodide();

        await pyodide_config.load_packages(["duckdb"]);
        this.con = pyodide.runPython(`
            import duckdb
            con = duckdb.connect(":memory:")
            con`)

        let url = this.services.resolve_resource("sql_query_builder.zip");
        let my_namespace = pyodide.toPy({ url: url });
        await pyodide.runPythonAsync(`
            from pyodide.http import pyfetch
            response = await pyfetch(url) 
            await response.unpack_archive() # by default, unpacks to the current dir
        `,{"globals":my_namespace})
        pyodide.pyimport("sql_query_builder");
        return this.con;
    }

    async get_duckdb_database() {
        let pyodide_config = this.services.get_configuration("visualtopology.pyodide");
        let pyodide = await pyodide_config.get_pyodide();
        let con  = await this.get_duckdb_connection();
        let my_namespace = pyodide.toPy({ con: con });
        let db = pyodide.runPython(`
            from sql_query_builder.duckdb_database import DuckDBDatabase
            db = DuckDBDatabase(con)
            db`,{"globals":my_namespace})
        return db;
    }

    open_client(page_id,client_options,page_service) {
        let themes = ["quartz","excel","ggplot2","vox","fivethirtyeight","dark","latimes","urbaninstitute","googlecharts","powerbi","carbonwhite","carbong10","carbong90","carbong100"]
        let attrs = {
            "options":JSON.stringify(themes.map(t => [t,t])),
            "value":this.services.get_property("theme","quartz")};
        page_service.set_attributes("select_theme",attrs);
        page_service.add_event_handler("select_theme","change", (v) => {
            this.services.set_property("theme",v);
            this.updated();
        });
    }

    close_client(page_id) {
    }

    get_theme() {
        return this.services.get_property("theme","quartz");
    }

    register_update_callback(callback) {
        console.log("registered callback");
        this.update_callbacks.push(callback);
    }

    unregister_update_callback(callback) {
        const idx = this.update_callbacks.indexOf(callback);
        if (idx > -1) {
            console.log("removed callback");
            this.update_callbacks.splice(idx,1);
        }
    }

    updated() {
        this.update_callbacks.forEach(callback => {
            try {
                callback();
            } catch(e) {
                console.error(e);
            }
        });
    }

    postprocess_dataset(dataset) {
        console.log(dataset.column_types);
        for(let col_idx=0; col_idx<dataset.column_types.length; col_idx+=1) {
            if (dataset.column_types[col_idx] === "DATETIME" || dataset.column_types[col_idx] === "DATE") {
                for(let row_idx=0; row_idx<dataset.data.length; row_idx+=1) {
                    let v = dataset.data[row_idx][col_idx];
                    if (v !== null && v !== undefined) {
                        dataset.data[row_idx][col_idx] = new Date(v*1000);
                    }
                }
            }
        }
    }
}