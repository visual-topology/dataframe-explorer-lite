/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.CsvImportNode = class {

    constructor(node_service) {
        this.node_service = node_service;

        this.update_status();

        this.example_datasets = [
            ["iris.csv", "iris"],
            ["titanic.csv", "titanic"],
            ["taxis.csv", "taxis"],
            ["seaice.csv", "seaice"],
            ["tips.csv", "tips"],
            ["test.csv","test"]
        ]

        this.schema = { "data":[], "columns": ["Name","Data Type"] };
        this.client_service = null;
    }

    get dataset_type() { return this.node_service.get_property("dataset_type","example") }
    set dataset_type(v) { this.node_service.set_property("dataset_type",v); }

    get example_dataset() { return this.node_service.get_property("example_dataset","iris.csv"); }
    set example_dataset(v) { return this.node_service.set_property("example_dataset",v); }

    get custom_filename() { return this.node_service.get_property("custom_filename",""); }
    set custom_filename(v) { this.node_service.set_property("custom_filename",v); }

    get filename() { return this.dataset_type === "custom" ? this.custom_filename : this.example_dataset }

    get has_headers() { return this.node_service.get_property("has_headers",true); }
    set has_headers(v) { return this.node_service.set_property("has_headers",v); }

    get delimiter() { return this.node_service.get_property("delimiter","comma"); }
    set delimiter(v) { return this.node_service.set_property("delimiter",v); }

    get custom_delimiter() { return this.node_service.get_property("custom_delimiter",";"); }
    set custom_delimiter(v) { return this.node_service.set_property("custom_delimiter",v); }

    get date_format() { return this.node_service.get_property("date_format","auto"); }
    set date_format(v) { return this.node_service.set_property("date_format",v); }

    get custom_date_format() { return this.node_service.get_property("custom_date_format","%Y/%m/%d %H:%M:%S"); }
    set custom_date_format(v) { return this.node_service.set_property("custom_date_format",v); }

    update_status() {
        if (this.filename) {
            this.node_service.set_status_info(this.filename);
        } else {
            this.node_service.set_status_warning("No file selected");
        }
    }

    update_section_visibility() {
        if (this.dataset_type === "custom") {
            this.client_service.set_attributes("upload_section", {"style": "display:block;"});
            this.client_service.set_attributes("example_section", {"style": "display:none;"});
        } else {
            this.client_service.set_attributes("upload_section", {"style": "display:none;"});
            this.client_service.set_attributes("example_section", {"style": "display:block;"});
        }
    }

    open_client(page_id, client_options, client_service) {
        client_service.set_attributes("dataset_type",{"value": this.dataset_type});
        client_service.set_attributes("delimiter", {"value": this.delimiter});
        client_service.set_attributes("custom_delimiter", {"value": this.custom_delimiter});
        client_service.set_attributes("has_headers", {"value": this.has_headers});
        client_service.set_attributes("date_format", {"value": this.date_format});
        client_service.set_attributes("custom_date_format", {"value": this.custom_date_format});
        client_service.set_attributes("upload", { "filename": this.custom_filename});

        client_service.set_attributes("select_example_dataset", { "options": JSON.stringify(this.example_datasets), "value": this.example_dataset});

        if (this.date_format === "custom") {
            client_service.set_attributes("custom_date_format_cell", {"style": "display:block;"});
        }

        if (this.delimiter === "custom") {
            client_service.set_attributes("custom_delimiter_cell", {"style": "display:block;"});
        }

        client_service.add_event_handler("select_example_dataset", "input", async (value) => {
            this.example_dataset = value;
            this.update_status();
            this.node_service.request_run();
        });

        client_service.add_event_handler("dataset_type", "change", (value) => {
            this.dataset_type = value;
            this.update_section_visibility();
            this.update_status();
            this.node_service.request_run();
        });

        client_service.add_event_handler("has_headers", "input", async (value) => {
           this.has_headers = value;
           this.node_service.request_run();
        },"checked");

        client_service.add_event_handler("delimiter", "change", (value) => {
            let custom_display = "display:"+((value === "custom") ? "block;": "none;");
            client_service.set_attributes("custom_delimiter_cell", {"style": custom_display});
            this.delimiter = value;
            this.node_service.request_run();
        });

        client_service.add_event_handler("date_format", "change", (value) => {
            let custom_display = "display:"+((value === "custom") ? "block;": "none;");
            client_service.set_attributes("custom_date_format_cell", {"style": custom_display});
            this.date_format = value;
            this.node_service.request_run();
        });

        client_service.add_event_handler("custom_date_format", "change", (value) => {
            this.custom_date_format = value;
            this.node_service.request_run();
        });

        client_service.add_event_handler("custom_delimiter", "change", (value) => {
            this.custom_delimiter = value;
            this.node_service.request_run();
        });

        client_service.set_message_handler(async (header, content) => {
            await this.recv_page_message(header, content);
        });
        this.client_service = client_service;
        this.update_section_visibility();
        this.refresh_schema();
    }

    close_client(page_id) {
        this.client_service = null;
    }

    async recv_page_message(header,content) {
       let encoder = new TextEncoder("utf-8");
       let binary_content = encoder.encode(content).buffer;
       await this.node_service.set_data("custom_content",binary_content);
       this.custom_filename = header["filename"];
       this.update_status();
       this.node_service.request_run();
    }

    refresh_schema() {
        if (this.client_service) {
            this.client_service.send_message(this.schema);
        }
    }

    async run(inputs) {
        let content = "";
        this.schema["data"] = [];

        if (this.dataset_type === "custom") {
            let binary_content = await this.node_service.get_data("custom_content");
            if (binary_content !== null) {
                let decoder = new TextDecoder("utf-8");
                content = decoder.decode(binary_content);
            }
        } else {
            if (this.example_dataset) {
                let url = this.node_service.resolve_resource("assets/" + this.example_dataset);
                let r = await fetch(url);
                content = await r.text();
                this.update_status();
            }
        }

        if (!content) {
            throw new Error("No Data");
        }

        let pyodide_config = this.node_service.get_configuration("visualtopology.pyodide");
        let pyodide = await pyodide_config.get_pyodide();
        let config = this.node_service.get_configuration();
        let con = await config.get_duckdb_connection();

        let node_id = this.node_service.get_node_id();

        let has_headers = (this.dataset_type === "custom" ? this.has_headers : true);
        let delimiter = (this.delimiter === "custom" && this.dataset_type === "custom") ? this.custom_delimiter : null;
        let date_format = (this.date_format === "custom" && this.dataset_type === "custom") ? this.custom_date_format : null;

        let my_namespace = pyodide.toPy({
            content: content,
            con: con,
            node_id: node_id,
            has_headers:has_headers,
            delimiter: delimiter,
            date_format: date_format
        });

        let bt = await pyodide.runPythonAsync(`
                from sql_query_builder.sql_query import BaseTable 
                with open(node_id+".csv","w") as f:
                    f.write(content)
                kwargs = {}
                if delimiter:
                    kwargs["delimiter"] = delimiter    
                if date_format:
                    kwargs["date_format"] = date_format 
                con.execute("DROP TABLE IF EXISTS "+node_id)   
                rel = con.read_csv(node_id+".csv",header=has_headers,**kwargs)
                rel.create(node_id) 
                BaseTable(node_id)
        `,{globals:my_namespace});

        let db = await config.get_duckdb_database();

        my_namespace = pyodide.toPy({ db: db, query: bt });
        let schema_proxy = await pyodide.runPythonAsync(`
            schema = query.get_schema(db)
            {"schema":schema}
        `,{globals:my_namespace});

        this.schema["data"] = schema_proxy.get("schema").toJs();

        console.log("schema:"+JSON.stringify(this.schema));
        this.refresh_schema();

        return {
                "data_out": bt
        }
    }
}
