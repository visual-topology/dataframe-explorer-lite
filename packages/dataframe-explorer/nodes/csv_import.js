/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.CsvImportNode = class {

    constructor(node_service) {
        this.node_service = node_service;
        this.imported_table = null;

        this.update_status();

        this.example_dataset_filenames = {
            "iris": "iris.csv",
            "drug-use-by-age": "drug-use-by-age.csv"
        }

        if (this.load_custom) {
            setTimeout(async () => {
                let binary_content = await this.node_service.get_data("custom_content");
                if (binary_content !== null) {
                    let decoder = new TextDecoder("utf-8");
                    let content = decoder.decode(binary_content);
                    this.load_custom_content(content);
                }
            },0);
        } else {
            this.upload(this.example_dataset);
        }
    }

    get load_custom() { return this.node_service.get_property("load_custom",false) }
    set load_custom(v) { this.node_service.set_property("load_custom",v); }

    get example_dataset() { return this.node_service.get_property("example_dataset","iris"); }
    set example_dataset(v) { return this.node_service.set_property("example_dataset",v); }

    get filename() { return this.node_service.get_property("filename",""); }
    set filename(v) { this.node_service.set_property("filename",v); }

    update_status() {
        if (this.imported_table == null) {
            if (this.load_custom && this.filename) {
                this.node_service.set_status_error("Uploaded file "+this.filename+" not found in browser storage,\n please re-upload.");
            } else {
                this.node_service.set_status_error("No file selected.");
            }
        } else {
            if (this.filename) {
                this.node_service.set_status_info(this.filename);
            } else {
                this.node_service.set_status_warning("No file selected");
            }
        }
    }

    async upload(dataset_name) {
        let filename = this.example_dataset_filenames[dataset_name];
        let url = this.node_service.resolve_resource("assets/"+filename);
        await fetch(url).then(r => r.text()).then(txt => {
            this.imported_table = aq.fromCSV(txt);
            this.filename = filename;
            this.node_service.request_run();
        });
        this.update_status();
    }

    load_custom_content(content) {
        if (content) {
            this.imported_table = aq.fromCSV(content);
            this.update_status();
            this.node_service.request_run();
        } else {
            this.imported_table = null;
            this.update_status();
            this.node_service.request_run();
        }
    }

    open_client(page_id, client_options, client_service) {
        client_service.set_attributes("select_example_dataset", {"value": this.example_dataset});

        if (this.load_custom) {
            client_service.set_attributes("use_custom", {"checked": "true"});
            client_service.set_attributes("upload_section", {"style": "display:block;"});
        } else {
            client_service.set_attributes("use_example", {"checked": "true"});
            client_service.set_attributes("upload_section", {"style": "display:none;"});
        }

        client_service.add_event_handler("select_example_dataset", "input", async (value) => {
            this.example_dataset = value;
            await this.upload(this.example_dataset);
        });

        client_service.add_event_handler("use_custom", "input", (evt) => {
            client_service.set_attributes("upload_section", {"style": "display:block;"});
            this.load_custom = true;
            this.filename = "";
            client_service.set_attributes("upload", {"filename": ""});
            this.imported_table = null;
            this.update_status();
            this.node_service.request_run();
        });

        client_service.add_event_handler("use_example", "input", async (evt) => {
            client_service.set_attributes("upload_section", {"style": "display:none;"});
            this.load_custom = false;
            await this.node_service.set_data("custom_content",null);
            await this.upload(this.example_dataset);
        });

        client_service.set_message_handler(async (header, content) => {
            await this.recv_page_message(header, content);
        });
    }

    close_client(page_id) {
    }

    async recv_page_message(header,content) {
        try {
            let encoder = new TextEncoder("utf-8");
            let binary_content = encoder.encode(content).buffer;
            await this.node_service.set_data("custom_content",binary_content);
            this.load_custom_content(content);
            this.filename = header["filename"];
            this.update_status();
        } catch(ex) {
            this.filename = '';
            this.node_service.set_status_error("Unable to load data from "+header["filename"]);
        }
    }

    async run(inputs) {
        // let pyodide_config = this.node_service.get_configuration("visualtopology.pyodide");
        // let pyodide = await pyodide_config.get_pyodide();
        // await pyodide_config.load_packages(["numpy"]);
        // console.log(pyodide.runPython(`
        //        import sys
        //        sys.version
        //        `));

        if (this.imported_table) {
            return {
                "data_out": this.imported_table
            };
        } else {
            return {};
        }
    }
}
