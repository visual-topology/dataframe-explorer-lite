/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.SampleRowsNode = class {

    constructor(node_service) {
        this.node_service = node_service;
        this.update_status();
    }

    open_client(page_id, client_options, client_service) {
        client_service.set_attributes("sample_size",{"value":""+this.sample_size});
        client_service.add_event_handler("sample_size","change", v => {
            try {
                this.sample_size = Number.parseInt(v);
                this.node_service.request_run();
            } catch(e) {
                this.sample_size = null;
            }
            this.update_status();
        });
    }

    get sample_size() { return this.node_service.get_property("sample_size",100); }
    set sample_size(v) { this.node_service.set_property("sample_size",v); }

    update_status() {
        if (this.sample_size === null || this.sample_size < 0) {
            this.node_service.set_status_error("Please use a positive integer");
        } else {
            this.node_service.set_status_info("" + this.sample_size);
        }
    }

    async run(inputs) {
        if (inputs["data_in"] && this.sample_size !== null) {
            let input_query = inputs["data_in"][0];
            let pyodide_config = this.node_service.get_configuration("visualtopology.pyodide");
            let pyodide = await pyodide_config.get_pyodide();
            let my_namespace = pyodide.toPy({
                sample_size: this.sample_size,
                input_query: input_query
            });
            let q = await pyodide.runPythonAsync(`
                    q = input_query.add_sample_rows(sample_size)
                    q
            `,{globals:my_namespace});

            return {
                    "data_out": q
            }
        }
        else {
            return {};
        }
    }
}

