/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.CreateColumnNode = class {

    constructor(node_service) {
        this.node_service = node_service;
        this.update_status();
    }

    get column_name() { return this.node_service.get_property("column_name",""); }
    set column_name(v) { this.node_service.set_property("column_name",v); }

    get column_expression() { return this.node_service.get_property("column_expression",""); }
    set column_expression(v) { this.node_service.set_property("column_expression",v); }

    update_status() {
        if (this.column_name !== "" && this.column_expression !== "") {
            this.node_service.set_status_info(""+this.column_name);
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
            this.update_status();
            this.node_service.request_run();
        });
    }

    async run(inputs) {
        if (inputs["data_in"] && this.column_name && this.column_expression) {
            let dataset = inputs["data_in"][0];
            let derive_cols = {};
            let aq = new DataFrameExplorer.AqUtils(dataset);
            try {
                derive_cols[this.column_name] = aq.preprocess_expression(this.column_expression);
                return { "data_out": dataset.derive(derive_cols) }
            } catch(e) {
                this.node_service.set_status_error(e.message);
            }
        } else {
            return {};
        }
    }
}
