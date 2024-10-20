/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.SelectRowsNode = class {

    constructor(node_service) {
        this.node_service = node_service;
        this.update_status();
    }

    get column_expression() { return this.node_service.get_property("column_expression",""); }
    set column_expression(v) { this.node_service.set_property("column_expression",v); }

    update_status() {
        if (this.column_expression !== "") {
            this.node_service.set_status_info(""+this.column_expression);
        } else {
            this.node_service.set_status_warning("Configure Settings");
        }
    }

    open_client(page_id, client_options, client_service) {
        client_service.set_attributes("column_expression",{"value":this.column_expression});
        client_service.add_event_handler("column_expression","change", v => {
            this.column_expression = v;
            this.update_status();
            this.node_service.request_run();
        });
    }

    async run(inputs) {
        if (inputs["data_in"]) {
            this.dataset = inputs["data_in"][0];
            let dataset = inputs["data_in"][0];
            if (this.column_expression) {
                let aq = new DataFrameExplorer.AqUtils(dataset);
                try {
                    let filter_expression = aq.preprocess_expression(this.column_expression);
                    return {"data_out": this.dataset.filter(filter_expression)};
                } catch(e) {
                    this.node_service.set_status_error(e.message);
                }
            }
        }
        return {};
    }
}

