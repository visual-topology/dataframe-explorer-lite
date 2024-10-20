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

    input_changed() {
        this.input_column_names1 = [];
        this.input_column_names2 = [];
        if (this.dataset1) {
            this.input_column_names1 = this.dataset1.columnNames();
        }
        if (this.dataset2) {
            this.input_column_names2 = this.dataset2.columnNames();
        }
        this.refresh_controls();
    }

    refresh_controls() {
        this.common_columns = [];
        this.input_column_names1.forEach(name => {
            if (this.input_column_names2.includes(name)) {
                this.common_columns.push(name);
            }
        });

        if (this.client_service) {
            let options = [["",""]];
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
        if (inputs["data_in1"]) {
            this.dataset1 = inputs["data_in1"][0];
        }
        if (inputs["data_in2"]) {
            this.dataset2 = inputs["data_in2"][0];
        }
        this.input_changed();
        if (this.dataset1 && this.dataset2) {
            let join_cols = this.valid_join_columns();
            if (join_cols.length) {
                return {"data_out": this.dataset1.join(this.dataset2, join_cols)};
            }
        }
        return undefined;
    }
}

