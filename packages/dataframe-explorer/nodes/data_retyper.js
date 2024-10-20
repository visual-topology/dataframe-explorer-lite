/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.DataRetyperNode = class {

    constructor(node_service) {
        this.node_service = node_service;
        this.input_column_types = {};
        this.input_dataset = null;
        this.output_dataset = null;
        this.client_service = null;
        this.refresh();
    }

    get title_label() { return this.node_service.get_property("title_label",""); }
    set title_label(v) { this.node_service.set_property("title_label",v); }

    get date_format() { return this.node_service.get_property("date_format","YYYY-MM-DD"); }
    set date_format(v) { this.node_service.set_property("date_format",v); }

    get_column_types(dataset) {
        let utils = new DataFrameExplorer.AqUtils(dataset);
        let column_names = dataset.columnNames();
        let column_types = {};
        column_names.forEach(name => {
            let column_type = utils.get_column_type(name);
            column_types[name] = column_type;
        });
        return column_types;
    }

    create_conversion(name, original_type, new_type) {
        if (new_type === "number") {
            return this.create_conversion_to_number(name, original_type);
        } else if (new_type === "string") {
            return this.create_conversion_to_string(name, original_type);
        } else if (new_type === "date") {
            return this.create_conversion_to_date(name, original_type);
        } else {
            return 'd => d["' + name + '"]';
        }
    }

    create_conversion_to_string(name, original_type) {
        return 'd => ""+d["'+name+'"]';
    }

    create_conversion_to_number(name, original_type) {
        return 'd => op.parse_float(d["'+name+'"])';
    }

    create_conversion_to_date(name, original_type) {
        return 'd => op.parse_date_custom(d["'+name+'"],"'+this.date_format+'")';
    }

    retype(dataset) {
        let custom_types = this.node_service.get_property("custom_types",{});
        let derives = {};
        for(let name in custom_types) {
            let original_type = this.input_column_types[name];
            let new_type = custom_types[name];
            if (original_type !== new_type) {
                derives[name] = this.create_conversion(name, original_type, new_type);
            }
        }

        if (derives) {
            return dataset.derive(derives);
        } else {
            return dataset;
        }

    }

    export_metadata() {
        let custom_types = this.node_service.get_property("custom_types",{});
        if (this.output_dataset) {
            let output_utils = new DataFrameExplorer.AqUtils(this.output_dataset);
            let metadata = { "metadata": { "columns": [] }};
            let column_names = this.output_dataset.columnNames();
            column_names.forEach(name => {
                let column_metadata = {
                    "name":name,
                    "type":this.input_column_types[name],
                    "custom_type":custom_types[name] || ""
                };
                let analysis = output_utils.analyse(name, 100);
                column_metadata["analysis"] = analysis;
                metadata.metadata.columns.push(column_metadata);
            });
            return metadata;
        } else {
            return {};
        }
    }

    refresh() {
        if (this.input_dataset) {
            this.node_service.set_status_info(""+this.input_dataset.numRows()+" Rows");
            if (this.client_service) {
                this.client_service.send_message(this.export_metadata());
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
        this.client_service.set_message_handler((msg) => { this.handle_page_message(msg) });
        this.client_service.set_attributes("date_format",{ "value": this.date_format });
        this.client_service.add_event_handler("date_format", "change", (v) => {
           this.date_format = v;
           this.node_service.request_run();
        });
        this.refresh();
    }

    handle_page_message(msg) {
        switch(msg.action) {
            case "set_custom_type":
                let column_name = msg.for_column;
                let custom_type = msg.custom_type;
                let custom_types = this.node_service.get_property("custom_types", {});
                if (custom_type) {
                    custom_types[column_name] = custom_type;
                } else {
                    if (column_name in custom_types) {
                        delete custom_types[column_name];
                    }
                }
                this.node_service.set_property("custom_types", custom_types);
                this.node_service.request_run();
                break;
        }
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
            this.input_dataset = inputs["data_in"][0];
            this.input_column_types = this.get_column_types(this.input_dataset);
            this.output_dataset = this.retype(this.input_dataset);
        } else {
            this.input_dataset = null;
            this.output_dataset = null;
        }
        this.refresh();
        if (this.output_dataset) {
            return { "data_out": this.output_dataset };
        }
        return undefined;
    }

}