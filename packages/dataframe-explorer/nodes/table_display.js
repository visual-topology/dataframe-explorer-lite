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

    export_table() {
        if (this.dataset) {
            let col_json = JSON.parse(this.dataset.toJSON({ schema: false }));
            let columns = [];
            for (let column in col_json) {
                columns.push(column);
            }
            let data = [];
            if (columns.length > 0) {
                let row_count = col_json[columns[0]].length;
                for(let idx=0; idx<row_count; idx++) {
                    let row = [];
                    columns.forEach(column => {
                        row.push(col_json[column][idx]);
                    });
                    data.push(row);
                }
            }
            return { "columns":columns, "data":data };
        } else {
            return {};
        }
    }

    refresh() {
        if (this.dataset) {
            this.node_service.set_status_info(""+this.dataset.numRows()+" Rows");
            if (this.client_service) {
                this.client_service.send_message(this.export_table());
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
            this.dataset = inputs["data_in"][0];
        } else {
            this.dataset = null;
        }
        this.refresh();
        if (this.dataset) {
            return {};
        }
        return undefined;
    }

}

