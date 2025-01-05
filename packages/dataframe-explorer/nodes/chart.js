/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.ChartNode = class {

    constructor(node_service, label_control_names, column_selector_control_names) {
        this.node_service = node_service;
        this.label_control_names = label_control_names;
        this.column_selector_control_names = column_selector_control_names;
        this.dataset = null;
        this.data_uploaded = false;
        this.client_service = null;
        this.configuration_update_callback = () => {
            this.redraw();
        }
        this.node_service.get_configuration().register_update_callback(this.configuration_update_callback);
        this.update_status();
    }

    get title_label() { return this.node_service.get_property("title_label",""); }
    set title_label(v) { this.node_service.set_property("title_label",v); }

    get x_axis_label() { return this.node_service.get_property("x_axis_label",""); }
    set x_axis_label(v) { this.node_service.set_property("x_axis_label",v); }

    get y_axis_label() { return this.node_service.get_property("y_axis_label",""); }
    set y_axis_label(v) { this.node_service.set_property("y_axis_label",v); }

    bind_controls() {
        this.label_control_names.forEach(control_name => {
            this.bind_label_control(control_name, this.node_service.get_property(control_name,""));
        });
        this.column_selector_control_names.forEach(control_name => {
            this.bind_column_selector_control(control_name, this.node_service.get_property(control_name,null));
        });
    }

    update_input_data() {
        this.data_uploaded = false;
        if (this.page_is_open()) {
            this.refresh_controls();
        }
        this.update_status();
        this.redraw();
    }

    update_status() {
        if (this.dataset) {
            if (this.valid()) {
                this.node_service.set_status_info("OK");
            } else {
                this.node_service.set_status_warning("Select Column(s)");
            }
        } else {
            this.node_service.set_status_warning("No Input Data");
        }
    }

    bind_column_selector_control(control_name, initial_value) {
        this.set_selector_options(control_name, []);
        if (initial_value != null) {
            this.client_service.set_attributes(control_name, {"value": initial_value});
        }
        this.client_service.add_event_handler(control_name, "change", v => {
            this.node_service.set_property(control_name, v);
            this.update_status();
            this.redraw();
        });
    }

    set_selector_options(sel_id, names) {
        let options = [["",""]];
        names.forEach(name => options.push([name,name]));
        this.client_service.set_attributes(sel_id,{"options": JSON.stringify(options)});
    }

    bind_label_control(control_name, initial_value) {
        this.client_service.set_attributes(control_name,{"value":initial_value});
        this.client_service.add_event_handler(control_name, "change", new_value => {
            this.node_service.set_property(control_name,new_value);
            this.redraw();
        });
    }

    open_client(page_id, client_options, client_service) {
        this.client_service = client_service;
        this.bind_controls();
        this.refresh_controls();
        this.redraw();
    }

    close_client(page_id) {
        this.client_service = null;
        this.data_uploaded = false;
    }

    page_is_open() {
        return (this.client_service !== null);
    }

    upload() {
        if (this.page_is_open() && !this.data_uploaded) {
            this.client_service.send_message({"dataset": this.dataset});
            this.data_uploaded = true;
        }
    }

    redraw() {
        if (this.page_is_open()) {
            if (this.dataset && this.valid()) {
                this.draw();
            } else {
                this.clear();
            }
        }
    }

    clear() {
        if (this.page_is_open()) {
            this.client_service.send_message({});
        }
    }

    reset_run() {
        this.dataset = null;
        this.data_uploaded = false;
        this.redraw();
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
                    rs = db.run_query(sql, convert_datetimes=True)
                    rs
            `,{globals:my_namespace});
            this.dataset = { "data": dataset_proxy.get("data").toJs(), "columns": dataset_proxy.get("columns").toJs(),
                "column_types": dataset_proxy.get("column_types").toJs()};
            config.postprocess_dataset(this.dataset);
        } else {
            this.dataset = null;
        }
        this.update_input_data();
    }

    close() {
        this.node_service.get_configuration().unregister_update_callback(this.configuration_update_callback);
    }

    valid() {
        /* implement in subclass - return true iff chart can be drawn */
    }

    refresh_controls() {
        /* update controls from dataset and node properties */
    }

    draw() {
        /* implement in subclass - draw contents */
    }

}