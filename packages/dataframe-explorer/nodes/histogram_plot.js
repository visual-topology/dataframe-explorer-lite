/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.HistogramPlotNode = class extends DataFrameExplorer.ChartNode {

    constructor(node_service) {
        super(node_service,["title_label","x_axis_label","y_axis_label"],["x_axis","hue"]);
    }

    get x_axis() { return this.node_service.get_property("x_axis",""); }
    set x_axis(v) { this.node_service.set_property("x_axis",v); }

    get hue() { return this.node_service.get_property("hue",""); }
    set hue(v) { this.node_service.set_property("hue",v); }

    get use_custom_settings() { return this.node_service.get_property("use_custom_settings",false); }
    set use_custom_settings(v) { this.node_service.set_property("use_custom_settings",v); }

    get custom_band_width() { return this.node_service.get_property("custom_band_width",1); }
    set custom_band_width(v) { this.node_service.set_property("custom_band_width",v); }

    valid() {
        let column_names = this.dataset.columns;
        if (column_names.includes(this.x_axis)) {
            if (this.hue === "" || column_names.includes(this.hue)) {
                return true;
            }
        }
        return false;
    }

    refresh_controls() {
        let column_names = [];
        if (this.dataset) {
            column_names = this.dataset.columns;
        }
        this.set_selector_options("x_axis", column_names);
        this.set_selector_options("hue", column_names);
    }

    draw() {
        super.upload();
        let spec = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": {"format": {"type": "json"}},
            "mark": "bar",
            "padding": 20,
            "autosize": {
              "type": "fit",
              "contains": "padding"
            },
            "encoding": {
                "x": {
                    "field": this.x_axis
                },
                "y": {"aggregate": "count"},
                "tooltip": {"aggregate": "count"}
            }
        }
        if (this.use_custom_settings) {
            spec["encoding"]["x"]["bin"] = {
                "step": this.custom_band_width
            }
        }
        if (this.title_label) {
            spec["title"] =  { "text": this.title_label };
        }
        if (this.x_axis_label) {
            spec.encoding.x.title = this.x_axis_label;
        }
         if (this.y_axis_label) {
            spec.encoding.y.title = this.y_axis_label;
        }
        if (this.hue) {
            spec["encoding"]["color"] = {"field":this.hue, "type": "nominal"};
        }
        let msg = {
            "theme": this.node_service.get_configuration().get_theme(),
            "spec": spec
        };
        this.client_service.send_message(msg);
    }

    open_client(page_id, client_options, client_service) {
        super.open_client(page_id, client_options, client_service);
        this.client_service.set_attributes("use_custom",{"value":""+this.use_custom_settings});
        this.client_service.add_event_handler("use_custom","change", (v) => {
            this.use_custom_settings = v;
            super.redraw();
        },"checked");

        this.client_service.set_attributes("custom_band_width",{"value":""+this.custom_band_width});
        this.client_service.add_event_handler("custom_band_width", "change", (v) => {
            this.custom_band_width = Number.parseFloat(v);
            super.redraw();
        });
    }
}

