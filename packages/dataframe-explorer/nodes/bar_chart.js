/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.BarChartNode = class extends DataFrameExplorer.ChartNode {

    constructor(node_service) {
        super(node_service,["title_label","x_axis_label","y_axis_label"],["x_axis","hue"]);
    }

    get x_axis() { return this.node_service.get_property("x_axis",""); }
    set x_axis(v) { this.node_service.set_property("x_axis",v); }

    get hue() { return this.node_service.get_property("hue",""); }
    set hue(v) { this.node_service.set_property("hue",v); }

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
            "description": "A scatterplot.",
            "data": {"format": {"type": "json"}},
            "mark": "bar",
            "padding": 20,
            "autosize": {
              "type": "fit",
              "contains": "padding"
            },
            "encoding": {
                "x": {"field": this.x_axis, "type": "nominal"},
                "y": {"aggregate":"count"},
                "tooltip": {"aggregate":"count"}
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
            spec["encoding"]["color"] = {"field": this.hue, "type": "nominal"};
        }
        let msg = {
            "theme": this.node_service.get_configuration().get_theme(),
            "spec": spec
        }
        this.client_service.send_message(msg);
    }
}

