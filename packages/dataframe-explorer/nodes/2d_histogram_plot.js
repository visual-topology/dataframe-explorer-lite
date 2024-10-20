/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var DataFrameExplorer = DataFrameExplorer || {};

DataFrameExplorer.Histogram2DPlotNode = class extends DataFrameExplorer.ChartNode {

    constructor(node_service) {
        super(node_service,["title_label","x_axis_label","y_axis_label"],["x_axis","y_axis"]);
    }

    get x_axis() { return this.node_service.get_property("x_axis",""); }
    set x_axis(v) { this.node_service.set_property("x_axis",v); }

    get y_axis() { return this.node_service.get_property("y_axis",""); }
    set y_axis(v) { this.node_service.set_property("y_axis",v); }

    refresh_controls() {
        let column_names = [];
        if (this.get_input_dataset()) {
            column_names = this.get_input_dataset().columnNames();
        }
        this.set_selector_options("x_axis", column_names);
        this.set_selector_options("y_axis", column_names);
    }

    valid() {
        let column_names = this.get_input_dataset().columnNames();
        if (column_names.includes(this.x_axis) && column_names.includes(this.y_axis)) {
            return true;
        }
        return false;
    }

    draw() {
        super.upload();
        let spec = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "description": "A 2D Histogram.",
            "data": {"format": {"type": "csv"}},
            "mark": "rect",
            "padding": 20,
            "autosize": {
              "type": "fit",
              "contains": "padding"
            },
            "encoding": {
                "x": {"field": this.x_axis, "type": "quantitative", "bin": {"maxbins": 50}},
                "y": {"field": this.y_axis, "type": "quantitative", "bin": {"maxbins": 50}},
                "color": {
                    "aggregate": "count",
                    "type": "quantitative"
                }
            },
            "config": {
                "view": {
                    "stroke": "transparent"
                }
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
        let msg = {
            "theme": this.node_service.get_configuration().get_theme(),
            "spec": spec
        }
        this.client_service.send_message(msg);
    }
}
