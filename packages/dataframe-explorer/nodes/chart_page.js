/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License
*/

var dataset = undefined;
var spec = undefined;
var theme = undefined;

function plot() {
    let vizdiv = document.getElementById("vizdiv");
    vizdiv.innerHTML = "";
    if (spec && dataset) {
        let r = vizdiv.getBoundingClientRect();
        let h = r.height;
        let w = r.width;
        spec.width = w;
        spec.height = h - 40;
        spec.data.values = dataset;
        vegaEmbed(vizdiv, spec, {
            "theme": theme,
            "defaultStyle": false,
            "actions": {"export": true, "source": false, "compiled": false, "editor": false}
        });
    }
}

skadi.page.set_message_handler((msg) => {
    if (msg.dataset) {
        dataset = msg.dataset;
    } else if (msg.spec) {
        spec = msg.spec;
        theme = msg.theme;
    } else {
        spec = undefined;
        theme = undefined;
    }
    plot();
});

window.addEventListener("resize",(evt) => {
    plot();
});