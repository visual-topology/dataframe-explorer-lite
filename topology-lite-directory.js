/*   Visual Topology - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var visualtopology_lite = visualtopology_lite || {};
/* src/js/topology-directory-lite.js */

var visualtopology_lite = visualtopology_lite || {};

visualtopology_lite.TopologyDirectoryLite = class {

    constructor(element_id, options) {
        this.element_id = element_id;
        this.options = options;
        this.designer_url = options["designer_url"];
    }

    async load() {
        let plugins = {
            "topology_store": new visualtopology_lite.TopologyStoreLite()
        }
        this.skadi_directory_api = new skadi.DirectoryApi(this.element_id, this.options, plugins);

        if (this.designer_url) {
            this.skadi_directory_api.set_open_topology_in_designer_handler((topology_id) => {
                window.open(this.designer_url + "?topology_id=" + topology_id, "_self")
            });
        }

        await this.skadi_directory_api.load();
    }
}







/* src/js/plugins/topology_store_lite.js */

var visualtopology_lite = visualtopology_lite || {};

visualtopology_lite.TopologyStoreLite = class extends skadi.TopologyStore {

    constructor() {
        super();
    }
}

