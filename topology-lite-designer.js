/*   Visual Topology - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License 
*/

var visualtopology_lite = visualtopology_lite || {};
/* src/js/topology-lite.js */

visualtopology_lite.version = "0.5.0";

visualtopology_lite.TopologyLite = class {

    constructor(topology_id, container_id, options) {
        this.topology_id = topology_id;
        this.container_id = container_id;
        this.options = options;
        if (!("worker_path" in options)) {
            this.options["worker_path"] = "topology-lite-worker.js";
        }

        if (!("document_path" in options)) {
            this.options["document_path"] = "";
        }

        this.graph_worker_client = new GraphWorkerClient(this.options["worker_path"], this.options["document_path"], this.options["utils_path"]);
    }


    async handle_load_topology_from(topology_id) {

        // first create a new entry in the directory / or update the access date
        let td = new visualtopology_lite.TopologyDirectory();
        await td.open_topology(topology_id);

        // load from client storage if present...
        let db = new visualtopology_lite.ClientStorage(topology_id);
        let contents = await db.get_item("topology.json");

        if (contents) {
            let obj = JSON.parse(contents);
            await this.load(obj, {});
        }
    }

    get_version() {
        return visualtopology_lite.version;
    }
}

/* src/js/topology-designer-lite.js */

visualtopology_lite.TopologyDesignerLite = class extends visualtopology_lite.TopologyLite {

    constructor(topology_id, container_id, options) {
        super(topology_id, container_id, options);
        this.designer = null;
    }

    async init() {

        let plugins = {
            "topology_store": new visualtopology_lite.TopologyStoreLite(),
            "resource_loader": null,
            "instance_factory": new visualtopology_lite.InstanceFactoryLite(this.graph_worker_client)
        };

        let platform_extensions = this.options.platform_extensions || [];
        platform_extensions.push({"name":"Visual Topology Lite", "version": this.get_version(), "license_name":"MIT", "url":"https://github.com/visual-topology/visual-topology-lite"})
        this.options.platform_extensions = platform_extensions;
        this.options.expose_pause_resume = true;
        this.designer = await skadi.start_designer(this.topology_id, this.container_id, this.options, plugins);

        this.graph_worker_client.bind(this.designer);
    }

    async load(from_obj, node_renamings, suppress_callbacks) {
        await this.designer.load(from_obj, node_renamings, suppress_callbacks);
    }
}

/* src/js/plugins/instance_factory_lite.js */

var visualtopology_lite = visualtopology_lite || {};

visualtopology_lite.InstanceFactoryLite = class {

    constructor(graph_worker_client) {
        this.graph_worker_client = graph_worker_client;
    }

    async create_node_instance(node_id, node_type_id, classname, service) {
        return new visualtopology_lite.GraphWorkerNodeWrapper(this.graph_worker_client, node_id, node_type_id, classname);
    }

    async create_configuration_instance(package_id, classname, service) {
        return new visualtopology_lite.GraphWorkerConfigurationWrapper(this.graph_worker_client, package_id, classname);
    }
}

/* src/js/plugins/topology_store_lite.js */

var visualtopology_lite = visualtopology_lite || {};

visualtopology_lite.TopologyStoreLite = class extends skadi.TopologyStore {

    constructor() {
        super();
    }
}

/* src/js/graph_worker/graph_worker_wrapper.js */

var visualtopology_lite = visualtopology_lite || {};

visualtopology_lite.GraphWorkerWrapper = class {

    constructor(graph_worker_client, target_id, classname) {
        this.target_id = target_id;
        this.classname = classname;
        this.graph_worker_client = graph_worker_client;
    }

    open_client(page_id, client_options, client_service) {
        this.graph_worker_client.open_client(this.target_id, page_id, client_options, client_service);
    }

    close_client(page_id) {
        this.graph_worker_client.close_client(this.target_id, page_id);
    }
}

/* src/js/graph_worker/graph_worker_configuration_wrapper.js */

var visualtopology_lite = visualtopology_lite || {};

visualtopology_lite.GraphWorkerConfigurationWrapper = class extends visualtopology_lite.GraphWorkerWrapper {

    constructor(client, package_id, classname) {
        super(client, package_id, classname);
    }


    updated() {
        // called when the configuration's properties/data have been updated from loading a new topology

    }
}

/* src/js/graph_worker/graph_worker_node_wrapper.js */

var visualtopology_lite = visualtopology_lite || {};

visualtopology_lite.GraphWorkerNodeWrapper = class extends visualtopology_lite.GraphWorkerWrapper{

    constructor(client, node_id, node_type_id, classname) {
        super(client, node_id, classname);
    }

    notify_connections_changed(new_connection_counts) {
        /* if (this.instance.connections_changed) {
            try {
                this.instance.connections_changed(new_connection_counts["inputs"],new_connection_counts["outputs"]);
            } catch(e) {
                console.error(e);
            }
        } */
    }

    remove() {

    }
}

/* src/js/graph_worker/graph_worker_client.js */

class GraphWorkerClient {

    constructor(worker_path, document_path, utils_path) {
        this.worker_path = worker_path;  /* path from the document to the worker code */
        this.document_path = document_path; /* path from worker to the document */
        this.utils_path = utils_path; /* path from document to skadi-utils.js */
        let script_path = this.worker_path.endsWith(".js") ? this.worker_path : this.worker_path+"/worker.js";
        this.worker = new Worker(script_path);
        this.worker.onmessage = (ev) => {
            let control_packet = JSON.parse(ev.data[0]);
            let extras = ev.data.slice(1);
            this.handle(control_packet,extras);
        }
        this.client_services = {};
    }

    send(control_packet,...extra) {
        let message_parts = [JSON.stringify(control_packet)];
        extra.forEach(o => {
            message_parts.push(o);
        })
        this.worker.postMessage(message_parts);
    }

    add_package(package_id, base_url) {
        this.send({
            "action": "add_package",
            "package_id": package_id,
            "base_url": base_url
        });
    }


    add_node(node_id, node_type_id) {
        this.send({
            "action": "add_node",
            "node_id": node_id,
            "node_type_id": node_type_id
        });
    }

    remove_node(node_id) {
        this.send({
            "action": "remove_node",
            "node_id": node_id
        });
    }

    add_link(link_id, link_type, from_node_id, from_port, to_node_id, to_port) {
        this.send({
            "action": "add_link",
            "link_id": link_id,
            "link_type": link_type,
            "from_node_id": from_node_id,
            "from_port": from_port,
            "to_node_id": to_node_id,
            "to_port": to_port
        });
    }

    remove_link(link_id) {
        this.send({
            "action": "remove_link",
            "link_id": link_id
        });
    }

    clear() {
        this.send({
            "action": "clear"
        });
    }

    pause() {
        this.send({
            "action": "pause"
        });
    }

    resume() {
        this.send({
            "action": "resume"
        });
    }

    bind(skadi) {

        this.skadi = skadi;

        let class_map = skadi.get_core().get_schema().get_class_map();

        let imports = [];
        if (!this.worker_path.endsWith(".js")) {
            // running in developer mode, import the scripts individually
            let skadi_utils_path = this.utils_path;
            if (this.document_path) {
                skadi_utils_path = this.document_path + "/" + skadi_utils_path;
            }
            imports.push(skadi_utils_path);
            imports.push("../plugins/topology_store_lite.js",
                "client_service.js", "package_settings.js", "wrapper.js", "service.js", "configuration_service.js", "configuration_wrapper.js",
                "graph_link.js", "graph_executor.js", "node_execution_failed.js",
                "node_service.js", "node_wrapper.js");
        }

        let resource_urls = skadi.get_core().get_schema().get_resource_urls();
        resource_urls.forEach((url) => imports.push(url.startsWith("http") ? url : (this.document_path ? this.document_path+"/"+url : url)));

        this.send({"action":"init","imports":imports, "class_map":class_map, "topology_id":skadi.get_core().get_id()});

        // for each package
        this.skadi.get_package_list().forEach(package_id => {
            let package_type = this.skadi.get_core().get_schema().get_package_type(package_id);
            let l10n_utils = package_type.get_l10n_utils();
            let language = undefined;
            let bundle_contents = {};
            if (l10n_utils) {
                let language = l10n_utils.get_language();
                let bundles = l10n_utils.get_bundles();
                Object.keys(bundles).forEach(key => {
                    bundle_contents[language] = bundles[language].get_content();
                });
            }
            this.add_package(package_id, this.document_path ? this.document_path+"/"+package_type.get_base_url() : package_type.get_base_url());
            // set up a listener for any language changes
        });

        if (skadi.add_node_event_handler) {

            // hook up to skadi events
            skadi.add_node_event_handler("add", (node_id, node_type_id) => {
                this.add_node(node_id, node_type_id);
            });

            skadi.add_node_event_handler("remove", (node_id, node_type_id) => {
                this.remove_node(node_id);
            });

            skadi.add_link_event_handler("add", (link_id, link_type, from_node_id, from_port,
                                                 to_node_id, to_port) => {
                this.add_link(link_id, link_type, from_node_id, from_port, to_node_id, to_port);
            });

            skadi.add_link_event_handler("remove", (link_id, link_type, from_node_id, from_port,
                                                    to_node_id, to_port) => {
                this.remove_link(link_id);
            });

            skadi.add_design_event_handler("clear", () => {
                this.clear();
            });

            skadi.add_design_event_handler("start_load", () => {
                console.log("start_load");
                this.pause();
            });

            skadi.add_design_event_handler("end_load", () => {
                console.log("end_load");
                this.resume();
            });

            skadi.add_design_event_handler("pause", (is_paused) => {
                if (is_paused) {
                    this.pause();
                } else {
                    this.resume();
                }
            });
        }

        this.pause();
        let network = skadi.get_network();
        let node_ids = network.get_node_list();
        node_ids.map(node_id => {
            let node = network.get_node(node_id);
            this.add_node(node_id, node.get_type().get_id());
        });

        let link_ids = network.get_link_list();
        link_ids.map(link_id => {
            let link = network.get_link(link_id);
            this.add_link(link_id,link.get_link_type(),link.get_from_node().get_id(),link.get_from_port_name(),link.get_to_node().get_id(), link.get_to_port_name());
        });
        this.resume();
    }

    /* client related */

    open_client(target_id, page_id, client_options, client_service) {
        let key = this.get_page_key(target_id,page_id);
        this.client_services[key] = client_service;
        client_service.set_message_handler((...msg) => this.forward_client_message(target_id, page_id, ...msg));
        this.send({
            "action": "open_client",
            "target_id": target_id,
            "page_id": page_id,
            "client_options": client_options
        });

    }

    close_client(target_id, page_id) {
        this.send({
            "action": "close_client",
            "target_id": target_id,
            "page_id": page_id
        });
        let key = this.get_page_key(target_id,page_id);
        if (key in this.client_services) {
            delete this.client_services[key];
        }
    }

    get_page_key(target_id, page_id) {
        return target_id + "/" + page_id;
    }

    forward_client_message(target_id, page_id, ...msg) {
        this.send({
            "action": "client_message",
            "target_id": target_id,
            "page_id": page_id
        }, ...msg);
    }

    /* handle client-related messages from the executor */

    handle_client_message(target_id, page_id, ...msg) {
        let key = this.get_page_key(target_id, page_id);
        let header = msg[0];
        switch(header["type"]) {
            case "page_message":
                this.client_services[key].send_message(...msg[1]);
                break;
            case "page_add_event_handler":
                this.client_services[key].add_event_handler(header["element_id"], header["event_type"],
                    (value) => {
                        this.send({
                            "action": "client_event",
                            "target_id": target_id,
                            "page_id": page_id,
                            "element_id": header["element_id"],
                            "event_type": header["event_type"],
                            "value": value,
                            "target_attribute": header["target_attribute"]
                        })
                    }, header["target_attribute"]);
                break;
            case "page_set_attributes":
                this.client_services[key].set_attributes(
                    header["element_id"],
                    header["attributes"]);
                break;
        }
    }

    /* handle messages from the executor */

    handle(control_packet, extras) {
        let action = control_packet["action"];
        switch(action) {
            case "client_message":
                let target_id = control_packet["target_id"];
                let page_id = control_packet["page_id"];
                this.handle_client_message(target_id, page_id, ...extras);
                break;
            case "update_node_status":
                this.skadi.set_node_status(control_packet["node_id"], control_packet["message"], control_packet["status"]);
                break;
            case "update_configuration_status":
                this.skadi.set_configuration_status(control_packet["package_id"], control_packet["message"], control_packet["status"]);
                break;
            case "update_execution_state":
                this.skadi.update_execution_state(control_packet["node_id"], control_packet["execution_state"], control_packet["is_manual"]);
                break;
            case "execution_complete":
                this.skadi.execution_complete();
                break;
            default:
                console.warn("Unhandled action from worker: " + action);
                break;
        }
    }
}


