/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024Visual Topology Ltd

     Licensed under the MIT License
*/

/* src/js/page/page.js */

var skadi = skadi || {};

skadi.Page = class {

    constructor() {
        this.message_handler = null;
        this.pending_messages = [];
        this.parent_window = window.opener || window.parent;
        this.parent_window.addEventListener("unload", (event) => {
            window.close();
        });
    }

    set_message_handler(handler) {
        this.message_handler = handler;
        for(let idx=0; idx<this.pending_messages.length; idx++) {
            let msg = this.pending_messages[idx];
            this.message_handler(...msg);
        }
        this.pending_messages = [];
    }

    handle_message(msg) {
        let header = msg[0];
        let type = header["type"];
        switch (type) {
            case "page_message":
                let message_parts = msg[1];
                if (this.message_handler) {
                    this.message_handler(...message_parts);
                } else {
                    this.pending_messages.push(message_parts);
                }
                break;
            case "page_set_attributes":
                this.set_attributes(header["element_id"], header["attributes"]);
                break;
            case "page_add_event_handler":
                this.add_event_handler(header["element_id"], header["event_type"], header["target_attribute"]);
                break;
            default:
                 console.warn("Unexpected msg received by page");
        }
    }

    set_attributes(element_id, attributes) {
        let elt = document.getElementById(element_id);
        if (elt) {
            for(let name in attributes) {
                let value = attributes[name];
                if (name === "innerHTML") {
                    elt.innerHTML = value;
                } else if (name === "value") {
                    elt.setAttribute(name, value);
                    elt.value = value;
                } else {
                    elt.setAttribute(name, value);
                }
            }
        } else {
            this.report_missing_element(element_id);
        }
    }

    add_event_handler(element_id, event_type, target_attribute) {
        let elt = document.getElementById(element_id);
        if (elt) {
            elt.addEventListener(event_type, (event) => {
                let value = event.target[target_attribute];
                this.send_to_network([{
                   "type": "event",
                   "element_id": element_id,
                   "event_type": event_type,
                   "value": value,
                   "target_attribute": target_attribute
                }]);
            })
        } else {
            this.report_missing_element(element_id);
        }
    }

    report_missing_element(element_id) {
        console.error("[SkadiUI] element "+element_id+" does not exist");
    }

    send_to_network(msg) {
        this.parent_window.postMessage(msg,window.location.origin);
    }

    send_message(...message_parts) {
        this.send_to_network([{"type":"page_message"},message_parts]);
    }
}

skadi.page = new skadi.Page();

window.addEventListener("message", (event) => skadi.page.handle_message(event.data));

