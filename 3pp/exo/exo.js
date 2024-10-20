/* js/exo-common.js */

/* MIT License

Copyright (c) 2021-2023 Visual Topology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

let colors = ["red","orange","blue","green","purple","brown","gray","pink","yellow"];
let dark_colors = colors.map((name) => "dark-"+name);
let light_colors = colors.map((name) => "light-"+name);

let all_colors = ["black","white"]+colors+dark_colors+light_colors;

let sizes = ["no","tiny","small","medium","large","huge"];

class ExoUtils {

    static createElement(tag, attrs) {
        let elt = document.createElement(tag);
        for(let name in attrs) {
            elt.setAttribute(name,attrs[name]);
        }
        return elt;
    }

    static addClasses(element, classnames) {
        classnames.forEach(classname => ExoUtils.addClass(element, classname));
    }

    static addClass(element, classname) {
        var classes = (element.getAttribute("class") || "").split(" ");
        var classnames = classname.split(" ");
        for(let idx=0; idx<classnames.length; idx++) {
            let cls = classnames[idx];
            if (classes.findIndex(name => name == cls) == -1) {
                classes.push(cls);
            }
        }
        var new_classes = classes.join(" ")
        element.setAttribute("class", new_classes);
    }

    static getClasses(element) {
        return (element.getAttribute("class") || "").split(" ").filter(name => name != "");
    }

    static removeClass(element, classname) {
        var classes = ExoUtils.getClasses(element);
        classes = classes.filter(name => name != classname);
        element.setAttribute("class", classes.join(" "));
    }

    static removeClasses(element, pattern) {
        var classes = ExoUtils.getClasses(element);
        classes.forEach(cls => {
            let resolved = cls.match(pattern);
            if (resolved != null) {
                this.removeClass(element, cls);
            }
        });
    }

    static removeClassesFromList(element, classname_list) {
        var classes = ExoUtils.getClasses(element);
        classes.forEach(cls => {
            if (classname_list.includes(cls)) {
                this.removeClass(element, cls);
            }
        });
    }

    static addStyle(element, name, value) {
        var style = element.getAttribute("style") || "";
        style = style + name + ": " + value + ";";
        element.setAttribute("style", style);
    }

    static setAttributes(element,attr_value_pairs) {
        attr_value_pairs.forEach(pair => { element.setAttribute(pair[0],pair[1])});
    }

    static moveChildNodes(from_element,to_element) {
        var to_move = [];
        for(var idx=0; idx<from_element.childNodes.length; idx++) {
            var node = from_element.childNodes[idx];
            to_move.push(node);
        }
        for(var idx=0; idx<to_move.length; idx++) {
            var node = to_move[idx];
            if (node != to_element) {
                from_element.removeChild(node);
                to_element.appendChild(node);
            }
        }
    }

    static replaceNode(old_node,new_node) {
        var parent = old_node.parentNode;
        if (!parent) {
            alert("problem here");
        }
        parent.replaceChild(new_node,old_node);
    }

    static removeAllChildren(elt) {
        while (elt.firstChild) {
            elt.removeChild(elt.firstChild);
        }
    }

    static applyColor(elt, name, value) {
        let to_remove = colors.map((color_name) => "exo-"+color_name+"-"+name);
        ExoUtils.removeClassesFromList(elt, to_remove);
        ExoUtils.addClass(elt,"exo-"+value+"-"+name);
    }

    static applySizedDimension(elt, name,value) {
        if (value == undefined) {
            return;
        }
        if (value == "") {
            value = "medium";
        }
        let to_remove = sizes.map((size_name) => "exo-"+size_name+"-"+name);
        ExoUtils.removeClassesFromList(elt, to_remove);
        if (value) {
            switch (value) {
                case "no":
                case "tiny":
                case "small":
                case "medium":
                case "large":
                case "huge":
                    ExoUtils.addClass(elt, "exo-" + value + "-" + name);
                    break;
                default:
                    console.log("Exo: Invalid " + name + " value: " + value + ", valid values are no,tiny,small,medium,large,huge");
            }
        }
    }
}

var exo_counter = 0;

class CustomExoControl extends HTMLElement {

    constructor() {
        super();
        this.value = undefined;
        this.exo_label = null;
        this.exo_tooltip_div = null;
        this.exo_tooltip_content = null;
        this.exo_br = null;
        this.exo_output = null;
        this.exo_id = "s"+exo_counter;
        this.value = undefined;
        exo_counter += 1;
        this.exo_built = false;
    }

    exoGetId() {
        return this.exo_id;
    }

    connectedCallback() {
       let parameters = this.exoGetParameters(this);
       this.exoBuild(parameters);
    }

    attributeChangedCallback(name, oldValue, newValue) {
        if (this.exo_built) {
            this.exoUpdate(name, newValue);
        }
    }

    static get observedAttributes() {
        return ["fg-color", "bg-color", "border-color", "border","margin","padding","rounded",
            "vmargin","hmargin","label","tooltip","disabled","class","aria-label", "visible"];
    }

    exoBuildCommon(tag, parameters) {

        this.exo_element = document.createElement(tag);

        if (parameters["full_width"]) {
            ExoUtils.addClass(this.exoGetInputElement(),"exo-full-width");
        }

        this.exo_root_element = document.createElement("div");
        this.exo_root_element.appendChild(this.exo_element);

        this.exo_built = true;
    }

    exoBuildComplete(parameters) {
        for(var parameter_name in parameters) {
            this.exoUpdate(parameter_name,parameters[parameter_name]);
        }
        this.appendChild(this.exoGetRootElement());
    }

    exoGetParameters() {
        let parameters = {};
        this.getAttributeNames().forEach(name => parameters[name] = this.getAttribute(name));
        return parameters;
    }

    exoUpdateParameters(parameters) {
        for(var name in parameters) {
            this.exoUpdate(name, parameters[name]);
        }
    }

    exoUpdate(name,value) {
        if (!this.exo_built) {
            return;
        }
        switch(name) {
            case "value":
                this.exoSetControlValue(value);
                break;
            case "fg-color":
                this.applyColor("fgr",value);
                break;
            case "bg-color":
                this.applyColor("bg",value);
                break;
            case "border-color":
                this.applyColor("border",value);
                break;
            case "border":
            case "margin":
            case "padding":
            case "rounded":
            case "vmargin":
            case "hmargin":
                this.applySizedDimension(name,value);
                break;
            case "bg-image":
                if (value) {
                    ExoUtils.addStyle(this.exoGetInputElement(),"background-image","url('"+value+"');");
                }
                break;
            case "full-width":
                ExoUtils.addClass(this.exoGetInputElement(),"exo-full-width");
                break;
            case "label":
                this.exoUpdateLabel(value);
                break;
            case "tooltip":
                this.exoUpdateTooltip(value);
                break;
            case "id":
                // ignore
                break;
            case "class":
                ExoUtils.addClass(this.exoGetInputElement(),value);
                break;
            case "disabled":
                switch(value) {
                    case "true":
                        this.exoGetInputElement().setAttribute("disabled", "disabled");
                        break;
                    case "false":
                        this.exoGetInputElement().removeAttribute("disabled");
                        break;
                }
                break;
            case "visible":
                switch(value) {
                    case "true":
                        ExoUtils.addStyle(this.exoGetRootElement(),"visibility", "visible");
                        break;
                    case "false":
                        ExoUtils.addStyle(this.exoGetRootElement(),"visibility", "hidden");
                        break;
                }
                break;
            case "aria-label":
                this.exoGetInputElement().setAttribute(name,value);
                break;
            default:
                console.log("Unrecognized exoUpdate: "+name+","+value);
        }
    }

    exoGetInputElement() {
        return this.exo_element;
    }

    applySizedDimension(name,value) {
        ExoUtils.applySizedDimension(this.exoGetInputElement(),name,value);
    }

    applyColor(name,value) {
        ExoUtils.applyColor(this.exoGetInputElement(),name,value);
    }

    exoDefineOutput() {
        if (!this.exo_br) {
            this.exoAddBr();
        }
        this.exo_output = document.createElement("output");
        this.exo_output.setAttribute("for",this.exoGetId());
        this.exoGetInputElement().parentElement.insertBefore(this.exo_output, this.exo_br);
    }

    exoSetOutputValue(value) {
        if (!this.exo_output) {
            this.exoDefineOutput();
        }
        ExoUtils.removeAllChildren(this.exo_output);
        this.exo_output.appendChild(document.createTextNode(value));
    }

    exoSetControlValue(value) {
        this.value = value;
    }

    exoGetControlValue() {
        return this.value;
    }

    exoGetRootElement() {
        return this.exo_root_element;
    }

    exoUpdateLabel(text) {
        if (!text) {
            if (this.exo_label) {
                this.exo_root_element.removeChild(this.exo_label);
                this.exo_label = null;
            }
            if (!this.exo_tooltip_div && !this.exo_label && !this.exo_output) {
                this.exoRemoveBr();
            }
        } else {
            this.exoAddBr();
            if (!this.exo_label) {
                this.exo_label = document.createElement("label");
                this.exo_label.setAttribute("for", this.exoGetId());
                this.exo_label.setAttribute("style", "display:inline; margin-right:5px;");
                if (this.exo_root_element.firstChild) {
                    this.exo_root_element.insertBefore(this.exo_label, this.exo_root_element.firstChild);
                } else {
                    this.exo_root_element.appendChild(this.exo_label);
                }
            }

            ExoUtils.removeAllChildren(this.exo_label);
            this.exo_label.appendChild(document.createTextNode(text ? text : "\u00A0"));
        }
    }

    exoUpdateTooltip(text) {
        if (!text) {
            if (this.exo_tooltip_div) {
                this.exo_root_element.removeChild(this.exo_tooltip_div);
                this.exo_tooltip_div = null;
                this.exo_tooltip_content = null;
            }
            if (!this.exo_tooltip_div && !this.exo_label && !this.exo_output) {
                this.exoRemoveBr();
            }
        } else {
            this.exoAddBr();
            if (!this.exo_tooltip_div) {
                this.exo_tooltip_div = document.createElement("div");
                this.exo_tooltip_div.setAttribute("tabindex", "0");
                this.exo_tooltip_div.setAttribute("class", "exo-icon exo-icon-help exo-icon-inline exo-help-tooltip");
                this.exo_tooltip_content = document.createElement("div");
                this.exo_tooltip_content.setAttribute("class", "exo-help-content exo-white-bg exo-border");
                this.exo_tooltip_div.appendChild(this.exo_tooltip_content);
                if (this.exo_output) {
                    this.exo_root_element.insertBefore(this.exo_tooltip_div, this.exo_output);
                } else {
                    this.exo_root_element.insertBefore(this.exo_tooltip_div, this.exo_br);
                }
            }
            ExoUtils.removeAllChildren(this.exo_tooltip_content);
            this.exo_tooltip_content.appendChild(document.createTextNode(text));
        }

    }

    exoAddBr() {
        if (!this.exo_br) {
            this.exo_br = document.createElement("br");
            if (this.exo_root_element.firstChild) {
                this.exo_root_element.insertBefore(this.exo_br,this.exo_root_element.firstChild);
            } else {
                this.exo_root_element.appendChild(this.exo_br);
            }
        }
    }

    exoRemoveBr() {
        if (this.exo_br) {
            this.exo_root_element.removeChild(this.exo_br);
            this.exo_br = null;
        }
    }
}





/* js/controls/exo-button.js */



class CustomExoButton extends CustomExoControl {
    constructor() {
        super();
    }

    exoBuild(parameters) {
        super.exoBuildCommon("input", parameters);
        this.exoGetInputElement().setAttribute("type","button");
        if (parameters["text"]) {
            this.exoGetInputElement().setAttribute("value", parameters["text"]);
        }
        if (parameters["icon"]) {
            ExoUtils.addClass(this.exoGetInputElement(),"exo-icon exo-icon-" + parameters["icon"]);
        }
        super.exoBuildComplete(parameters);
    }

    exoUpdate(name,value) {
        switch(name) {
            case "text":
                if (this.exoGetInputElement()) {
                    this.exoGetInputElement().setAttribute("value", value);
                }
                break;
            default:
                super.exoUpdate(name,value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('text');
        return attrs;
    }
}

customElements.define("exo-button", CustomExoButton);



/* js/controls/exo-checkbox.js */


class CustomExoCheckbox extends CustomExoControl {
    constructor() {
        super();
    }

    exoBuild(parameters) {
        super.exoBuildCommon("input", parameters);

        this.exoGetInputElement().setAttribute("type","checkbox");
        this.exoGetInputElement().addEventListener("change", evt => {
            let v = evt.target.checked;
            this.exoSetControlValue(v?"true":"false");
            this.checked = v;
            this.dispatchEvent(new CustomEvent("change"));
            evt.stopPropagation();
        });

        super.exoBuildComplete(parameters);
    }

    exoUpdate(name,value) {
        switch(name) {
            case "value":
                const bvalue = (value == "true") ? true : false;
                this.exoGetInputElement().checked = bvalue;
                this.exoSetControlValue(value);
                this.checked = bvalue;
                break;
            default:
                super.exoUpdate(name,value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('value');
        return attrs;
    }
}

customElements.define("exo-checkbox", CustomExoCheckbox);


/* js/controls/exo-date.js */


class CustomExoDateTimeBase extends CustomExoControl {

    constructor() {
        super();
    }

    exoBuild(parameters,input_type) {
        super.exoBuildCommon("input", parameters);

        this.exoGetInputElement().setAttribute("type",input_type);

        super.exoBuildComplete(parameters);
    }

    exoUpdate(name,value) {
        switch(name) {
            case "value":
                this.exoGetInputElement().value = value;
                this.exoSetControlValue(value);
                break;
            case "min":
                this.exoGetInputElement().setAttribute("min", value);
                break;
            case "max":
                this.exoGetInputElement().setAttribute("max", value);
                break;
            default:
                super.exoUpdate(name,value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('value','min','max');
        return attrs;
    }
}

class CustomExoDate extends CustomExoDateTimeBase {

    constructor() {
        super();
    }

    exoBuild(parameters, type) {
        super.exoBuild(parameters,"date");
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('value','min','max');
        return attrs;
    }
}

class CustomExoTime extends CustomExoDateTimeBase {

    constructor() {
        super();
    }

    exoBuild(parameters) {
        super.exoBuild(parameters, "time");
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('value','min','max');
        return attrs;
    }
}

class CustomExoDateTimeLocal extends CustomExoDateTimeBase {

    constructor() {
        super();
    }

    exoBuild(parameters) {
        super.exoBuild(parameters, "datetime-local");
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('value','min','max');
        return attrs;
    }
}

customElements.define("exo-date", CustomExoDate);
customElements.define("exo-time", CustomExoTime);
customElements.define("exo-datetime-local", CustomExoDateTimeLocal);




/* js/controls/exo-file.js */


class CustomExoFile extends CustomExoControl {

    constructor() {
        super();
        this.filename_span = null;
    }

    exoBuild(parameters) {
        super.exoBuildCommon("input", parameters);
        this.exoGetInputElement().setAttribute("type","file");
        this.exoGetInputElement().setAttribute("id","file_test");

        ExoUtils.addStyle(this.exoGetInputElement(),"display","none");
        this.label = document.createElement("label");
        this.label.setAttribute("class","exo-button");
        this.label.setAttribute("for","file_test");

        this.filename_span = document.createElement("span");
        this.filename_span.setAttribute("style","margin-left:10px;")
        this.filename_span.appendChild(document.createTextNode("filename.txt"));

        this.exoGetRootElement().appendChild(this.label);
        this.exoGetRootElement().appendChild(this.filename_span);

        var that = this;

        this.exoGetInputElement().oninput = function (evt) {
            const filelist = that.exoGetInputElement().files;
            var files = {};
            for(var idx=0; idx<filelist.length;idx++) {
                var file = filelist[idx];
                files[file.name] = file;
                ExoUtils.removeAllChildren(that.filename_span);
                that.filename_span.appendChild(document.createTextNode(file.name));
            }
            that.dispatchEvent(new CustomEvent("exo-file-changed",{detail:files}));
        }
        super.exoBuildComplete(parameters);
    }

    exoUpdate(name,value) {
        switch(name) {
            case "button-text":
                 ExoUtils.removeAllChildren(this.label);
                 this.label.appendChild(document.createTextNode(value));
                 break;
            case "filename":
                ExoUtils.removeAllChildren(this.filename_span);
                this.filename_span.appendChild(document.createTextNode(value));
                break;
            default:
                super.exoUpdate(name,value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push("filename","button-text");
        return attrs;
    }
/*
    async upload(filelist, callback) {
        var uploaded = {};
        for (var idx = 0; idx < filelist.length; idx++) {
            await this.upload_file(filelist[idx], uploaded);
        }
        callback(uploaded);
    }

    async upload_file(file, uploaded) {
        var that = this;
        var ab = await file.arrayBuffer();
        var dec = new TextDecoder();
        uploaded[file.name] = dec.decode(ab);
    }
*/
}

customElements.define("exo-file", CustomExoFile);


/* js/controls/exo-number.js */


class CustomExoNumber extends CustomExoControl {

    constructor() {
        super();
    }

    exoBuild(parameters) {
        super.exoBuildCommon("input", parameters);

        this.exoGetInputElement().setAttribute("type","number");

         this.exoGetInputElement().addEventListener("change", evt => {
            let v = evt.target.value;
            this.exoSetControlValue(v);
            this.dispatchEvent(new CustomEvent("change"));
            evt.stopPropagation();
        });

        super.exoBuildComplete(parameters);
    }

    exoUpdate(name,value) {
        switch(name) {
            case "min":
                this.exoGetInputElement().setAttribute("min", value);
                break;
            case "max":
                this.exoGetInputElement().setAttribute("max", value);
                break;
            case "step":
                this.exoGetInputElement().setAttribute("step", value);
                break;
            case "value":
                this.exoGetInputElement().value = value;
                this.exoSetControlValue(value);
                break;
            default:
                super.exoUpdate(name,value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('value','min','max','step');
        return attrs;
    }
}

customElements.define("exo-number", CustomExoNumber);


/* js/controls/exo-radio.js */


class CustomExoRadio extends CustomExoControl {
    constructor() {
        super();
        this.exo_button_count = 0;
        this.exo_button_map = {};
        this.exo_radio_name = "";
    }

    exoBuild(parameters) {
        super.exoBuildCommon("div", parameters);
        this.exo_radio_name = this.exoGetId()+"_rbg";
        this.appendChild(this.exoGetRootElement());
        super.exoBuildComplete(parameters);
    }

    exoUpdate(name, value) {
        switch (name) {
            case "value":
                this.exoSetControlValue(value);
                this.exoSetButtonStates();
                this.exoGetInputElement().value = value;
                break;
            case "options":
                let options = JSON.parse(value);
                this.exoClearRadioButtons();
                options.map((item) => this.exoAddRadioButton(item[0],item[1]));
                this.exoSetButtonStates(); // update the buttons
                break;
            default:
                super.exoUpdate(name, value);
        }
    }

    exoSetButtonStates() {
        var value = this.exoGetControlValue();
        for(var v in this.exo_button_map) {
            var btn = this.exo_button_map[v];
            if (value == v) {
                btn.checked = true;
            } else {
                btn.checked = false;
            }
        }
    }


    exoClearRadioButtons() {
        ExoUtils.removeAllChildren(this.exoGetInputElement());
        this.exo_button_map = {};
        this.exo_button_count = 0;
    }

    exoAddRadioButton(value,label_text) {

        var btn_id = this.exoGetId()+"_b"+this.exo_button_count;
        this.exo_button_count += 1;

        var label = document.createElement("label");
        ExoUtils.addStyle(label,"display","inline-block");
        label.setAttribute("for",btn_id);
        var span = document.createElement("span");
        ExoUtils.addClass(span,"label-body");
        span.appendChild(document.createTextNode(label_text));
        label.appendChild(span);
        var input = document.createElement("input");
        input.setAttribute("id",btn_id);
        input.setAttribute("type","radio");
        input.setAttribute("name", this.exo_radio_name);
        input.setAttribute("value",value);

        this.exo_button_map[value] = input;

        this.exoGetInputElement().appendChild(label);
        this.exoGetInputElement().appendChild(input);

        input.addEventListener("input", evt => {
            this.exoSetControlValue(value);
            evt.stopPropagation();
        });
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('options','value');
        return attrs;
    }


}

customElements.define("exo-radio", CustomExoRadio);

/* js/controls/exo-range.js */




class CustomExoRange extends CustomExoControl {

    constructor() {
        super();
    }

    exoBuild(parameters) {
        super.exoBuildCommon("input", parameters);

        this.exoGetInputElement().setAttribute("type","range");
        this.exoGetInputElement().setAttribute("style","display:inline;");

        this.exoGetInputElement().setAttribute("min", parameters["min"]);
        this.exoGetInputElement().setAttribute("max", parameters["max"]);

        if ("step" in parameters) {
            this.exoGetInputElement().setAttribute("step", parameters["step"]);
        }
        this.exoGetInputElement().value = parameters["value"];

        this.exoGetInputElement().addEventListener("change", evt => {
            let v = evt.target.value;
            this.exoSetControlValue(v);
            this.exoSetOutputValue(v);
            this.dispatchEvent(new CustomEvent("change"));
            evt.stopPropagation();
        });

        let elt = this.exoGetInputElement();
        elt.parentNode.insertBefore(document.createTextNode(parameters["min"]),elt);
        elt.parentNode.insertBefore(document.createTextNode(parameters["max"]),elt.nextSibling);

        this.exoSetOutputValue(parameters["value"]);

        super.exoBuildComplete(parameters);
    }

    exoUpdate(name,value) {
        switch(name) {
            case "value":
                this.exoGetInputElement().value = value;
                this.exoSetControlValue(value);
                this.exoSetOutputValue(value);
                break;
            case "min":
                this.exoGetInputElement().setAttribute("min", value);
                break;
            case "max":
                this.exoGetInputElement().setAttribute("max", value);
                break;
            case "step":
                this.exoGetInputElement().setAttribute("step", value);
                break;
            default:
                super.exoUpdate(name,value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('value','min','max','step');
        return attrs;
    }
}

customElements.define("exo-range", CustomExoRange);


/* js/controls/exo-select.js */



class CustomExoSelect extends CustomExoControl {

    constructor() {
        super();
        this.exo_value_label_map = {};
        this.exo_is_multiple = false;
    }

    exoBuild(parameters) {

        super.exoBuildCommon("select", parameters);
        this.exoGetInputElement().setAttribute("class", "select");

        this.exoGetInputElement().addEventListener("change", evt => {
            let v = undefined;
            if (this.exo_is_multiple) {
                var selected_values = [];
                var options = this.exoGetInputElement().querySelectorAll("option");
                for (var i=0; i<options.length; i++) {
                    let opt = options[i];
                    if (opt.selected) {
                        selected_values.push(opt.value);
                    }
                }
                v = JSON.stringify(selected_values);
            } else {
                v = this.exoGetInputElement().value;
            }
            this.exoSetControlValue(v);
            this.dispatchEvent(new CustomEvent("change"));
            evt.stopPropagation();
        });
        this.appendChild(this.exoGetRootElement());

        if ("multiple" in parameters) {
            this.exo_is_multiple = true;
            delete parameters["multiple"];
            this.exoGetInputElement().setAttribute("multiple","multiple");
        }
        if ("options" in parameters) {
            this.exoSetOptions(parameters["options"]);
            delete parameters["options"];
        }
        super.exoBuildComplete(parameters);
    }

    exoUpdate(name, value) {
        switch (name) {
            case "value":
                if (this.exo_is_multiple) {
                    let value_arr = [];
                    if (value) {
                        value_arr = JSON.parse(value);
                    }
                    let options = this.exoGetInputElement().querySelectorAll("option");
                    for (var i=0; i<options.length; i++) {
                        let opt = options[i];
                        if (value_arr.includes(opt.value)) {
                            opt.selected = true;
                        } else {
                            opt.selected = false;
                        }
                    }
                    this.exoSetControlValue(value);
                } else {
                    this.exoGetInputElement().value = value;
                    this.exoSetControlValue(value);
                }
                break;
            case "size":
                this.exoGetInputElement().setAttribute("size",value);
                break;
            case "options":
                this.exoSetOptions(value);
                break;
            default:
                super.exoUpdate(name, value);
        }
    }

    exoSetOptions(value) {
        let options = JSON.parse(value);
        let current_value = this.exoGetControlValue();
        this.exoClearOptions();
        options.map((item) => this.exoAddOption(item[0],item[1]));
        let updated_value = undefined;
        if (!this.exo_is_multiple) {
            if (current_value in this.exo_value_label_map) {
                updated_value = current_value;
            }
        } else {
            if (current_value) {
                let current_values = JSON.parse(current_value);
                let updated_values = [];
                current_values.forEach(value => {
                    if (value in this.exo_value_label_map) {
                        updated_values.push(value);
                    }
                });
                updated_value = JSON.stringify(updated_values);
            }
        }
        this.exoUpdate("value",updated_value);
    }

    exoClearOptions() {
        if (this.exoGetInputElement()) {
            ExoUtils.removeAllChildren(this.exoGetInputElement());
        }
        this.exo_value_label_map = {};
    }

    exoAddOption(value,label,disabled) {
        var option = document.createElement("option");
        option.appendChild(document.createTextNode(label));
        option.setAttribute("value", value);
        if (disabled) {
            option.setAttribute("disabled", "disabled");
        } else {
            this.exo_value_label_map[value] = label;
        }
        this.exoGetInputElement().appendChild(option);
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('options','multiple','value');
        return attrs;
    }

}

CustomExoSelect.INVALID_ERROR = "Invalid";

customElements.define("exo-select", CustomExoSelect);

/* js/controls/exo-text.js */


class CustomExoText extends CustomExoControl {

    constructor() {
        super();
    }

    exoBuild(parameters) {
        super.exoBuildCommon("input", parameters);

        this.exoGetInputElement().setAttribute("type","text");
        this.exoGetInputElement().value = parameters["value"] || "";

        this.exoGetInputElement().addEventListener("change", evt => {
            let v = evt.target.value;
            this.exoSetControlValue(v);
            this.dispatchEvent(new CustomEvent("change"));
            evt.stopPropagation();
        });

        super.exoBuildComplete(parameters);
    }

    exoUpdate(name,value) {
        switch(name) {
            case "value":
                this.exoGetInputElement().value = value;
                this.exoSetControlValue(value);
                break;
            default:
                super.exoUpdate(name,value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('value');
        return attrs;
    }
}

customElements.define("exo-text", CustomExoText);


/* js/controls/exo-textarea.js */


class CustomExoTextArea extends CustomExoControl {

    constructor() {
        super();
    }

    exoBuild(parameters) {
        super.exoBuildCommon("textarea", parameters);

        var that = this;

        this.exoGetInputElement().addEventListener("change", evt => {
            let v = evt.target.value;
            this.exoSetControlValue(v);
            this.dispatchEvent(new CustomEvent("change"));
            evt.stopPropagation();
        });

        super.exoBuildComplete(parameters);
    }

    exoUpdate(name,value) {
        switch(name) {
            case "value":
                this.exoGetInputElement().value = value;
                this.exoSetControlValue(value);
                break;
            case "rows":
            case "cols":
                this.exoGetInputElement().setAttribute(name,value);
                break;
            default:
                super.exoUpdate(name,value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('value','rows','cols');
        return attrs;
    }
}

customElements.define("exo-textarea", CustomExoTextArea);


/* js/controls/exo-toggle.js */


class CustomExoToggle extends CustomExoControl {
    constructor() {
        super();
    }

    exoBuild(parameters) {
        super.exoBuildCommon("input", parameters);

        var element = this.exoGetInputElement();
        element.setAttribute("type","checkbox");

        var label = document.createElement("label");
        label.setAttribute("class","exo-toggle");
        var parent = element.parentElement;
        parent.removeChild(element);
        var span = document.createElement("span");
        span.setAttribute("class","exo-toggle-slide");
        label.appendChild(element);
        span.appendChild(document.createTextNode(""));
        label.appendChild(span);
        parent.appendChild(label);

        this.label = label;
        this.tt_span = null;
        this.ft_span = null;

        this.slider_span = span;

        this.exoGetInputElement().addEventListener("change", evt => {
            let v = evt.target.checked;
            this.exoSetControlValue(v?"true":"false");
            this.checked = v;
            this.dispatchEvent(new CustomEvent("change"));
            evt.stopPropagation();
        });

        super.exoBuildComplete(parameters);
    }

    exoUpdate(name,value) {
        switch(name) {
            case "value":
                const bvalue = (value == "true") ? true : false;
                this.exoGetInputElement().checked = bvalue;
                this.exoSetControlValue(value);
                this.checked = bvalue;
                break;
            case "true-text":
                if (!this.tt_span) {
                    this.tt_span = document.createElement("span");
                    this.tt_span.setAttribute("class", "exo-toggle-true");
                    this.label.appendChild(this.tt_span);
                }
                ExoUtils.removeAllChildren(this.tt_span);
                if (value) {
                    this.tt_span.appendChild(document.createTextNode(value));
                }
                break;
            case "false-text":
                 if (!this.ft_span) {
                    this.ft_span = document.createElement("span");
                    this.ft_span.setAttribute("class", "exo-toggle-false");
                    this.label.appendChild(this.ft_span);
                }
                ExoUtils.removeAllChildren(this.ft_span);
                if (value) {
                    this.ft_span.appendChild(document.createTextNode(value));
                }
                break;
            case "fg-color":
                ExoUtils.removeClasses( this.slider_span, /(exo-)(.*)(-fg)/);
                ExoUtils.addClass(this.slider_span,"exo-"+value+"-fg");
                break;
            case "bg-color":
                ExoUtils.removeClasses(this.slider_span, /(exo-)(.*)(-bg)/);
                ExoUtils.addClass(this.slider_span,"exo-"+value+"-bg");
                break;
            default:
                super.exoUpdate(name,value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('value');
        attrs.push('true_text');
        attrs.push('false_text');
        return attrs;
    }
}

customElements.define("exo-toggle", CustomExoToggle);


/* js/controls/exo-download.js */


class CustomExoDownload extends CustomExoControl {

    constructor() {
        super();
        this.exo_download_content = null;
        this.exo_download_mimetype = null;
        this.exo_download_href = null;
        this.exo_download_filename = null;
        this.exo_a = null;
    }

    exoBuild(parameters) {
        super.exoBuildCommon("div", parameters);
        this.exoGetInputElement().setAttribute("class","exo-button");

        this.exo_a = document.createElement("a");
        this.exo_a.setAttribute("href","");
        this.exoGetInputElement().appendChild(this.exo_a);
        var that = this;

        this.exo_a.onclick = function (evt) {
            that.dispatchEvent(new CustomEvent("exo-download",{"detail":{}}));
            that.exoSetHref();
        }
        super.exoBuildComplete(parameters);
    }

    exoSetHref() {
        if (!this.exo_download_href) {
            var data_uri = "data:" + this.exo_download_mimetype + ";base64," + btoa(this.exo_download_content);
            this.exo_a.setAttribute("href", data_uri);
        }
    }

    exoUpdate(name,value) {
        switch(name) {
            case "download-filename":
                this.exo_download_filename = value;
                this.exo_a.setAttribute("download", value);
                ExoUtils.removeAllChildren(this.exo_a);
                this.exo_a.appendChild(document.createTextNode(value));
                break;
            case "download-mimetype":
            case "download-content":
                if (name == "download-mimetype") {
                    this.exo_download_mimetype = value;
                } else {
                    this.exo_download_content = value;
                }
                if (this.exo_download_mimetype && this.exo_download_content) {
                    this.exo_download_href = "";
                    this.exo_a.setAttribute("href", this.exo_download_href);
                }
                break;
            case "href":
                this.exo_download_href = value;
                this.exo_a.setAttribute("href", this.exo_download_href);
                break;
            case "fg-color":
                // apply the color to the anchor to override the default link style
                ExoUtils.removeClasses( this.exo_a, /exo-(.*)-fg/);
                ExoUtils.addClass(this.exo_a,"exo-"+value+"-fg");
                super.exoUpdate(name,value);
                break;
            default:
                super.exoUpdate(name,value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push("download-filename");
        attrs.push("download-content");
        attrs.push("download-mimetype");
        attrs.push("href");
        return attrs;
    }

}

customElements.define("exo-download", CustomExoDownload);

/* js/composite-controls/exo-merge-lists.js */


class CustomExoMergeLists extends CustomExoControl {

    constructor() {
        super();
        this.merged = [];
        this.exo_key_map = {};
        this.options1 = null;
        this.options2 = null;
        this.addb = null;
        this.subb = null;
        this.sel1 = null;
        this.sel2 = null;
        this.sel3 = null;
        const value_s = JSON.stringify(this.merged);
        this.exoSetControlValue(value_s);
    }

    exoBuild(parameters) {
        super.exoBuildCommon("div", parameters);

        let r = document.createElement("div");
        r.setAttribute("class","exo-row");
        let cell1 = document.createElement("div");
        cell1.setAttribute("class","exo-cell");
        this.sel1 = document.createElement("exo-select");
        this.sel2 = document.createElement("exo-select");

        this.sel1.addEventListener("change", (evt) => this.exoUpdateButtons());
        this.sel2.addEventListener("change", (evt) => this.exoUpdateButtons());

        let cell2 = document.createElement("div");
        cell2.setAttribute("class","exo-cell exo-cell-centered");
        this.addb = document.createElement("exo-button");
        this.addb.setAttribute("icon","arrow-right");
        this.addb.setAttribute("disabled","true");
        this.addb.setAttribute("aria-label","Add Combination");
        this.addb.addEventListener("click", (evt) => {
            const v1 = this.sel1.value;
            const v2 = this.sel2.value;
            if (v1 && v2) {
                this.sel1.exoSetControlValue(null);
                this.sel2.exoSetControlValue(null);
                this.exoAddMerge(v1, v2);
                this.exoUpdateMerged();
                this.exoUpdateButtons();
                this.exoDispatch();
            }
        });
        this.subb = document.createElement("exo-button");
        this.subb.setAttribute("icon","arrow-left");
        this.subb.setAttribute("disabled","true");
        this.subb.setAttribute("aria-label","Remove Combination");
        this.subb.addEventListener("click", (evt) => {
            const k = this.sel3.value;
            const v = this.exo_key_map[k];
            this.exoRemoveMerge(v[0],v[1]);
            delete this.exo_key_map[k];
            this.exoUpdateMerged();
            this.exoUpdateButtons();
            this.exoDispatch();
        });
        cell2.appendChild(this.addb);
        cell2.appendChild(this.subb);

        let cell3 = document.createElement("div");
        cell3.setAttribute("class","exo-cell");
        this.sel3 = document.createElement("exo-select");
        this.sel3.addEventListener("change", (evt) => this.exoUpdateButtons());
        cell3.appendChild(this.sel3);

        r.appendChild(cell1);
        r.appendChild(cell2);
        r.appendChild(cell3);
        cell1.appendChild(this.sel1);
        cell1.appendChild(this.sel2);
        this.exoGetInputElement().appendChild(r);
        super.exoBuildComplete(parameters);
    }

    exoAddMerge(v1,v2) {
        for(let idx=0; idx<this.merged.length; idx++) {
            if (this.merged[idx][0] == v1 && this.merged[idx][1] == v2) {
                return;
            }
        }
        this.merged.push([v1,v2]);
    }

    exoRemoveMerge(v1,v2) {
        this.merged = this.merged.filter((item) => item[0] != v1 || item[1] != v2);
    }

    exoUpdateMerged() {
        this.sel3.exoClearOptions();
        const options = [];
        this.exo_key_map = {};
        for (let idx = 0; idx < this.merged.length; idx++) {
            let v1 = this.merged[idx][0];
            let v2 = this.merged[idx][1];
            let key = v1 + "+" + v2;
            let label = this.options1[v1] + "+" + this.options2[v2];
            this.exo_key_map[key] = [v1, v2];
            options.push([key, label]);
        }
        const options_s = JSON.stringify(options);
        const value_s = JSON.stringify(this.merged);
        this.sel3.setAttribute("options", options_s);
        this.exoSetControlValue(value_s);
    }

    exoUpdateButtons() {
        if (this.sel3.value) {
            this.subb.setAttribute("disabled","false");
        } else {
            this.subb.setAttribute("disabled","true");
        }
        if (this.sel1.value && this.sel2.value) {
            this.addb.setAttribute("disabled","false");
        } else {
            this.addb.setAttribute("disabled","true");
        }
    }

    exoDispatch() {
        const value_s = JSON.stringify(this.merged);
        this.dispatchEvent(new CustomEvent("exo-value", {detail: value_s}));
    }

    exoUpdate(name, value) {
        switch (name) {
            case "value":
                this.merged = JSON.parse(value);
                if (this.options1  && this.options2) {
                    this.exoUpdateMerged();
                }
                break;
            case "options1":
                this.sel1.setAttribute("options",value);
                this.options1 = {};
                JSON.parse(value).map((item) => this.options1[item[0]] = item[1]);
                if (this.options2) {
                    this.exoUpdateMerged();
                }
                break;
            case "options2":
                this.sel2.setAttribute("options",value);
                this.options2 = {};
                JSON.parse(value).map((item) => this.options2[item[0]] = item[1]);
                if (this.options1) {
                    this.exoUpdateMerged();
                }
                break;
            case "size":
                this.sel1.setAttribute("size",value);
                this.sel2.setAttribute("size",value);
                this.sel3.setAttribute("size",""+2*Number.parseInt(value));
                break;
            default:
                super.exoUpdate(name, value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('options1','options2','value');
        return attrs;
    }


}

customElements.define("exo-merge-lists", CustomExoMergeLists);

/* js/composite-controls/exo-table-control.js */


/*
    The value of this control is a data structure as below
    {
        "columns": [{
               "name": "col1"
               "label": "Column 1"
               "type": "string" | "boolean"
               "editable": true|false
            }],
        "rows": [
            {
               "col1": "aaa"
            }
        ]
    }
 */

class CustomExoTableControl extends CustomExoControl {

    constructor() {
        super();
        this.exo_tbl_elt = null;
        this.exo_table_definition = {};
        this.exo_column_names = [];
        this.exo_columns_by_name = {};
        const value_s = JSON.stringify(this.exo_table_definition);
        this.exoSetControlValue(value_s);
    }

    exoBuild(parameters) {
        super.exoBuildCommon("table", parameters);
        this.exo_tbl_elt = this.exoGetInputElement();
        ExoUtils.addClass(this.exo_tbl_elt,"exo-border");
        super.exoBuildComplete(parameters);
    }

    exoRefresh() {
        ExoUtils.removeAllChildren(this.exo_tbl_elt);
        this.exo_visible_column_names = [];
        if (this.exo_table_definition.columns) {
            let tr_elt = document.createElement("tr");
            this.exo_table_definition.columns.map((column_definition) => {
                let th_elt = document.createElement("th");
                th_elt.appendChild(document.createTextNode(column_definition.label));
                tr_elt.appendChild(th_elt);
                this.exo_column_names.push(column_definition.name);
                this.exo_columns_by_name[column_definition.name] = column_definition;
            });
            this.exo_tbl_elt.appendChild(tr_elt);
        }
        if (this.exo_table_definition.rows) {

            this.exo_table_definition.rows.map((row,row_index) => {
                let tr_elt = document.createElement("tr");
                this.exo_column_names.map((name) => {
                    let td_elt = document.createElement("td");
                    let col_def = this.exo_columns_by_name[name];
                    let value = row[name];
                    if (col_def.type == "boolean") {
                        let ctrl = document.createElement("exo-checkbox");
                        ctrl.setAttribute("value",""+value);
                        if (!col_def.editable) {
                            ctrl.setAttribute("disabled","true");
                        } else {
                            ctrl.addEventListener("change", (evt) => {
                                this.exoUpdateCell(name, row_index, evt.target.checked);
                            });
                        }
                        td_elt.appendChild(ctrl);
                    } else {
                        if (!col_def.editable) {
                            td_elt.appendChild(document.createTextNode("" + value));
                        } else {

                            let enable = document.createElement("exo-checkbox");


                            enable.setAttribute("class", "exo-inline");

                            td_elt.appendChild(enable);

                            let ctrl = document.createElement("exo-text");

                            ctrl.addEventListener("change", (evt) => {
                                this.exoUpdateCell(name, row_index, evt.target.value);
                            });

                            if (value == undefined) {
                                enable.setAttribute("value", "false");
                                value = "";
                                ctrl.setAttribute("visible","false");
                            } else {
                                enable.setAttribute("value", "true");
                            }

                            ctrl.setAttribute("value", value);

                            enable.addEventListener("change", (evt) => {
                                ctrl.setAttribute("visible", evt.target.checked ? "true" : "false");
                                if (!evt.target.checked) {
                                    this.exoUpdateCell(name, row_index, undefined);
                                } else {
                                    let v = ctrl.exoGetInputElement().value;
                                    this.exoUpdateCell(name, row_index, v);
                                }
                            });

                            td_elt.appendChild(ctrl);
                        }
                    }
                    tr_elt.appendChild(td_elt);
                });
                this.exo_tbl_elt.appendChild(tr_elt);
            });
        }
    }

    exoUpdateCell(column_name, row_index, new_value) {
        if (new_value !== undefined) {
            this.exo_table_definition.rows[row_index][column_name] = new_value;
        } else {
            delete this.exo_table_definition.rows[row_index][column_name];
        }
        const value_s = JSON.stringify(this.exo_table_definition);
        this.exoSetControlValue(value_s);
        this.dispatchEvent(new CustomEvent("exo-value", {detail: value_s}));
    }

    exoUpdate(name, value) {
        switch (name) {
            case "value":
                this.exo_table_definition = JSON.parse(value);
                this.exoRefresh();
                break;
            default:
                super.exoUpdate(name, value);
        }
    }

    static get observedAttributes() {
        var attrs = CustomExoControl.observedAttributes;
        attrs.push('value');
        return attrs;
    }

}

customElements.define("exo-table-control", CustomExoTableControl);

/* js/layouts/tree.js */


class ExoTree extends HTMLDivElement {
    constructor() {
      super();
      this.connected = false;
    }

    connectedCallback() {
        if (this.connected) {
            return;
        }
        this.connected = true;
        let p_elt = this.parentElement;
        if (p_elt.exo_ul == undefined) {

            // p_elt.exo_ul = ExoUtils.createElement("ul",{"class":"exo-tree"});
            p_elt.exo_ul = ExoUtils.createElement("ul",{});
            p_elt.appendChild(p_elt.exo_ul);
        }
        let li = ExoUtils.createElement("li",{"role":"treeitem"});
        let inp = ExoUtils.createElement("input",{"type":"checkbox","aria-hidden":"true"});
        this.label = ExoUtils.createElement("label");
        this.label.appendChild(document.createTextNode(this.getAttribute("label")));
        li.appendChild(inp);
        li.appendChild(this.label);
        p_elt.exo_ul.appendChild(li);
        li.appendChild(this);
        this.set
    }

    attributeChangedCallback(name, oldValue, newValue) {
        if (name == "label" && this.label) {
            ExoUtils.removeAllChildren(this.label);
            this.label.appendChild(document.createTextNode(newValue));
        }
    }

    static get observedAttributes() {
        return ["label"];
    }
}

customElements.define(
  "exo-tree",
  ExoTree,
    {"extends":"div"}
);



/* js/layouts/tabs.js */


class ExoTab extends HTMLElement {
    constructor() {
      super();
      this.connected = false;
    }

    connectedCallback() {
        if (this.connected) {
            return;
        }
        this.connected = true;
        let p_elt = this.parentElement;
        if (p_elt.exo_tab_count == undefined) {
            p_elt.exo_tab_count = 0;
            p_elt.exo_tabs = [];
        }
        let parent_id = p_elt.getAttribute("id");
        let group_id = parent_id+"-tab-group";
        let tab_content_id = parent_id+"-tab-content";
        let cb_id = parent_id+"-cb";
        let first = false;
        let content_elt = null;
        if (p_elt.exo_tab_count == 0) {

            let cb_elt = ExoUtils.createElement("input",{"aria-hidden":"true", "type":"checkbox", "id":cb_id});
            let cb_label_open = ExoUtils.createElement("label",{
                "aria-hidden":"true",
                "class":"exo-tabs-open exo-button exo-icon exo-icon-medium exo-icon-menu",
                "for":cb_id});
            let cb_label_close = ExoUtils.createElement("label",{
                "aria-hidden":"true",
                "class":"exo-tabs-close exo-button exo-rounded exo-icon exo-icon-medium exo-icon-clear",
                "for":cb_id});

            let break_elt = document.createElement("br");
            content_elt = document.createElement("div");
            content_elt.setAttribute("class","exo-tabs-content");
            content_elt.setAttribute("id", tab_content_id);
            p_elt.insertBefore(content_elt,p_elt.firstElementChild);
            p_elt.insertBefore(break_elt,p_elt.firstElementChild);
            p_elt.insertBefore(cb_label_close,p_elt.firstElementChild);
            p_elt.insertBefore(cb_label_open,p_elt.firstElementChild);
            p_elt.insertBefore(cb_elt,p_elt.firstElementChild);
            first = true;
        } else {
            content_elt = document.getElementById(tab_content_id);
        }
        let content_id = parent_id + "-tab-"+p_elt.exo_tab_count;

        this.label = ExoUtils.createElement("label",{
            "for":content_id,
            "aria-hidden":"true",
            "class":"exo-tabs-item exo-white-bg"});
        this.label.appendChild(document.createTextNode(this.getAttribute("label")));

        let input = ExoUtils.createElement("input", {
            "type":"radio","name":group_id,"id":content_id
        });
        if (first) {
            input.setAttribute("checked","checked");
        }
        p_elt.insertBefore(input, content_elt);
        p_elt.insertBefore(this.label, content_elt);
        p_elt.exo_tabs.push(this);
        p_elt.exo_tab_count += 1;
        document.getElementById(tab_content_id).appendChild(this);
    }

    attributeChangedCallback(name, oldValue, newValue) {
        if (name == "label" && this.label) {
            ExoUtils.removeAllChildren(this.label);
            this.label.appendChild(document.createTextNode(newValue));
        }
    }

    static get observedAttributes() {
        return ["label"];
    }
}

customElements.define(
  "exo-tab",
  ExoTab
);

/* js/layouts/modal.js */


var exo_modal_counter = 0;

class ExoModal extends HTMLDivElement {
    constructor() {
      super();
      this.connected = false;
    }

    connectedCallback() {
        if (this.connected) {
            return;
        }
        this.connected = true;
        this.modal_id = "exo_modal_id"+exo_modal_counter;
        exo_modal_counter += 1;
        this.setAttribute("class","exo-modal-window-content");
        var w = ExoUtils.createElement("div",{"class":"exo-modal-window"});
        var m = ExoUtils.createElement("div", {"class":"exo-modal"});
        this.checkbox = ExoUtils.createElement("input",{"type":"checkbox","id":this.modal_id});
        if (this.hasAttribute("display")) {
            this.checkbox.checked = (this.getAttribute("display") == "true") ? true : false;
        }
        this.label = ExoUtils.createElement("label",{"for":this.modal_id, "style":"display:none;"});
        this.open_button = ExoUtils.createElement("label",{"class":"exo-button","for":this.modal_id, "style":"display:none;"});
        m.appendChild(this.label);
        m.appendChild(this.checkbox);
        m.appendChild(this.open_button);
        m.appendChild(w);
        var cl = ExoUtils.createElement("label",{"for":this.modal_id});
        this.close_button = ExoUtils.createElement("span",{"tabindex":"-1","class":"exo-icon exo-modal-close"});
        cl.appendChild(this.close_button);
        let p_elt = this.parentElement;
        w.appendChild(this);
        this.appendChild(cl);
        p_elt.appendChild(m);

        ExoModal.observedAttributes.map((attr) => {
            if (this.hasAttribute(attr)) {
                this.attributeChangedCallback(attr,null,this.getAttribute(attr));
            }
        });
    }

    attributeChangedCallback(name, oldValue, newValue) {
        if (this.close_button) {
            switch(name) {
                case "button-text":
                    ExoUtils.removeAllChildren(this.open_button);
                    this.open_button.appendChild(document.createTextNode(newValue));
                    if (newValue) {
                        this.open_button.setAttribute("style","display:inline;");
                    } else {
                        this.open_button.setAttribute("style","display:none;");
                    }
                    break;
                case "button-label":
                    ExoUtils.removeAllChildren(this.label);
                    this.label.appendChild(document.createTextNode(newValue));
                    if (newValue) {
                        this.label.setAttribute("style","display:block;");
                    } else {
                        this.label.setAttribute("style","display:none;");
                    }
                    break;
                case "button-fg-color":
                   ExoUtils.applyColor(this.open_button,"fg",newValue);
                    break;
                case "button-bg-color":
                    ExoUtils.applyColor(this.open_button,"bg",newValue);
                    break;
                case "fg-color":
                   ExoUtils.applyColor(this,"fg",newValue);
                    break;
                case "bg-color":
                    ExoUtils.applyColor(this,"bg",newValue);
                    break;
                case "close-button-color":
                    ExoUtils.addClass(this.close_button,"exo-"+newValue);
                    break;
                case "border-color":
                    ExoUtils.applyColor(this, "border", newValue);
                    ExoUtils.addClass(this,"exo-border");
                    break;
                case "display":
                    this.checkbox.checked = (newValue == "true") ? true : false;
                    break;
                case "border":
                case "rounded":
                    ExoUtils.applySizedDimension(this,name,newValue);
                    break;
                case "width":
                    ExoUtils.addStyle(this,"width",newValue);
                    break;
                case "height":
                    ExoUtils.addStyle(this,"max-height",newValue);
                    break;
            }
        }
    }

    static get observedAttributes() {
        return ["button-text", "button-label", "button-fg-color", "button-bg-color", "fg-color", "bg-color", "close-button-color",
            "display", "border-color", "border", "margin", "rounded", "padding", "width", "height"];
    }
}

customElements.define(
  "exo-modal",
  ExoModal,
    {"extends":"div"}
);



/* js/layouts/autocell.js */


/*
    Call this function after the document is loaded (or whenever cell content is updated) to snap the cell width of all cells marked exo-auto-cell
 */
function exo_autocell_snap2grid() {

    function getElementWidth(p,e) {
        var sty = getComputedStyle(p);
        return e.getBoundingClientRect().width +
            parseFloat(sty.getPropertyValue('margin-left')) +
            parseFloat(sty.getPropertyValue('margin-right'));
    }

    var elts = document.getElementsByClassName("exo-auto-cell");
    for(var idx=0; idx<elts.length; idx++) {
        var elt = elts[idx];
        var elt0 = elt.firstElementChild;
        if (elt0) {
            ExoUtils.removeClasses(elt0, /exo-(.*)-cell/);
            var w = getElementWidth(elt,elt0);
            var cells = Math.min(Math.ceil(w/128),12);
            ExoUtils.addClass(elt0,"exo-"+cells+"-cell");
        }
    }
}

