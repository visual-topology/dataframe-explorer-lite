/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License
*/

/* src/js/utils/index_db.js */

var skadi = skadi || {};

skadi.IndexDB = class {

    constructor(name) {
        this.name = "topology-"+name;
    }

    async init() {
        this.db = await this.open();
    }

    async open() {
        return await new Promise((resolve,reject) => {
            const request = indexedDB.open(this.name, 1);
            request.onsuccess = (evt) => {
                resolve(evt.target.result);
            }
            request.onerror = (evt) => {
                console.error(evt.target.errorCode);
                resolve(null);
            }
            request.onupgradeneeded = (evt) => {
                // Save the IDBDatabase interface
                let db = evt.target.result;
                db.createObjectStore("data", {});
            }
        });
    }


    async get(key) {
        return await new Promise((resolve,reject) => {
            const transaction = this.db.transaction(["data"], "readonly");
            const request = transaction.objectStore("data").get(key);
            request.onsuccess = (evt) => {
                resolve(evt.target.result);
            }
            request.onerror = (evt) => {
                resolve(undefined);
            }
        });
    }

    async put(key, value) {
        return await new Promise((resolve,reject) => {
            const transaction = this.db.transaction(["data"], "readwrite");
            const request = transaction.objectStore("data").put(value,key);
            request.onsuccess = (evt) => {
                resolve(true);
            }
            request.onerror = (evt) => {
                resolve(true);
            }
        });
    }

    async delete(key) {
        return await new Promise((resolve,reject) => {
            const transaction = this.db.transaction(["data"], "readwrite");
            const request = transaction.objectStore("data").delete(key);
            request.onsuccess = (evt) => {
                resolve(evt.target.result);
            }
            request.onerror = (evt) => {
                resolve(undefined);
            }
        });
    }

    async getAllKeys() {
        return await new Promise((resolve,reject) => {
            const transaction = this.db.transaction(["data"], "readonly");
            const request = transaction.objectStore("data").getAllKeys();
            request.onsuccess = (evt) => {
                resolve(evt.target.result);
            }
            request.onerror = (evt) => {
                resolve(undefined);
            }
        });
    }
}

skadi.IndexDB.create = async function(name) {
    let db = new skadi.IndexDB(name);
    await db.init();
    return db;
}

skadi.IndexDB.remove = async function(name) {
    return await new Promise((resolve,reject) => {
        const request = indexedDB.deleteDatabase(name);
        request.onsuccess = (evt) => {
            resolve(true);
        }
        request.onerror = (evt) => {
            console.error(evt.target.errorCode);
            resolve(false);
        }
    });
}

/* src/js/utils/client_storage.js */

skadi = skadi || {};

skadi.ClientStorage = class {

    constructor(db_name) {
        this.db_name = db_name;
        this.db = null;
    }

    static check_valid_key(key) {
        if (!key.match(/^[0-9a-zA-Z_]+$/)) {
            throw new Error("data key can only contain alphanumeric characters and underscores");
        }
    }

    static check_valid_value(data) {
        if (data instanceof ArrayBuffer) {
            return;
        } else if (data === null) {
            return;
        }
        throw new Error("data value can only be null or ArrayBuffer")
    }

    async open() {
        this.db = await skadi.IndexDB.create(this.db_name);
    }

    close() {
        this.db = null;
    }

    async get_item(key) {
        if (!this.db) {
            await this.open();
        }
        let result = await this.db.get(key);
        if (result === undefined) {
            result = null;
        }
        return result;
    }

    async set_item(key, value) {
        if (!this.db) {
            await this.open();
        }
        await this.db.put(key, value);
    }

    async remove_item(key) {
        if (!this.db) {
            await this.open();
        }
        await this.db.delete(key);
    }

    async remove() {
        this.close();
        await skadi.IndexDB.remove(this.db_name);
    }

    async get_keys() {
        if (!this.db) {
            await this.open();
        }
        return await this.db.getAllKeys();
    }

    async clear() {
        // clear the database by removing and re-opening
        await this.remove();
        await this.open();
    }
}

/* src/js/plugins/topology_store.js */

var skadi = skadi || {};

skadi.TopologyStore = class {

    constructor(workspace_id) {
        this.workspace_id = workspace_id;
        this.designer_or_application = null;
        this.db = null;
    }

    get_workspace_path(path) {
        return "workspace."+this.workspace_id+"."+path;
    }

    get_topology_path(path) {
        return "workspace."+this.workspace_id+".topology."+path
    }

    async init()  {
    }

    async open_workspace() {
        let db = new skadi.ClientStorage(this.get_workspace_path("__root__"));
        await db.open();
        return db;
    }

    async create_topology(topology_id) {
        let db = await this.open_workspace();
        await db.set_item(topology_id, {});
    }

    async open_topology(topology_id) {
        let db = await this.open_workspace();
        let item = await db.get_item(topology_id);
        if (item === null) {
            await db.set_item(topology_id, {});
        }
    }

    async remove_topology(topology_id) {
        let topology_db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        await topology_db.remove();
        let db = await this.open_workspace();
        await db.remove_item(topology_id);
    }

    async list_topologies() {
        let db = await this.open_workspace();
        return await this.db.get_keys();
    }

    async get_topology_details() {
        let db = await this.open_workspace();
        let topology_ids = await db.get_keys();
        let topologies = {};
        for (let idx = 0; idx < topology_ids.length; idx++) {
            let topology_id = topology_ids[idx];
            let topology_db = new skadi.ClientStorage(this.get_topology_path(topology_id));
            let topology = await topology_db.get_item("topology.json");
            let obj = JSON.parse(topology);
            if (!obj) {
                obj = {"metadata": {}, "packages": []};
            }
            let metadata = obj["metadata"] || {};
            let packages = [];
            for (let node_id in obj["nodes"]) {
                let node_type = obj["nodes"][node_id]["node_type"];
                let package_id = node_type.split(":")[0];
                if (!packages.includes(package_id)) {
                    packages.push(package_id);
                }
            }
            topologies[topology_id] = {"metadata": metadata, "package_ids": packages};
        }
        return topologies;
    }

    async get_topology_detail(topology_id) {
        return await this.db.get_item(topology_id);
    }

    async load_topology(topology_id) {
        let db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        let contents = await db.get_item("topology.json");

        if (contents) {
            return JSON.parse(contents);
        } else {
            return {};
        }
    }

    bind(designer_or_application) {
        this.designer_or_application = designer_or_application;

        // hook up to skadi events.  when the network changes, save them to local storage
        this.designer_or_application.add_node_event_handler("add", async (node_id, node_type_id) => {
            await this.save();
        });

        this.designer_or_application.add_node_event_handler("remove", async (node_id, node_type_id) => {
            await this.save();
        });

        this.designer_or_application.add_node_event_handler("update_position", async (node_id, node_type_id) => {
            await this.save();
        });

        this.designer_or_application.add_link_event_handler("add", async (link_id, link_type, from_node_id, from_port,
                                          to_node_id, to_port) => {
            await this.save();
        });

        this.designer_or_application.add_link_event_handler("remove", async (link_id, link_type, from_node_id, from_port,
                                             to_node_id, to_port) => {
            await this.save();
        });

        this.designer_or_application.add_design_event_handler("clear", async () => {
            await this.save();
        });

        this.designer_or_application.add_design_event_handler("update_design_metadata", async () => {
            await this.save();
        });

        this.designer_or_application.add_design_event_handler("update_metadata", async (node_id) => {
            await this.save();
        });
    }

    async save() {
        let saved = this.designer_or_application.save();
        let db = new skadi.ClientStorage(this.get_topology_path(this.designer_or_application.get_id()));
        await db.set_item("topology.json",JSON.stringify(saved, null, 4));
    }

    async get_save_link() {
        const zipFileWriter = new zip.BlobWriter();

        const zipWriter = new zip.ZipWriter(zipFileWriter);

        let db = new skadi.ClientStorage(this.get_topology_path(this.designer_or_application.get_id()));

        let data_keys = await db.get_keys();
        for(let j=0; j<data_keys.length; j++) {
            let data_key = data_keys[j];
            let data = await db.get_item(data_key);
            if (data instanceof String || typeof(data) == "string") {
                let rdr = new zip.TextReader(data);
                await zipWriter.add(data_key,rdr);
            } else if (data instanceof ArrayBuffer) {
                let rdr = new zip.BlobReader(new Blob([data]));
                await zipWriter.add(data_key,rdr);
            } else {
                // this should not happen
                console.error("Unable to serialise data " + data_key);
            }
        }

        await zipWriter.close();

        const zipFileBlob = await zipFileWriter.getData();
        let url = URL.createObjectURL(zipFileBlob);
        return url;
    }

    async load_from(file, node_renamings) {
        try {
            let db = new skadi.ClientStorage(this.get_topology_path(this.designer_or_application.get_id()));
            const fileReader = new zip.BlobReader(file);
            const zipReader = new zip.ZipReader(fileReader);
            let entries = await zipReader.getEntries();
            let topology_object = null;
            for (let idx = 0; idx < entries.length; idx++) {
                let entry = entries[idx];
                let name = entry.filename;
                let name_components = name.split("/");
                if (name_components[0] === "node") {
                    let node_id = name_components[1];
                    if (this.designer_or_application.get_network().has_node(node_id)) {
                        node_renamings[node_id] = this.designer_or_application.core.next_id("n");
                        name_components[1] = node_renamings[node_id];
                        name = name_components.join("/");
                    }
                }
                if (name === "topology.json" || name.endsWith("/properties.json")) {
                    let text = await entry.getData(new zip.TextWriter());
                    if (name === "topology.json") {
                        topology_object = JSON.parse(text);
                    }
                    await db.set_item(name, text);
                } else {
                    let blob = await entry.getData(new zip.BlobWriter());
                    let buffer = await blob.arrayBuffer();
                    await db.set_item(name, buffer);
                }
            }

            await this.designer_or_application.load(topology_object, node_renamings, []);
        } catch(ex) {
           console.error("load error: "+ex);
        }
    }

    get_file_suffix() {
        return ".zip";
    }

    async get_properties(topology_id, target_id, target_type) {
        let db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        let path = target_type + "/" + target_id + "/properties.json";
        let properties = await db.get_item(path);
        properties = JSON.parse(properties);
        properties = properties || {};
        return properties;
    }

    async set_properties(topology_id, target_id, target_type, properties) {
        let db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        let path = target_type + "/" + target_id + "/properties.json";
        await db.set_item(path, JSON.stringify(properties));
    }

    async get_data(topology_id, target_id, target_type, key) {
        skadi.ClientStorage.check_valid_key(key);
        let db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        let path = target_type+"/"+target_id+"/data/"+key;
        return await db.get_item(path);
    }

    async set_data(topology_id, target_id, target_type, key, data) {
        skadi.ClientStorage.check_valid_key(key);
        skadi.ClientStorage.check_valid_value(data);
        let db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        let path = target_type+"/"+target_id+"/data/"+key;
        if (data === null) {
            await db.remove_item(path);
        } else {
            await db.set_item(path, data);
        }
    }
}

/* src/js/utils/expr_parser.js */

var skadi = skadi || {};

skadi.ExpressionParser = class {

    constructor() {
        this.input = undefined;
        this.unary_operators = {};
        this.binary_operators = {};
        this.reset();
    }

    reset() {
        // lexer state
        this.index = 0;
        this.tokens = [];
        this.current_token_type = undefined; // s_string, d_string, string, name, operator, number, open_parenthesis, close_parenthesis, comma
        this.current_token_start = 0;
        this.current_token = undefined;
    }

    add_unary_operator(name) {
        this.unary_operators[name] = true;
    }

    add_binary_operator(name,precedence) {
        this.binary_operators[name] = precedence;
    }

    is_alphanum(c) {
        return (this.is_alpha(c) || (c >= "0" && c <= "9"));
    }

    is_alpha(c) {
        return ((c >= "a" && c <= "z") || (c >= "A" && c <= "Z"));
    }

    flush_token() {
        if (this.current_token_type !== undefined) {
            if (this.current_token_type === "name") {
                // convert to name => operator if the name matches known operators
                if (this.current_token in this.binary_operators || this.current_token in this.unary_operators) {
                    this.current_token_type = "operator";
                }
            }
            this.tokens.push([this.current_token_type, this.current_token, this.current_token_start]);
        }
        this.current_token = "";
        this.current_token_type = undefined;
        this.current_token_start = undefined;
    }

    read_whitespace(c) {
        switch(this.current_token_type) {
            case "s_string":
            case "d_string":
                this.current_token += c;
                break;
            case "name":
            case "operator":
            case "number":
                this.flush_token();
                break;
        }
    }

    read_doublequote() {
        switch(this.current_token_type) {
            case "d_string":
                this.flush_token();
                break;
            case "s_string":
                this.current_token += '"';
                break;
            default:
                this.flush_token();
                this.current_token_type = "d_string";
                this.current_token_start = this.index;
                break;
        }
    }

    read_singlequote() {
        switch(this.current_token_type) {
            case "s_string":
                this.flush_token();
                break;
            case "d_string":
                this.current_token += "'";
                break;
            default:
                this.flush_token();
                this.current_token_type = "s_string";
                this.current_token_start = this.index;
                break;
        }
    }

    read_digit(c) {
        switch(this.current_token_type) {
            case "operator":
                this.flush_token();
            case undefined:
                this.current_token_type = "number";
                this.current_token_start = this.index;
                this.current_token = c;
                break;
            case "d_string":
            case "s_string":
            case "name":
            case "number":
                this.current_token += c;
                break;
        }
    }

    read_e(c) {
        switch(this.current_token_type) {
            case "number":
                // detect exponential notation E or e
                this.current_token += c;
                // special case, handle negative exponent eg 123e-10
                if (this.input[this.index+1] === "-") {
                    this.current_token += "-";
                    this.index += 1;
                }
                break;

            default:
                this.read_default(c);
                break;
        }
    }

    read_parenthesis(c) {
        switch(this.current_token_type) {
            case "s_string":
            case "d_string":
                this.current_token += c;
                break;
            default:
                this.flush_token();
                this.tokens.push([(c === "(") ? "open_parenthesis" : "close_parenthesis",c, this.index]);
                break;
        }
    }

    read_comma(c) {
        switch(this.current_token_type) {
            case "d_string":
            case "s_string":
                this.current_token += c;
                break;
            default:
                this.flush_token();
                this.tokens.push(["comma",c, this.index]);
                break;
        }
    }

    read_default(c) {
        switch(this.current_token_type) {
            case "d_string":
            case "s_string":
                this.current_token += c;
                break;
            case "name":
                if (this.is_alphanum(c) || c === "_" || c === ".") {
                    this.current_token += c;
                } else {
                    this.flush_token();
                    this.current_token_type = "operator";
                    this.current_token_start = this.index;
                    this.current_token = c;
                }
                break;
            case "number":
                this.flush_token();
                // todo handle exponential notation eg 1.23e10
                if (this.is_alphanum(c)) {
                    throw {"error":"invalid_number","error_pos":this.index,"error_content":c};
                } else {
                    this.flush_token();
                    this.current_token_type = "operator";
                    this.current_token_start = this.index;
                    this.current_token = c;
                }
                break;
            case "operator":
                if (this.is_alphanum(c)) {
                    this.flush_token();
                    this.current_token_type = "name";
                    this.current_token_start = this.index;
                    this.current_token = c;
                } else {
                    if (this.current_token in this.unary_operators || this.current_token in this.binary_operators) {
                        this.flush_token();
                        this.current_token_type = "operator";
                        this.current_token_start = this.index;
                    }
                    this.current_token += c;
                }
                break;
            case undefined:
                this.current_token = c;
                if (this.is_alpha(c)) {
                    this.current_token_type = "name";
                } else {
                    this.current_token_type = "operator";
                }
                this.current_token_start = this.index;
                break;
            default:
                throw {"error":"internal_error","error_pos":this.index};
        }
    }

    read_eos() {
        switch(this.current_token_type) {
            case "d_string":
            case "s_string":
                throw {"error":"unterminated_string","error_pos":this.input.length};
            default:
                this.flush_token();
        }
    }

    merge_string_tokens() {
        let merged_tokens = [];
        let buff = "";
        let buff_pos = -1;
        for(let idx=0; idx<this.tokens.length;idx++) {
            let t = this.tokens[idx];
            let ttype = t[0];
            let tcontent = t[1];
            let tstart = t[2];
            if (ttype === "s_string" || ttype === "d_string") {
                buff += tcontent;
                buff_pos = (buff_pos < 0) ? tstart : buff_pos;
            } else {
                if (buff_pos >= 0) {
                    merged_tokens.push(["string",buff,buff_pos]);
                    buff = "";
                    buff_pos = -1;
                }
                merged_tokens.push(t);
            }
        }
        if (buff_pos >= 0) {
            merged_tokens.push(["string", buff, buff_pos]);
        }
        this.tokens = merged_tokens;
    }

    lex() {
        this.reset();
        this.index = 0;
        while(this.index < this.input.length) {
            let c = this.input.charAt(this.index);
            switch(c) {
                case " ":
                case "\t":
                case "\n":
                    this.read_whitespace(c);
                    break;
                case "\"":
                    this.read_doublequote();
                    break;
                case "'":
                    this.read_singlequote();
                    break;
                case "(":
                case ")":
                    this.read_parenthesis(c);
                    break;
                case ",":
                    this.read_comma(c);
                    break;
                case "0":
                case "1":
                case "2":
                case "3":
                case "4":
                case "5":
                case "6":
                case "7":
                case "8":
                case "9":
                case ".":
                    this.read_digit(c);
                    break;
                case "e":
                case "E":
                    this.read_e(c);
                    break;
                default:
                    this.read_default(c);
                    break;
            }
            this.index += 1;
        }
        this.read_eos();
        this.merge_string_tokens();
        return this.tokens;
    }

    get_ascending_precedence() {
        let prec_list = [];
        for(let op in this.binary_operators) {
            prec_list.push(this.binary_operators[op]);
        }

        prec_list = [...new Set(prec_list)];

        prec_list = prec_list.sort();

        return prec_list;
    }

    parse(s) {
        this.input = s;
        try {
            this.lex();
            this.token_index = 0;
            let parsed = this.parse_expr();
            this.strip_debug(parsed);
            return parsed;
        } catch(ex) {
            return ex;
        }
    }

    get_parser_context() {
        return {
            "type": this.tokens[this.token_index][0],
            "content": this.tokens[this.token_index][1],
            "pos": this.tokens[this.token_index][2],
            "next_type": (this.token_index < this.tokens.length - 1) ? this.tokens[this.token_index+1][0] : null,
            "last_type": (this.token_index > 0) ? this.tokens[this.token_index-1][0] : null
        }
    }

    parse_function_call(name) {
        let ctx = this.get_parser_context();
        let result = {
            "function": name,
            "args": [],
            "pos": ctx.pos
        }
        // skip over function name and open parenthesis
        this.token_index += 2;

        // special case - no arguments
        ctx = this.get_parser_context();
        if (ctx.type === "close_parenthesis") {
            return result;
        }

        while(this.token_index < this.tokens.length) {
            ctx = this.get_parser_context();
            if (ctx.last_type === "close_parenthesis") {
                return result;
            } else {
                if (ctx.type === "comma") {
                    throw {"error": "comma_unexpected", "error_pos": ctx.pos};
                }
                // read an expression and a following comma or close parenthesis
                result.args.push(this.parse_expr());
            }
        }
        return result;
    }

    parse_expr() {
        let args = [];
        while(this.token_index < this.tokens.length) {
            let ctx = this.get_parser_context();
            switch(ctx.type) {
                case "name":
                    if (ctx.next_type === "open_parenthesis") {
                        args.push(this.parse_function_call(ctx.content));
                    } else {
                        this.token_index += 1;
                        args.push({"name":ctx.content,"pos":ctx.pos});
                    }
                    break;
                case "string":
                    args.push({"literal":ctx.content,"pos":ctx.pos});
                    this.token_index += 1;
                    break;
                case "number":
                    args.push({"literal":Number.parseFloat(ctx.content),"pos":ctx.pos});
                    this.token_index += 1;
                    break;
                case "open_parenthesis":
                    this.token_index += 1;
                    args.push(this.parse_expr());
                    break;
                case "close_parenthesis":
                case "comma":
                    this.token_index += 1;
                    return this.refine_expr(args,this.token_index-1);
                case "operator":
                    args.push({"operator":ctx.content,"pos":ctx.pos});
                    this.token_index += 1;
                    break;
            }
        }
        return this.refine_expr(args,this.token_index);
    }

    refine_binary(args) {
        let precedences = this.get_ascending_precedence();
        for(let precedence_idx=0; precedence_idx < precedences.length; precedence_idx++) {
            let precedence = precedences[precedence_idx];
            for(let idx=args.length-2; idx>=0; idx-=2) {
                let subexpr = args[idx];
                if (subexpr.operator && this.binary_operators[subexpr.operator] === precedence) {
                    let lhs = args.slice(0,idx);
                    let rhs = args.slice(idx+1,args.length);
                    return {"operator":subexpr.operator,"pos":subexpr.pos,"args":[this.refine_binary(lhs),this.refine_binary(rhs)]};
                }
            }
        }
        return args[0];
    }

    refine_expr(args,end_pos) {
        if (args.length === 0) {
            throw {"error": "expression_expected", "pos": end_pos};
        }
        // first deal with unary operators
        for(let i=args.length-1; i>=0; i--) {
            // unary operators
            let arg = args[i];
            let prev_arg = (i>0) ? args[i-1] : undefined;
            let next_arg = (i<args.length-1) ? args[i+1] : undefined;
            if (arg.operator && (arg.operator in this.unary_operators)) {
                if (prev_arg === undefined || prev_arg.operator) {
                    if (next_arg !== undefined) {
                        // special case, convert unary - followed by a number literal to a negative number literal
                        if (arg.operator === "-" && typeof next_arg.literal === "number") {
                            args = args.slice(0, i).concat([{
                                "literal": -1*next_arg.literal,
                                "pos": arg.pos
                            }]).concat(args.slice(i + 2, args.length));
                        } else {
                            args = args.slice(0, i).concat([{
                                "operator": arg.operator,
                                "pos": arg.pos,
                                "args": [next_arg]
                            }]).concat(args.slice(i + 2, args.length));
                        }
                    }
                }
            }
        }

        // check that args are correctly formed, with operators in every second location, ie "e op e op e" and all operators
        // are binary operators with no arguments already assigned
        for(let i=0; i<args.length; i+=1) {
            let arg = args[i];
            if (i % 2 === 1) {
                if (!arg.operator || "args" in arg) {
                    throw {"error": "operator_expected", "error_pos": arg.pos };
                } else {
                    if (!(arg.operator in this.binary_operators)) {
                        throw {"error": "binary_operator_expected", "error_pos": arg.pos};
                    }
                }
            }
            if (i % 2 === 0 || i === args.length-1) {
                if (arg.operator && !("args" in arg)) {
                    throw {"error": "operator_unexpected", "error_pos": arg.pos};
                }
            }
        }

        return this.refine_binary(args);
    }

    strip_debug(expr) {
        if ("pos" in expr) {
            delete expr.pos;
        }
        if ("args" in expr) {
            expr.args.forEach(e => this.strip_debug(e));
        }
    }

}


/* src/js/core/node_execution_states.js */

var skadi = skadi || {};

skadi.NodeExecutionStates = class {
    static pending = "pending";
    static executing = "executing";
    static executed = "executed";
    static failed = "failed";
}

/* src/js/core/status_states.js */

var skadi = skadi || {};

skadi.StatusStates = class {
    static get info() { return "info" };
    static get warning() { return "warning" };
    static get error() { return "error" };
    static get clear() { return "" };
}

/* src/js/page/page_service.js */

var skadi = skadi || {};

skadi.PageService = class {

    constructor(w) {
        this.event_handlers = [];
        this.page_message_handler = null;
        this.pending_page_messages = [];
        this.window = w;
        window.addEventListener("message", (event) => {
            if (event.source === this.window) {
                this.recv_message(event.data);
            }
        });
    }

    send_fn(...msg) {
        this.window.postMessage(msg,window.location.origin);
    }

    set_message_handler(handler) {
        this.page_message_handler = handler;
        this.pending_page_messages.forEach((m) => this.page_message_handler(...m));
        this.pending_page_messages = [];
    }

    send_message(...message) {
        let msg_header = {
            "type": "page_message"
        }
        this.send_fn(...[msg_header,message]);
    }

    add_event_handler(element_id, event_type, callback, target_attribute) {
        target_attribute = target_attribute || "value";
        this.event_handlers.push([element_id, event_type, callback, target_attribute]);
        let msg = {
            "type": "page_add_event_handler",
            "element_id": element_id,
            "event_type": event_type,
            "target_attribute": target_attribute
        };
        this.send_fn(...[msg,null]);
    }

    handle_event(element_id, event_type, value, target_attribute) {
        for(let idx=0; idx<this.event_handlers.length; idx++) {
            let handler_spec = this.event_handlers[idx];
            if (element_id === handler_spec[0] && event_type === handler_spec[1] && target_attribute === handler_spec[3]) {
                handler_spec[2](value);
            }
        }
    }

    set_attributes(element_id, attributes) {
        let msg = {
            "type": "page_set_attributes",
            "element_id": element_id,
            "attributes": attributes
        }
        this.send_fn(...[msg,null]);
    }

    recv_message(msg) {
        let header = msg[0];
        let type = header.type;
        switch (type) {
            case "page_message":
                if (this.page_message_handler) {
                    this.page_message_handler(...msg[1]);
                } else {
                    this.pending_page_messages.push(msg[1]);
                }
                break;
            case "event":
                this.handle_event(header["element_id"], header["event_type"], header["value"], header["target_attribute"]);
                break;
            default:
                console.error("Unknown message type received from page: " + msg.type);
        }
    }

}

/* src/js/plugins/topology_store.js */

var skadi = skadi || {};

skadi.TopologyStore = class {

    constructor(workspace_id) {
        this.workspace_id = workspace_id;
        this.designer_or_application = null;
        this.db = null;
    }

    get_workspace_path(path) {
        return "workspace."+this.workspace_id+"."+path;
    }

    get_topology_path(path) {
        return "workspace."+this.workspace_id+".topology."+path
    }

    async init()  {
    }

    async open_workspace() {
        let db = new skadi.ClientStorage(this.get_workspace_path("__root__"));
        await db.open();
        return db;
    }

    async create_topology(topology_id) {
        let db = await this.open_workspace();
        await db.set_item(topology_id, {});
    }

    async open_topology(topology_id) {
        let db = await this.open_workspace();
        let item = await db.get_item(topology_id);
        if (item === null) {
            await db.set_item(topology_id, {});
        }
    }

    async remove_topology(topology_id) {
        let topology_db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        await topology_db.remove();
        let db = await this.open_workspace();
        await db.remove_item(topology_id);
    }

    async list_topologies() {
        let db = await this.open_workspace();
        return await this.db.get_keys();
    }

    async get_topology_details() {
        let db = await this.open_workspace();
        let topology_ids = await db.get_keys();
        let topologies = {};
        for (let idx = 0; idx < topology_ids.length; idx++) {
            let topology_id = topology_ids[idx];
            let topology_db = new skadi.ClientStorage(this.get_topology_path(topology_id));
            let topology = await topology_db.get_item("topology.json");
            let obj = JSON.parse(topology);
            if (!obj) {
                obj = {"metadata": {}, "packages": []};
            }
            let metadata = obj["metadata"] || {};
            let packages = [];
            for (let node_id in obj["nodes"]) {
                let node_type = obj["nodes"][node_id]["node_type"];
                let package_id = node_type.split(":")[0];
                if (!packages.includes(package_id)) {
                    packages.push(package_id);
                }
            }
            topologies[topology_id] = {"metadata": metadata, "package_ids": packages};
        }
        return topologies;
    }

    async get_topology_detail(topology_id) {
        return await this.db.get_item(topology_id);
    }

    async load_topology(topology_id) {
        let db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        let contents = await db.get_item("topology.json");

        if (contents) {
            return JSON.parse(contents);
        } else {
            return {};
        }
    }

    bind(designer_or_application) {
        this.designer_or_application = designer_or_application;

        // hook up to skadi events.  when the network changes, save them to local storage
        this.designer_or_application.add_node_event_handler("add", async (node_id, node_type_id) => {
            await this.save();
        });

        this.designer_or_application.add_node_event_handler("remove", async (node_id, node_type_id) => {
            await this.save();
        });

        this.designer_or_application.add_node_event_handler("update_position", async (node_id, node_type_id) => {
            await this.save();
        });

        this.designer_or_application.add_link_event_handler("add", async (link_id, link_type, from_node_id, from_port,
                                          to_node_id, to_port) => {
            await this.save();
        });

        this.designer_or_application.add_link_event_handler("remove", async (link_id, link_type, from_node_id, from_port,
                                             to_node_id, to_port) => {
            await this.save();
        });

        this.designer_or_application.add_design_event_handler("clear", async () => {
            await this.save();
        });

        this.designer_or_application.add_design_event_handler("update_design_metadata", async () => {
            await this.save();
        });

        this.designer_or_application.add_design_event_handler("update_metadata", async (node_id) => {
            await this.save();
        });
    }

    async save() {
        let saved = this.designer_or_application.save();
        let db = new skadi.ClientStorage(this.get_topology_path(this.designer_or_application.get_id()));
        await db.set_item("topology.json",JSON.stringify(saved, null, 4));
    }

    async get_save_link() {
        const zipFileWriter = new zip.BlobWriter();

        const zipWriter = new zip.ZipWriter(zipFileWriter);

        let db = new skadi.ClientStorage(this.get_topology_path(this.designer_or_application.get_id()));

        let data_keys = await db.get_keys();
        for(let j=0; j<data_keys.length; j++) {
            let data_key = data_keys[j];
            let data = await db.get_item(data_key);
            if (data instanceof String || typeof(data) == "string") {
                let rdr = new zip.TextReader(data);
                await zipWriter.add(data_key,rdr);
            } else if (data instanceof ArrayBuffer) {
                let rdr = new zip.BlobReader(new Blob([data]));
                await zipWriter.add(data_key,rdr);
            } else {
                // this should not happen
                console.error("Unable to serialise data " + data_key);
            }
        }

        await zipWriter.close();

        const zipFileBlob = await zipFileWriter.getData();
        let url = URL.createObjectURL(zipFileBlob);
        return url;
    }

    async load_from(file, node_renamings) {
        try {
            let db = new skadi.ClientStorage(this.get_topology_path(this.designer_or_application.get_id()));
            const fileReader = new zip.BlobReader(file);
            const zipReader = new zip.ZipReader(fileReader);
            let entries = await zipReader.getEntries();
            let topology_object = null;
            for (let idx = 0; idx < entries.length; idx++) {
                let entry = entries[idx];
                let name = entry.filename;
                let name_components = name.split("/");
                if (name_components[0] === "node") {
                    let node_id = name_components[1];
                    if (this.designer_or_application.get_network().has_node(node_id)) {
                        node_renamings[node_id] = this.designer_or_application.core.next_id("n");
                        name_components[1] = node_renamings[node_id];
                        name = name_components.join("/");
                    }
                }
                if (name === "topology.json" || name.endsWith("/properties.json")) {
                    let text = await entry.getData(new zip.TextWriter());
                    if (name === "topology.json") {
                        topology_object = JSON.parse(text);
                    }
                    await db.set_item(name, text);
                } else {
                    let blob = await entry.getData(new zip.BlobWriter());
                    let buffer = await blob.arrayBuffer();
                    await db.set_item(name, buffer);
                }
            }

            await this.designer_or_application.load(topology_object, node_renamings, []);
        } catch(ex) {
           console.error("load error: "+ex);
        }
    }

    get_file_suffix() {
        return ".zip";
    }

    async get_properties(topology_id, target_id, target_type) {
        let db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        let path = target_type + "/" + target_id + "/properties.json";
        let properties = await db.get_item(path);
        properties = JSON.parse(properties);
        properties = properties || {};
        return properties;
    }

    async set_properties(topology_id, target_id, target_type, properties) {
        let db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        let path = target_type + "/" + target_id + "/properties.json";
        await db.set_item(path, JSON.stringify(properties));
    }

    async get_data(topology_id, target_id, target_type, key) {
        skadi.ClientStorage.check_valid_key(key);
        let db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        let path = target_type+"/"+target_id+"/data/"+key;
        return await db.get_item(path);
    }

    async set_data(topology_id, target_id, target_type, key, data) {
        skadi.ClientStorage.check_valid_key(key);
        skadi.ClientStorage.check_valid_value(data);
        let db = new skadi.ClientStorage(this.get_topology_path(topology_id));
        let path = target_type+"/"+target_id+"/data/"+key;
        if (data === null) {
            await db.remove_item(path);
        } else {
            await db.set_item(path, data);
        }
    }
}

/* src/js/executor/client_service.js */

var skadi = skadi || {};

skadi.ClientService = class {

    constructor(send_fn) {
        this.event_handlers = [];
        this.page_message_handler = null;
        this.pending_page_messages = [];
        this.send_fn = send_fn;
    }

    set_message_handler(handler) {
        this.page_message_handler = handler;
        this.pending_page_messages.forEach((m) => this.page_message_handler(...m));
        this.pending_page_messages = [];
    }

    send_message(...message) {
        let msg_header = {
            "type": "page_message"
        }

        this.send_fn(...[msg_header,message]);
    }

    add_event_handler(element_id, event_type, callback, target_attribute) {
        target_attribute = target_attribute || "value";
        this.event_handlers.push([element_id, event_type, callback, target_attribute]);
        let msg = {
            "type": "page_add_event_handler",
            "element_id": element_id,
            "event_type": event_type,
            "target_attribute": target_attribute
        };
        this.send_fn(...[msg,null]);
    }

    handle_page_message(...message_parts) {
        if (this.page_message_handler) {
            this.page_message_handler(...message_parts);
        } else {
            this.pending_page_messages.push(message_parts);
        }
    }

    handle_event(element_id, event_type, value, target_attribute) {
        for(let idx=0; idx<this.event_handlers.length; idx++) {
            let handler_spec = this.event_handlers[idx];
            if (element_id === handler_spec[0] && event_type === handler_spec[1] && target_attribute === handler_spec[3]) {
                handler_spec[2](value);
            }
        }
    }

    set_attributes(element_id, attributes) {
        let msg = {
            "type": "page_set_attributes",
            "element_id": element_id,
            "attributes": attributes
        }
        this.send_fn(...[msg,null]);
    }

    recv_message(...msg) {
        let header = msg[0];
        let type = header.type;
        console.log("recv_message");
        switch (type) {
            case "page_message":
                let message_parts = msg.slice(1);
                this.handle_page_message(...message_parts);
                break;
            case "event":
                this.handle_event(header["element_id"], header["event_type"], header["value"], header["target_attribute"]);
                break;
            default:
                console.error("Unknown message type received from page: " + type);
        }
    }

}

/* src/js/executor/package_settings.js */

var skadi = skadi || {};

skadi.PackageSettings = class {

    constructor(package_id, base_url) {
        this.package_id = package_id;
        this.base_url = base_url;
    }

    get_base_url() {
        return this.base_url;
    }
}

/* src/js/executor/wrapper.js */

var skadi = skadi || {};

skadi.Wrapper = class {

    constructor(topology_id, target_id, target_type, topology_store, services) {
        this.topology_id = topology_id;
        this.target_id = target_id;
        this.target_type = target_type;
        this.topology_store = topology_store;
        this.services = services;
        this.instance = null;
        this.properties = undefined;
        this.services.wrapper = this;
    }

    set_instance(instance) {
        this.instance = instance;
    }

    get_instance() {
        return this.instance;
    }

    async load_properties() {
        this.properties = await this.topology_store.get_properties(this.topology_id, this.target_id, this.target_type);
    }

    get_property(property_name, default_value) {
        if (property_name in this.properties) {
            return this.properties[property_name];
        } else {
            return default_value;
        }
    }

    set_property(property_name, property_value) {
        this.properties[property_name] =  property_value;
        setTimeout(async () => {
            await this.topology_store.set_properties(this.topology_id, this.target_id, this.target_type, this.properties);
        },0);
    }

    async get_data(key) {
        return await this.topology_store.get_data(this.topology_id, this.target_id, this.target_type, key);
    }

    async set_data(key, data) {
        await this.topology_store.set_data(this.topology_id, this.target_id, this.target_type, key, data);
    }

    get_services() {
        return this.services;
    }

    open_client(page_id, client_options, client_service) {
        if (this.instance.open_client) {
            try {
                this.instance.open_client(page_id, client_options, client_service);
            } catch(e) {
                console.error(e);
            }
        }
    }

    close_client(page_id) {
        if (this.instance.close_client) {
            try {
                this.instance.close_client(page_id);
            } catch(e) {
                console.error(e);
            }
        }
    }

    close() {
        if (this.instance.close) {
            try {
                this.instance.close();
            } catch(e) {
                console.error(e);
            }
        }
    }

    remove() {
        if (this.instance.close) {
            try {
                this.instance.close();
            } catch(e) {
                console.error(e);
            }
        }
    }
}

/* src/js/executor/service.js */

var skadi = skadi || {};

skadi.Service = class  {

    constructor(executor, update_status_callback, package_settings) {
        this.executor = executor;
        this.update_status_callback = update_status_callback;
        this.package_settings = package_settings;
        this.wrapper = null;
    }

    get_property(property_name, default_value) {
        return this.wrapper.get_property(property_name, default_value);
    }

    set_property(property_name, property_value) {
        this.wrapper.set_property(property_name, property_value);
    }

    resolve_resource(resource_path) {
        return this.package_settings.get_base_url() + "/" + resource_path;
    }

    async get_data(key) {
        return await this.wrapper.get_data(key);
    }

    async set_data(key, data) {
        await this.wrapper.set_data(key, data);
    }
}


/* src/js/executor/configuration_service.js */

var skadi = skadi || {};

skadi.ConfigurationService = class extends skadi.Service {

    constructor(executor, package_id, update_status_callback, package_settings) {
        super(executor, update_status_callback, package_settings);
        this.package_id = package_id;
    }

    set_status_info(status_msg) {
        this.update_status_callback(this.package_id, skadi.StatusStates.info, status_msg);
    }

    set_status_warning(status_msg) {
        this.update_status_callback(this.package_id, skadi.StatusStates.warning, status_msg);
    }

    set_status_error(status_msg) {
        this.update_status_callback(this.package_id, skadi.StatusStates.error, status_msg);
    }

    clear_status() {
        this.update_status_callback(this.package_id, skadi.StatusStates.clear, "");
    }

    get_configuration(package_id) {
        let configuration_wrapper = this.executor.get_configuration(package_id);
        if (configuration_wrapper) {
            return configuration_wrapper.get_instance();
        } else {
            return null;
        }
    }
}


/* src/js/executor/configuration_wrapper.js */

var skadi = skadi || {};

skadi.ConfigurationWrapper = class extends skadi.Wrapper {

    constructor(topology_id, topology_store, package_id, services) {
        super(topology_id, package_id, "configuration", topology_store, services);
        this.package_id = package_id;
    }
}

/* src/js/executor/node_service.js */

var skadi = skadi || {};

skadi.NodeService = class extends skadi.Service {

    constructor(executor, node_id, node_type_id, update_status_callback, update_execution_state_callback, package_settings) {
        super(executor, update_status_callback, package_settings);
        this.node_id = node_id;
        this.node_type = node_type_id;
        this.package_id = node_type_id.split(":")[0];
        this.update_execution_state_callback = update_execution_state_callback;
    }

    get_node_id() {
        return this.node_id;
    }

    get_configuration(package_id) {
        let configuration_wrapper = this.executor.get_configuration(package_id || this.package_id);
        if (configuration_wrapper) {
            return configuration_wrapper.get_instance();
        } else {
            return null;
        }
    }

    request_run() {
        this.executor.request_execution(this.node_id);
    }

    set_status_info(status_msg) {
        this.update_status_callback(this.node_id, skadi.StatusStates.info, status_msg);
    }

    set_status_warning(status_msg) {
        this.update_status_callback(this.node_id, skadi.StatusStates.warning, status_msg);
    }

    set_status_error(status_msg) {
        this.update_status_callback(this.node_id, skadi.StatusStates.error, status_msg);
    }

    clear_status() {
        this.update_status_callback(this.node_id, skadi.StatusStates.clear, "");
    }

    set_state(new_state) {
        if (this.update_execution_state_callback) {
            this.update_execution_state_callback(this.node_id, new_state);
        }
    }
}


/* src/js/executor/node_wrapper.js */

var skadi = skadi || {};

skadi.NodeWrapper = class extends skadi.Wrapper {

    constructor(topology_id, topology_store, node_id, services) {
        super(topology_id, node_id, "node", topology_store, services);
        this.node_id = node_id;
    }

    reset_execution() {
        if (this.instance.reset_run) {
            try {
                this.instance.reset_run();
            } catch(e) {
                console.error(e);
            }
        }
    }

    async execute(inputs) {
        if (this.instance.run) {
            try {
                let results = await this.instance.run(inputs);
                return results;
            } catch(e) {
                console.error(e);
                throw e;
            }
        }
    }

    notify_connections_changed(new_connection_counts) {
        if (this.instance.connections_changed) {
            try {
                this.instance.connections_changed(new_connection_counts["inputs"],new_connection_counts["outputs"]);
            } catch(e) {
                console.error(e);
            }
        }
    }
}

/* src/js/executor/graph_link.js */

var skadi = skadi || {};

skadi.GraphLink = class {

    constructor(executor, from_node_id, from_port, to_node_id, to_port) {
        this.executor = executor;
        this.from_node_id = from_node_id;
        this.from_port = from_port;
        this.to_node_id = to_node_id;
        this.to_port = to_port;
    }

    get_value() {
        if (this.from_node_id in this.executor.node_outputs) {
            let outputs = this.executor.node_outputs[this.from_node_id];
            if (outputs && this.from_port in outputs) {
                return outputs[this.from_port];
            }
        }
        return null;
    }
}


/* src/js/executor/graph_executor.js */

var skadi = skadi || {};

skadi.GraphExecutor = class {

    constructor(workspace_id, topology_id, class_map, execution_complete_callback, execution_state_callback, node_status_callback, configuration_status_callback) {
        skadi.graph_executor = this;
        this.workspace_id = workspace_id;
        this.topology_id = topology_id;
        this.class_map = class_map;
        this.topology_store = new skadi.TopologyStore(workspace_id);

        this.nodes = {}; // node-id => node-wrapper
        this.links = {}; // link-id => GraphLink
        this.out_links = {}; // node-id => output-port => [GraphLink]
        this.in_links = {};  // node-id => input-port => [GraphLink]

        this.package_l10n = {};
        this.configurations = {}; // package-id => configuration-wrapper
        this.package_settings = {}; // package-id => package-settings

        this.dirty_nodes = {}; // node-id => True
        this.executing_nodes = {}; // node-id => True
        this.executed_nodes = {};  // node-id => True
        this.failed_nodes = {};    // node-id => Exception
        this.execution_limit = 4;
        this.node_outputs = {}; // node-id => output-port => value

        this.paused = false;

        this.dispatch();

        this.execution_complete_callback = execution_complete_callback;
        this.execution_state_callback = execution_state_callback;
        this.node_status_callback = node_status_callback;
        this.configuration_status_callback = configuration_status_callback;
    }

    get executing_node_count() {
        return Object.keys(this.executing_nodes).length;
    }

    clear() {
        this.nodes = {};
        this.links = {};

        this.out_links = {};
        this.in_links = {};

        this.executing_nodes = {};
        this.dirty_nodes = {};
        this.node_outputs = {};
    }

    valid_node(node_id) {
        return (node_id in this.nodes);
    }

    pause() {
        this.paused = true;
    }

    resume() {
        this.paused = false;
        this.dispatch().then(r => {
        });
    }

    mark_dirty(node_id) {
        if (node_id in this.dirty_nodes) {
            return;
        }

        this.dirty_nodes[node_id] = true;
        this.reset_execution(node_id);
        delete this.node_outputs[node_id];

        /* mark all downstream nodes as dirty */
        for (let out_port in this.out_links[node_id]) {
            let outgoing_links = this.out_links[node_id][out_port];
            outgoing_links.map((link) => this.mark_dirty(link.to_node_id));
        }
    }

    reset_execution(node_id) {
        if (!(node_id in this.nodes)) {
            return;
        }
        let node = this.nodes[node_id];
        this.update_execution_state(node_id, skadi.NodeExecutionStates.pending);
        delete this.failed_nodes[node_id];
        delete this.executed_nodes[node_id];
        node.reset_execution();
    }

    async dispatch() {
        if (this.paused) {
            return;
        }
        let launch_nodes = [];
        let launch_limit = (this.execution_limit - this.executing_node_count);
        if (launch_limit > 0) {
            for (let node_id in this.dirty_nodes) {
                if (this.can_execute(node_id)) {
                    launch_nodes.push(node_id);
                }
                if (launch_nodes.length >= launch_limit) {
                    break;
                }
            }
        }

        if (launch_nodes.length === 0 && this.executing_node_count == 0) {
            this.execution_complete();
        }

        for (let idx = 0; idx < launch_nodes.length; idx++) {
            let node_id = launch_nodes[idx];
            this.launch_execution(node_id);
        }
    }

    can_execute(node_id) {
        for (let in_port in this.in_links[node_id]) {
            let in_links = this.in_links[node_id][in_port];
            for (let idx in in_links) {
                let in_link = in_links[idx];
                let pred_node_id = in_link.from_node_id;
                if (!(pred_node_id in this.executed_nodes)) {
                    return false;
                }
            }
        }
        return true;
    }

    launch_execution(node_id) {
        if (!(node_id in this.nodes)) {
            return;
        }
        console.log("Executing: "+node_id);
        delete this.dirty_nodes[node_id];
        this.executing_nodes[node_id] = true;
        let node = this.nodes[node_id];
        let inputs = {};
        for (let in_port in this.in_links[node_id]) {
            let in_links = this.in_links[node_id][in_port];
            inputs[in_port] = [];
            for (let idx in in_links) {
                let in_link = in_links[idx];
                inputs[in_port].push(in_link.get_value());
            }
        }

        this.update_execution_state(node_id, skadi.NodeExecutionStates.executing);

        node.execute(inputs).then(
            (outputs) => this.executed(node_id, outputs),
            (reason) => this.executed(node_id, null, reason)).then(
            () => this.dispatch()
        );
    }

    executed(node_id, outputs, reject_reason) {
        if (!this.valid_node(node_id)) {
            return; // node has been deleted since it started executing
        }
        delete this.executing_nodes[node_id];
        delete this.node_outputs[node_id];
        if (reject_reason) {
            this.update_execution_state(node_id, skadi.NodeExecutionStates.failed);
            console.error("Execution of " + node_id + " failed with reason: " + reject_reason);
            if (reject_reason.stack) {
                console.error(reject_reason.stack);
            }
            this.failed_nodes[node_id] = reject_reason;
        } else {
            this.update_execution_state(node_id, skadi.NodeExecutionStates.executed);
            this.node_outputs[node_id] = outputs;
            this.executed_nodes[node_id] = true;
        }
    }

    async add_package(package_id, base_url) {
        let package_settings = new skadi.PackageSettings(package_id, base_url);
        this.package_settings[package_id] = package_settings;
        if (package_id in this.class_map.configurations) {
            await this.add_configuration(package_id, package_settings);
        }
    }

    async add_configuration(package_id, package_settings) {

        let services = new skadi.ConfigurationService(this, package_id, (package_id, status, status_msg) => {
            this.update_configuration_status(package_id, status, status_msg);
        }, package_settings);
        let wrapper = new skadi.ConfigurationWrapper(this.topology_id, this.topology_store, package_id, services);

        await wrapper.load_properties();

        let classname = this.class_map.configurations[package_id];
        let cls = eval(classname);
        let o = new cls(services);

        if (o.load) {
            await o.load();
        }

        wrapper.set_instance(o);
        this.configurations[package_id] = wrapper;
    }

    get_configuration(package_id) {
        return this.configurations[package_id];
    }

    async add_node(node_id, node_type_id) {
        let package_id = node_type_id.split(":")[0];
        let package_settings = this.package_settings[package_id];
        let services = new skadi.NodeService(this, node_id, node_type_id,
            (node_id, status, status_msg) => {
                this.update_node_status(node_id, status, status_msg);
            },
            (node_id, execution_state) => {
                this.update_execution_state(node_id, execution_state, true);
            },
            package_settings);
        let wrapper = new skadi.NodeWrapper(this.topology_id, this.topology_store, node_id, services);

        await wrapper.load_properties();

        let classname = this.class_map.nodes[node_type_id];
        let cls = eval(classname);
        let o = new cls(services);
        if (o.load) {
            await o.load();
        }

        wrapper.set_instance(o);
        this.nodes[node_id] = wrapper;

        this.in_links[node_id] = {};
        this.out_links[node_id] = {};
        this.node_outputs[node_id] = {};
        this.mark_dirty(node_id);
        this.dispatch().then(r => {
        });
    }

    add_link(link_id, from_node_id, from_port, to_node_id, to_port) {

        let link = new skadi.GraphLink(this, from_node_id, from_port, to_node_id, to_port);
        this.links[link_id] = link;

        if (!(from_port in this.out_links[from_node_id])) {
            this.out_links[from_node_id][from_port] = [];
        }

        if (!(from_port in this.in_links[to_node_id])) {
            this.in_links[to_node_id][to_port] = [];
        }

        this.out_links[from_node_id][from_port].push(link);
        this.in_links[to_node_id][to_port].push(link);

        this.mark_dirty(to_node_id);

        this.dispatch().then(r => {
        });
    }

    remove_link(link_id) {
        let link = this.links[link_id];
        delete this.links[link_id];

        let arr_out = this.out_links[link.from_node_id][link.from_port];
        arr_out.splice(arr_out.indexOf(link), 1);

        let arr_in = this.in_links[link.to_node_id][link.to_port];
        arr_in.splice(arr_in.indexOf(link), 1);

        this.mark_dirty(link.to_node_id);

        this.dispatch().then(r => {
        });
    }

    remove_node(node_id) {
        delete this.executing_nodes[node_id];
        delete this.failed_nodes[node_id];
        delete this.executed_nodes[node_id];
        delete this.dirty_nodes[node_id];
        delete this.nodes[node_id];
        delete this.node_outputs[node_id];
    }

    get_node(node_id) {
        return this.nodes[node_id];
    }

    request_execution(node_id) {
        this.mark_dirty(node_id);
        this.dispatch().then(r => {
        });
    }

    execution_complete() {
        if (this.execution_complete_callback) {
            this.execution_complete_callback();
        }
    }

    update_execution_state(node_id, execution_state, is_manual) {
        if (this.execution_state_callback) {
            this.execution_state_callback(node_id, execution_state, is_manual !== undefined ? is_manual : false);
        }
    }

    update_node_status(node_id, status, msg) {
        if (this.node_status_callback) {
            this.node_status_callback(node_id, status, msg);
        }
    }

    update_configuration_status(package_id, status, msg) {
        if (this.configuration_status_callback) {
            this.configuration_status_callback(package_id, status, msg);
        }
    }
}

/* src/js/executor/node_execution_failed.js */

var skadi = skadi || {};

skadi.NodeExecutionFailed = class extends Error {

    constructor(node_id, message, from_exn) {
        super(message);
        this.node_id = node_id;
        this.cause = from_exn;
    }

    toString() {
        return "Execution Failed for Node "+this.node_id+": "+this.super.toString();
    }

}

/* src/js/executor/worker.js */

var skadi = skadi || {};

skadi.Worker = class {

    constructor() {
        this.graph_executor = null;
        this.class_map = null;
        this.packages = {};
        this.message_queue = [];
        this.handling = false;
        this.client_services = {};
    }

    async init(o) {
        o["imports"].forEach( name => {
            console.log("worker importing:"+name);
            importScripts(name);
        });

        this.graph_executor = new skadi.GraphExecutor(o["workspace_id"], o["topology_id"], o["class_map"], () => {
            this.send({"action":"execution_complete"});
        }, (node_id,execution_state,is_manual) => {
            this.send({"action":"update_execution_state","node_id":node_id, "execution_state":execution_state, "is_manual": is_manual});
        }, (node_id,status, msg) => {
            this.send({"action":"update_node_status","node_id":node_id, "status":status, "message":msg});
        }, (package_id,status,msg) => {
            this.send({"action":"update_configuration_status","package_id":package_id, "status":status, "message":msg});
        });

        for(let idx=0; idx<o.packages.length; idx++) {
            let package_id = o.packages[idx]["package_id"];
            let base_url = o.packages[idx]["base_url"];
            console.log("worker adding package:"+package_id);
            await this.graph_executor.add_package(package_id, base_url);
        }
    }

    async add_node(o) {
        await this.graph_executor.add_node(o["node_id"],o["node_type_id"]);
    }

    remove_node(o) {
        this.graph_executor.remove_node(o["node_id"]);
    }

    add_link(o) {
        this.graph_executor.add_link(o["link_id"], o["from_node_id"], o["from_port"], o["to_node_id"],o["to_port"]);
    }

    remove_link(o) {
        this.graph_executor.remove_link(o["link_id"]);
    }

    create_client_key(target_id, page_id) {
        return target_id + "_" + page_id;
    }

    open_client(o) {
        let target_id = o["target_id"];
        let page_id = o["page_id"];
        let client_key = this.create_client_key(target_id, page_id);
        let client_service = new skadi.ClientService( (...msg) => {
                console.log("sending client message");
                this.send({ "action": "client_message", "page_id":page_id, "target_id":target_id },...msg);
            });
        this.client_services[client_key] = client_service;
        let client_options = o["client_options"];
        let target = this.graph_executor.get_node(target_id);
        if (target === undefined) {
            target = this.graph_executor.get_configuration(target_id);
        }
        if (target) {
            target.open_client(page_id, client_options,client_service);
        }
    }

    client_message(o,...msg) {
        let target_id = o["target_id"];
        let page_id = o["page_id"];
        let client_key = this.create_client_key(target_id, page_id);
        let client_service = this.client_services[client_key];
        if (client_service) {
            client_service.handle_page_message(...msg);
        }
    }

    client_event(o) {
        console.log("handle_event");
        let target_id = o["target_id"];
        let page_id = o["page_id"];
        let client_key = this.create_client_key(target_id, page_id);
        let client_service = this.client_services[client_key];
        client_service.handle_event(o["element_id"], o["event_type"], o["value"], o["target_attribute"]);
    }

    close_client(o) {
        let target_id = o["target_id"];
        let page_id = o["page_id"];
        let client_key = this.create_client_key(target_id, page_id);
        delete this.client_services[client_key];
        let target = this.graph_executor.get_node(target_id);
        if (target === undefined) {
            target = this.graph_executor.get_configuration(target_id);
        }
        if (target) {
            target.close_client(page_id);
        }
    }

    pause(o) {
        this.graph_executor.pause();
    }

    resume(o) {
        this.graph_executor.resume();
    }

    clear(o) {
        this.graph_executor.clear();
    }

    async recv(msg) {
        if (this.handling) {
            this.message_queue.push(msg);
        } else {
            this.handling = true;
            try {
                await this.handle(msg);
            } finally {
                while(true) {
                    let msg = this.message_queue.shift();
                    if (msg) {
                        try {
                            await this.handle(msg);
                        } catch(ex) {
                        }
                    } else {
                        break;
                    }
                }
                this.handling = false;
            }
        }
    }

    async handle(msg) {
        let o = JSON.parse(msg[0]);
        switch(o.action) {
            case "init":
                await this.init(o);
                this.send({"action":"init_complete"});
                break;
            case "add_node":
                await this.add_node(o);
                break;
            case "remove_node":
                await this.remove_node(o);
                break;
            case "add_link":
                this.add_link(o);
                break;
            case "remove_link":
                this.remove_link(o);
                break;
            case "open_client":
                this.open_client(o);
                break;
            case "client_message":
                this.client_message(o,...msg.slice(1));
                break;
            case "client_event":
                this.client_event(o);
                break;
            case "close_client":
                this.close_client(o);
                break;
            case "pause":
                this.pause(o);
                break;
            case "resume":
                this.resume(o);
                break;
            case "clear":
                this.clear(o);
                break;
        }
    }

    send(control_packet,...extra) {
        let message_parts = [JSON.stringify(control_packet)];
        extra.forEach(o => {
            message_parts.push(o);
        })
        postMessage(message_parts);
    }
}

skadi.worker = new skadi.Worker();

onmessage = async (e) => {
    await skadi.worker.recv(e.data);
}

