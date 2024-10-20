/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024Visual Topology Ltd

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

    constructor() {
        this.designer_or_application = null;
        this.db = null;
    }

    async init()  {
        this.db = new skadi.ClientStorage("__root__");
        await this.db.open();
    }

    async create_topology(topology_id) {
        await this.db.set_item(topology_id, {});
    }

    async open_topology(topology_id) {
        let item = await this.db.get_item(topology_id);
        if (item === null) {
            await this.db.set_item(topology_id, {});
        }
    }

    async remove_topology(topology_id) {
        let topology_db = new skadi.ClientStorage(topology_id);
        await topology_db.remove();
        await this.db.remove_item(topology_id);
    }

    async list_topologies() {
        return await this.db.get_keys();
    }

    async get_topology_details() {
        let topology_ids = await this.db.get_keys();
        let details = {};
        for(let idx=0; idx<topology_ids.length; idx++) {
            let topology_id = topology_ids[idx];
            let topology_db = new skadi.ClientStorage(topology_id);
            let topology = await topology_db.get_item("topology.json");
            let obj = JSON.parse(topology);
            if (!obj) {
                obj = {"metadata":{}, "packages":[]};
            }
            let metadata = obj["metadata"] || {};
            let packages = [];
            for(let node_id in obj["nodes"]) {
                let node_type = obj["nodes"][node_id]["node_type"];
                let package_id = node_type.split(":")[0];
                if (!packages.includes(package_id)) {
                    packages.push(package_id);
                }
            }
            details[topology_id] = {"metadata":metadata, "package_ids":packages};
        }
        return details;
    }

    async get_topology_detail(topology_id) {
        return await this.db.get_item(topology_id);
    }

    async load_topology(topology_id) {
        let db = new skadi.ClientStorage(topology_id);
        let contents = await db.get_item("topology.json");

        if (contents) {
            return JSON.parse(contents);
        } else {
            return {};
        }
    }

    bind_application(application) {
        this.designer_or_application = application;
    }

    bind_designer(design) {

        this.designer_or_application = design;

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
        let db = new skadi.ClientStorage(this.designer_or_application.get_id());
        await db.set_item("topology.json",JSON.stringify(saved, null, 4));
    }

    async get_save_link() {
        const zipFileWriter = new zip.BlobWriter();

        const zipWriter = new zip.ZipWriter(zipFileWriter);

        let db = new skadi.ClientStorage(this.designer_or_application.get_id());

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
            let db = new skadi.ClientStorage(this.designer_or_application.get_id());
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
        let db = new skadi.ClientStorage(topology_id);
        let path = target_type + "/" + target_id + "/properties.json";
        let properties = await db.get_item(path);
        properties = JSON.parse(properties);
        properties = properties || {};
        return properties;
    }

    async set_properties(topology_id, target_id, target_type, properties) {
        let db = new skadi.ClientStorage(topology_id);
        let path = target_type + "/" + target_id + "/properties.json";
        await db.set_item(path, JSON.stringify(properties));
    }

    async get_data(topology_id, target_id, target_type, key) {
        skadi.ClientStorage.check_valid_key(key);
        let db = new skadi.ClientStorage(topology_id);
        let path = target_type+"/"+target_id+"/data/"+key;
        return await db.get_item(path);
    }

    async set_data(topology_id, target_id, target_type, key, data) {
        skadi.ClientStorage.check_valid_key(key);
        skadi.ClientStorage.check_valid_value(data);
        let db = new skadi.ClientStorage(topology_id);
        let path = target_type+"/"+target_id+"/data/"+key;
        if (data === null) {
            await db.remove_item(path);
        } else {
            await db.set_item(path, data);
        }
    }
}

/* src/js/utils/l10n_bundle.js */

var skadi = skadi || {};

skadi.L10NBundle = class {

    constructor(bundle_content) {
        this.bundle_content = bundle_content;
    }

    localise(input) {
        if (input in this.bundle_content) {
            return this.bundle_content[input];
        }
        // for empty bundles, localise returns the input
        if (Object.keys(this.bundle_content).length == 0) {
            return input;
        }
        // treat the input as possibly containing embedded keys, delimited by {{ and }},
        // for example "say {{hello}}" embeds they key hello
        // substitute any embedded keys and the surrounding delimiters with their values, if the key is present in the bundle
        let idx = 0;
        let s = "";
        while(idx<input.length) {
            if (input.slice(idx, idx+2) === "{{") {
                let startidx = idx+2;
                idx += 2;
                while(idx<input.length) {
                    if (input.slice(idx,idx+2) === "}}") {
                        let token = input.slice(startidx,idx);
                        if (token in this.bundle_content) {
                            token = this.bundle_content[token];
                        }
                        s += token;
                        idx += 2;
                        break;
                    } else {
                        idx += 1;
                    }
                }
            } else {
                s += input.charAt(idx);
                idx++;
            }
        }
        return s;
    }

    get_content() {
        return this.bundle_content;
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

