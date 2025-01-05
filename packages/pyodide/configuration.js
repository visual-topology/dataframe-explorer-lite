/*   Skadi - A visual modelling tool for constructing and executing directed graphs.

     Copyright (C) 2022-2024 Visual Topology Ltd

     Licensed under the MIT License
*/

PyodideConfiguration = class {

    constructor(configuration_services) {
        this.services = configuration_services;
        this.update_callbacks = [];
        importScripts("https://cdn.jsdelivr.net/pyodide/v0.26.2/full/pyodide.js");
        this.pyodide = null;
        this.load_pyodide = null;
        this.loaded_packages = [];
    }

    async load() {
        this.pyodide = await loadPyodide();
    }

    get_pyodide() {
        return this.pyodide;
    }

    async load_packages(packages) {
        if (this.pyodide === null) {
            await this.get_pyodide();
        }
        let to_load = [];
        packages.forEach(package_id => {
            if (!this.loaded_packages.includes(package_id)) {
                to_load.push(package_id);
            }
        });
        await this.pyodide.loadPackage(to_load);
    }

    async mount_filesystem(mount_point) {
        if (this.pyodide === null) {
            await this.get_pyodide();
        }
        this.pyodide.FS.mkdir(mount_point);
        this.pyodide.FS.mount(this.pyodide.FS.filesystems.IDBFS, { "autoPersist":true }, mount_point);
        await this.pyodide.FS.syncfs(true, () => {});
    }

    open_client(page_id,client_options,page_service) {
    }

    close_client(page_id) {
    }
}