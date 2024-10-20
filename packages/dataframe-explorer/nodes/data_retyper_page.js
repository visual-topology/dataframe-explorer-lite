function create_type_change_callback(for_column) {
    return (evt) => {
        skadiui.page_send_message({"action":"set_custom_type","for_column":for_column,"custom_type":evt.target.value});
    }
}

skadiui.set_message_handler((msg) => {
    let table = document.getElementById("table");
    table.innerHTML = "";
    if ("metadata" in msg) {
        let columns = msg.metadata.columns || [];
        columns.forEach(item => {
          let row = document.createElement("tr");
          let name_col = document.createElement("td");
          name_col.appendChild(document.createTextNode(item.name));
          row.appendChild(name_col);
          let type_col = document.createElement("td");
          type_col.appendChild(document.createTextNode(item.type));
          row.appendChild(type_col);
          let convert_select = document.createElement("select");
          convert_select.setAttribute("class","exo-white-bg")
          let types = ["", "number","string","date"].filter(t => t != item.type);
          for(let idx=0; idx<types.length; idx++) {
              let opt = document.createElement("option");
              opt.setAttribute("value",types[idx]);
              opt.appendChild(document.createTextNode(types[idx]));
              convert_select.appendChild(opt);
          }
          convert_select.value = item.custom_type;
          convert_select.addEventListener("change", this.create_type_change_callback(item.name));
          row.appendChild(convert_select);
          let analysis_col = document.createElement("td");
          if (item.analysis) {
              switch(item.custom_type || item.type) {
                 case "number":
                 case "date":
                     let text = "Range: "+item.analysis.range.min + " - " + item.analysis.range.max;
                     analysis_col.appendChild(document.createTextNode(text));
                     break;

                 case "string":
                     let value_table = document.createElement("table");
                     let value_arr = [];
                     for(let value in item.analysis.value_counts) {
                         value_arr.push({"value":value, "count":item.analysis.value_counts[value]});
                     }
                     // sort ascending order
                     value_arr.sort(function(a, b) {
                        return b.count - a.count;
                     });

                     if (value_arr.length > 10) {
                         let remainder_count = 0;
                         for(let idx=10; idx<value_arr.length; idx++) {
                             remainder_count += value_arr[idx].count;
                         }
                         value_arr = value_arr.slice(0,10);
                         value_arr.push({"value":"...","count":remainder_count});
                     }

                     value_arr.forEach((it) => {
                        let tr = document.createElement("tr");
                        let td1 = document.createElement("td");
                        td1.appendChild(document.createTextNode(it.value));
                        tr.appendChild(td1);
                        let td2 = document.createElement("td");
                        td2.appendChild(document.createTextNode(""+it.count));
                        tr.appendChild(td2);
                        value_table.appendChild(tr);
                     });
                     analysis_col.appendChild(value_table);
                     break;
              }
          }

          row.appendChild(analysis_col);
          table.appendChild(row);
        });
    }
});