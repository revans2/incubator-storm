/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
$(function () {
    $(".js-only").show();
});

//Add in custom sorting for some data types
$.extend( $.fn.dataTableExt.oSort, {
  "time-str-pre": function (raw) {
    var s = $(raw).text();
    if (s == "") {
      s = raw;
    }
    if (s.search('All time') != -1) {
      return 1000000000;
    }
    var total = 0;
    $.each(s.split(' '), function (i, v) {
       var amt = parseInt(v);
       if (v.search('ms') != -1) {
         total += amt;
       } else if (v.search('s') != -1) {
         total += amt * 1000;
       } else if (v.search('m') != -1) {
         total += amt * 1000 * 60;
       } else if (v.search('h') != -1) {
         total += amt * 1000 * 60 * 60;
       } else if (v.search('d') != -1) {
         total += amt * 1000 * 60 * 60 * 24;
       }
     });
     return total;
   },
   "time-str-asc": function ( a, b ) {
      return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },
    "time-str-desc": function ( a, b ) {
      return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    }
});

function dtAutoPage(selector, conf) {
  if ($(selector.concat(" tr")).length <= 20) {
    $.extend(conf, {paging: false});
  }
  return $(selector).DataTable(conf);
}

function toggleSys() {
    var sys = $.cookies.get('sys') || false;
    sys = !sys;

    var exDate = new Date();
    exDate.setDate(exDate.getDate() + 365);

    $.cookies.set('sys', sys, {'path':'/', 'expiresAt':exDate.toUTCString()});
    window.location = window.location;
}

function ensureInt(n) {
    var isInt = /^\d+$/.test(n);
    if (!isInt) {
        alert("'" + n + "' is not integer.");
    }

    return isInt;
}

function sendRequest(id, action, extra, body, cb){
   var opts = {
        type:'POST',
        url:'/api/v1/topology/' + id + '/' + action
    };

    if (body) {
        opts.data = JSON.stringify(body);
        opts.contentType = 'application/json; charset=utf-8';
    }

    opts.url += extra ? "/" + extra : "";

    $.ajax(opts).always(function(data){
        cb (data);
    }).fail (function(){
        alert("Error while communicating with Nimbus.");
    });
}

function confirmAction(id, name, action, wait, defaultWait) {
    var opts = {
        type:'POST',
        url:'/api/v1/topology/' + id + '/' + action
    };
    if (wait) {
        var waitSecs = prompt('Do you really want to ' + action + ' topology "' + name + '"? ' +
                              'If yes, please, specify wait time in seconds:',
                              defaultWait);

        if (waitSecs != null && waitSecs != "" && ensureInt(waitSecs)) {
            opts.url += '/' + waitSecs;
        } else {
            return false;
        }
    } else if (!confirm('Do you really want to ' + action + ' topology "' + name + '"?')) {
        return false;
    }

    $("input[type=button]").attr("disabled", "disabled");
    $.ajax(opts).done(function () {
        window.location.reload();
    }).fail(function () {
        alert("Error while communicating with Nimbus.");
    });

    return false;
}

$(function () {
  $('[data-toggle="tooltip"]').tooltip()
})

function formatConfigData(data) {
    var mustacheFormattedData = {'config':[]};
    for (var prop in data) {
       if(data.hasOwnProperty(prop)) {
           mustacheFormattedData['config'].push({
               'key': prop,
               'value': JSON.stringify(data[prop])
           });
       }
    }
    return mustacheFormattedData;
}

function formatErrorTimeSecs(response){
    var errors = response["componentErrors"];
    for(var i = 0 ; i < errors.length ; i++){
        var time = errors[i]['time'];
        errors[i]['time'] = moment.utc(time).local().format("ddd, DD MMM YYYY HH:mm:ss Z");
    }
    return response;
}


function renderToggleSys(div) {
    var sys = $.cookies.get("sys") || false;
    if(sys) {
       div.append("<span data-original-title=\"Use this to toggle inclusion of storm system components.\" class=\"tip right\"><input onclick=\"toggleSys()\" value=\"Hide System Stats\" type=\"button\" class=\"btn btn-default\"></span>");
    } else {
       div.append("<span class=\"tip right\" title=\"Use this to toggle inclusion of storm system components.\"><input onclick=\"toggleSys()\" value=\"Show System Stats\" type=\"button\" class=\"btn btn-default\"></span>");
    }
}

function topologyActionJson(id, encodedId, name,status,msgTimeout) {
    var jsonData = {};
    jsonData["id"] = id;
    jsonData["encodedId"] = encodedId;
    jsonData["name"] = name;
    jsonData["msgTimeout"] = msgTimeout;
    jsonData["activateStatus"] = (status === "INACTIVE") ? "enabled" : "disabled";
    jsonData["deactivateStatus"] = (status === "ACTIVE") ? "enabled" : "disabled";
    jsonData["rebalanceStatus"] = (status === "ACTIVE" || status === "INACTIVE" ) ? "enabled" : "disabled";
    jsonData["killStatus"] = (status !== "KILLED") ? "enabled" : "disabled";
    return jsonData;
}

function topologyActionButton(id,name,status,actionLabel,command,wait,defaultWait) {
    var buttonData = {};
    buttonData["buttonStatus"] = status ;
    buttonData["actionLabel"] = actionLabel;
    buttonData["command"] = command;
    buttonData["isWait"] = wait;
    buttonData["defaultWait"] = defaultWait;
    return buttonData;
}

$.blockUI.defaults.css = {
    border: 'none',
    padding: '15px',
    backgroundColor: '#000',
    '-webkit-border-radius': '10px',
    '-moz-border-radius': '10px',
    'border-radius': '10px',
    opacity: .5,
    color: '#fff',margin:0,width:"30%",top:"40%",left:"35%",textAlign:"center"
};

function makeSupervisorWorkerStatsTable (response, elId) {
    makeWorkerStatsTable (response, elId, "supervisor");
};

function makeTopologyWorkerStatsTable (response, elId) {
    makeWorkerStatsTable (response, elId, "topology");
};

// Show at most 10 components per worker
// if the worker has more than 10 components, show a button instead that when clicked
// opens a row below the worker's row with the components
// Note that components are still searchable in the datatable
var maxInlineComponents = 10;

// Build a table of per-worker resources and components (when permitted)
var makeWorkerStatsTable = function (response, elId, type) {
    var showCpu = response.schedulerDisplayResource;
    var formatComponents = function (row) {
        var result = '';
        Object.keys(row.componentNumTasks || {}).forEach (function (component){
            var numTasks = row.componentNumTasks[component];
            result += '<a class="worker-component-button btn btn-xs btn-primary" href="/component.html?id=' + 
                            component + '&topology_id=' + row.topologyId + '">';
            result += component;
            result += '<span class="badge">' + numTasks + '</span>';
            result += '</a>';
        });
        return result;
    };

    var format = function (row){
        var result = '<div class="worker-child-row">Worker components: ';
        result += formatComponents (row);
        result += '</div>';
        return result;
    };

    var columns = [
        {
            data: 'host', 
            render: function (data, type, row){
                return type === 'display' ? 
                    ('<a href="/supervisor.html?host=' + data + '">' + data + '</a>') :
                    row.topologyId;
            }
        },
        {
            data: 'port',
            render: function (data, type, row) {
                var logLink = row.workerLogLink;
                return type === 'display' ?
                    ('<a href="' + logLink + '">' + data + '</a>'): 
                    data;
            }
        },
        { data: 'uptime' },
        { data: 'executorsTotal' },
        { 
            data: function (row){
                return row.assignedMemOnHeap + row.assignedMemOffHeap;
            }
        },
    ];

    if (showCpu) {
        columns.push ({ data: 'assignedCpu' });
    }

    columns.push ({ 
        data: function (row, type, obj, dt) {
            var components = Object.keys(row.componentNumTasks || {});
            if (components.length === 0){
                // if no components returned, it means the worker
                // topology isn't one the user is authorized to see
                return "N/A";
            }

            if (type !== 'display'){
                return components
            }

            var result = '';
            var expand = components.length > maxInlineComponents;

            if (!expand) {
                result += '<div>';
                result += formatComponents (row);
                result += '</div>';
            } else {
                // we want an expand button 
                result += '<button class="btn btn-xs btn-info details-control" type="button">' + 
                    components.length + ' components</button>';
            }
            return result;
        }
    });

    switch (type){
        case 'topology':
            columns.unshift ({
                data: 'supervisorId', 
                render: function (data, type, row){
                    return type === 'display' ? 
                        ('<a href="/supervisor.html?id=' + data + '">' + data + '</a>') :
                        data;
                }
            });
        break;
        case 'supervisor':
            columns.unshift ({
                data: function (row, type){
                    return type === 'display' ? 
                        ('<a href="/topology.html?id=' + row.topologyId + '">' + row.topologyName + '</a>') :
                        row.topologyId;
                }
            });
        break;
    }

    var workerStatsTable = dtAutoPage(elId, {
        data: response.workers,
        autoWidth: false,
        columns: columns
    });

    // Add event listener for opening and closing details
    $(elId + ' tbody').on('click', 'button.details-control', function () {
        var tr = $(this).closest('tr');
        var row = workerStatsTable.row( tr );

        if (row.child.isShown()) {
            // This row is already open - close it
            row.child.hide();
            tr.removeClass('shown');
        } else {
            // Open this row
            row.child (format (row.data())).show();
            tr.addClass('shown');
        }
    });

    $(elId + ' [data-toggle="tooltip"]').tooltip();
};
