import React from "react";
import {Link} from "react-router";
import Header from "./header";
import Table from "./table";
import Util from "./util";
import {LOAD_SUCCESS} from "./loadstatus";

class Applications extends React.Component {
  constructor(props) {
    super(props);
    // table spec
    this.spec = {
      info: {
        title: "Applications",
        equalColumnWidth: false,
        sortCol: "starttime",
        ascending: false,
        paging: true,
        pageSize: 4
      },
      cols: [
        // application settings
        {name: "appId", desc: "App ID", sortable: true, hidden: false},
        {name: "appName", desc: "App name", sortable: true, hidden: false},
        {name: "starttime", desc: "Start time", sortable: true, hidden: false},
        {name: "endtime", desc: "End time", sortable: true, hidden: false},
        {name: "user", desc: "User", sortable: true, hidden: false},
        {name: "appStatus", desc: "App status", sortable: true, hidden: false},
        // application log settings
        {name: "size", desc: "File size", sortable: false, hidden: true},
        {name: "mtime", desc: "Last modified", sortable: false, hidden: true},
        {name: "loadStatus", desc: "Load status", sortable: false, hidden: true}
      ]
    };
    this.state = {data: []};
    this.onStateChanged = this.onStateChanged.bind(this);
    this.updateData = this.updateData.bind(this);
  }

  updateData(page, pageSize, sortBy, asc) {
    var url = Util.urlGet("/api/apps", {
      page: page,
      pageSize: pageSize,
      sortBy: sortBy,
      asc: asc
    });
    fetch(url)
    .then(response => response.json())
    .then(json => {
      // apply preprocessing on field values, json is array
      // only render app link if load status is LOAD_SUCCESS
      for (var i = 0; i < json.length; i++) {
        if (json[i].loadStatus == LOAD_SUCCESS) {
          json[i].appId = <Link to={`/apps/${json[i].appId}/environment`}>{json[i].appId}</Link>;
        }
        json[i].starttime = Util.displayTime(json[i].starttime);
        json[i].endtime = Util.displayTime(json[i].endtime);
        json[i].mtime = Util.displayTime(json[i].mtime);
        json[i].size = Util.displayBytes(json[i].size);
      }
      this.setState({data: json});
    })
    .catch(error => {
      console.error(error);
    })
  }

  /** Table API, invoked when table state has changed */
  onStateChanged(prevState, state) {
    // whether or not we should request data
    var shouldUpdate =
      prevState.currentPage != state.currentPage ||
      prevState.sortCol != state.sortCol ||
      prevState.ascending != state.ascending;
    if (shouldUpdate) {
      this.updateData(state.currentPage, state.pageSize, state.sortCol, state.ascending);
      console.log(state);
    }
  }

  render() {
    return (
      <div>
        <Header />
        <div className="container-fluid">
          <Table spec={this.spec} data={this.state.data} stateChanged={this.onStateChanged} />
        </div>
      </div>
    );
  }
}

module.exports = Applications;
