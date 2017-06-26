import React from "react";
import ReactDOM from "react-dom";
import Table from "./table";
import Util from "./util";

class Applications extends React.Component {
  constructor(props) {
    super(props);
    // table spec
    this.spec = {
      info: {
        title: "Applications",
        sortCol: "starttime",
        ascending: false,
        paging: true,
        pageSize: 4
      },
      cols: [
        // application settings
        {name: "appId", desc: "App ID", sortable: true},
        {name: "appName", desc: "App name", sortable: true},
        {name: "starttime", desc: "Start time", sortable: true},
        {name: "endtime", desc: "End time", sortable: true},
        {name: "user", desc: "User", sortable: true},
        {name: "appStatus", desc: "App status", sortable: true},
        // application log settings
        {name: "size", desc: "File size", sortable: false},
        {name: "mtime", desc: "Last modified", sortable: false},
        {name: "loadStatus", desc: "Load status", sortable: false}
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
      // apply preprocessing on fields, json is array
      for (var i = 0; i < json.length; i++) {
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
    }
    console.log(state);
  }

  render() {
    return <Table spec={this.spec} data={this.state.data} stateChanged={this.onStateChanged} />;
  }
}

const element = <Applications />;

ReactDOM.render(
  element,
  document.getElementById("main")
);
