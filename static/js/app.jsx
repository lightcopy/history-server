import React from "react";
import ReactDOM from "react-dom";
import Table from "./table";
import Util from "./util";

class Applications extends React.Component {
  constructor(props) {
    super(props);
    // number of applications per page
    this.pageSize = 100;
    // table spec
    this.spec = {
      info: {
        title: "Applications",
        paging: true
      },
      cols: [
        // application settings
        {name: "appId", desc: "App ID", sortable: true},
        {name: "appName", desc: "App name", sortable: true},
        {name: "starttime", desc: "Start time", sortable: true},
        {name: "endtime", desc: "End time", sortable: true},
        {name: "user", desc: "User", sortable: true},
        // application log settings
        {name: "size", desc: "File size", sortable: false},
        {name: "mtime", desc: "Last modified", sortable: false},
        {name: "status", desc: "Load status", sortable: false}
      ]
    };
    this.state = {data: []};
    this.onStateChanged = this.onStateChanged.bind(this);
    this.updateData = this.updateData.bind(this);
  }

  componentDidMount() {
    // launch with default parameters
    this.updateData(1, this.pageSize, "starttime", false);
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

  onStateChanged(prevState, state) {
    // whether or not we should request data
    var shouldUpdate =
      prevState.currentPage != state.currentPage ||
      prevState.sortCol != state.sortCol ||
      prevState.ascending != state.ascending;
    if (shouldUpdate) {
      // page size is constant
      this.updateData(state.currentPage, this.pageSize, state.sortCol, state.ascending);
      console.log(state);
    }
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
