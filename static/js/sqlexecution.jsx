import React from "react";
import {Link} from "react-router";
import Header from "./header";
import Table from "./table";
import Util from "./util";

class QueryDescription extends React.Component {
  constructor(props) {
    super(props);
    this.state = {on: false};
    this.toggleDetails = this.toggleDetails.bind(this);
  }

  toggleDetails() {
    this.setState({on: !this.state.on});
  }

  render() {
    var appId = this.props.appId;
    var hidden = this.state.on ? "" : "hidden";
    return (
      <div style={{minWidth: "300px"}}>
        <Link to={`/apps/${appId}/sql/${this.props.executionId}`}>{this.props.desc}</Link>
        <div className="details pull-right" onClick={this.toggleDetails}>+ details</div>
        <pre className={`details-content ${hidden}`}>{this.props.details}</pre>
      </div>
    );
  }
}

class SQLExecution extends React.Component {
  constructor(props) {
    super(props);
    this.spec = {
      info: {
        title: "Queries",
        equalColumnWidth: false,
        sortCol: "executionId",
        ascending: false,
        paging: true,
        pageSize: 4
      },
      cols: [
        {name: "executionId", desc: "ID", sortable: true, hidden: false},
        {name: "description", desc: "Description", sortable: true, hidden: false},
        {name: "starttime", desc: "Submitted", sortable: true, hidden: false},
        {name: "endtime", desc: "Finished", sortable: true, hidden: false},
        {name: "duration", desc: "Duration", sortable: true, hidden: false},
        {name: "status", desc: "Status", sortable: true, hidden: false},
      ]
    };
    this.state = {data: []};
    this.updateData = this.updateData.bind(this);
  }

  updateData(page, pageSize, sortBy, asc) {
    var appId = this.props.params.appId;
    var url = Util.urlGet(`/api/apps/${appId}/sql`, {
      page: page,
      pageSize: pageSize,
      sortBy: sortBy,
      asc: asc
    });
    fetch(url)
    .then(response => response.json())
    .then(json => {
      for (var i = 0; i < json.length; i++) {
        json[i].description = (
          <QueryDescription
            appId={appId}
            executionId={json[i].executionId}
            desc={json[i].description}
            details={json[i].details} />
        );
        json[i].starttime = Util.displayTime(json[i].starttime);
        json[i].endtime = Util.displayTime(json[i].endtime);
        json[i].duration = Util.displayTimeDiff(json[i].duration);
      }
      this.setState({data: json});
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    return (
      <div>
        <Header
          appId={this.props.params.appId}
          appName="Sample"
          active="sql" />
        <div className="container-fluid">
          <h2>SQL</h2>
          <Table spec={this.spec} data={this.state.data} updateData={this.updateData} />
        </div>
      </div>
    );
  }
}

class SQLExecutionQuery extends React.Component {
  constructor(props) {
    super(props);
    this.state = {query: {}};
  }

  componentDidMount() {
    var appId = this.props.params.appId;
    var executionId = this.props.params.id;
    var url = Util.urlGet(`/api/apps/${appId}/sql/${executionId}`, {});
    fetch(url)
    .then(response => response.json())
    .then(json => {
      this.setState({query: json.sql});
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    var queryDetails = null;
    if (this.state.query) {
      queryDetails = (
        <div>
          <ul className="list-unstyled">
            <li>
              <strong>Description: </strong>
              <span>{this.state.query.description}</span>
            </li>
            <li>
              <strong>Submitted: </strong>
              <span>{Util.displayTime(this.state.query.starttime)}</span>
            </li>
            <li>
              <strong>Completed: </strong>
              <span>{Util.displayTime(this.state.query.endtime)}</span>
            </li>
            <li>
              <strong>Duration: </strong>
              <span>{Util.displayTimeDiff(this.state.query.duration)}</span>
            </li>
            <li>
              <strong>Status: </strong>
              <span>{this.state.query.status}</span>
            </li>
          </ul>
          <h4>Query plan</h4>
          <pre>{this.state.query.physicalPlan}</pre>
        </div>
      );
    }
    return (
      <div>
        <Header
          appId={this.props.params.appId}
          appName="Sample"
          active="sql" />
        <div className="container-fluid">
          <h3>Details for Query {this.props.params.id}</h3>
          {queryDetails}
        </div>
      </div>
    );
  }
}

module.exports = {SQLExecution, SQLExecutionQuery};
