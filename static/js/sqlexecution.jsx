import React from "react";
import {Link} from "react-router";
import Details from "./details";
import Header from "./header";
import Table from "./table";
import Util from "./util";

class QueryDescription extends React.Component {
  render() {
    return (
      <Details
        to={`/apps/${this.props.appId}/sql/${this.props.executionId}`}
        desc={this.props.desc}
        details={this.props.details}
        minWidth={300} />
    );
  }
}

class SQLExecutionTable extends React.Component {
  constructor(props) {
    super(props);

    this.spec = {
      info: {
        title: "Queries",
        equalColumnWidth: false,
        showMetadata: false,
        sortCol: "executionId",
        ascending: false,
        paging: true,
        pageSize: 100
      },
      cols: [
        {name: "executionId", desc: "ID", sortable: true, hidden: false},
        {name: "description", desc: "Description", sortable: true, hidden: false},
        {name: "starttime", desc: "Submitted", sortable: true, hidden: false},
        {name: "endtime", desc: "Finished", sortable: true, hidden: false},
        {name: "duration", desc: "Duration", sortable: true, hidden: false},
        {name: "status", desc: "Status", sortable: true, hidden: false},
        {name: "jobs", desc: "Jobs", sortable: false, hidden: false}
      ]
    };
    this.state = {data: []};
    this.updateData = this.updateData.bind(this);
    this.generateTableHeader = this.generateTableHeader.bind(this);
  }

  /** Generate table title based on number of queries */
  generateTableHeader(numRunningQueries, numCompletedQueries) {
    if (numCompletedQueries > 0 && numRunningQueries > 0) {
      return `Queries (${numCompletedQueries} completed, ${numRunningQueries} running)`;
    } else if (numCompletedQueries > 0) {
      return `Queries (${numCompletedQueries} completed)`;
    } else if (numRunningQueries > 0) {
      return `Queries (${numRunningQueries} running)`;
    } else {
      return "Queries";
    }
  }

  updateData(page, pageSize, sortBy, asc) {
    var appId = this.props.appId;
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
        // add job link
        var elems = [];
        for (var j = 0; j < json[i].jobIds.length; j++) {
          var jobId = json[i].jobIds[j];
          var elem = (
            <div key={jobId}>
              <Link to={`/apps/${appId}/jobs/${jobId}`}>{`Job ${jobId}`}</Link>
            </div>
          );
          elems.push(elem);
        }
        json[i].jobs = <div>{elems}</div>;
      }
      this.setState({data: json});
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    // update table title based on number of queries
    this.spec.info.title = this.generateTableHeader(
      this.props.numRunningQueries, this.props.numCompletedQueries);
    return(
      <Table
        id="sqlexecution-table"
        spec={this.spec}
        data={this.state.data}
        updateData={this.updateData} />
    );
  }
}

class SQLExecution extends React.Component {
  constructor(props) {
    super(props);
    this.state = {runningQueries: 0, completedQueries: 0};
  }

  componentDidMount() {
    fetch(`/api/apps/${this.props.params.appId}/summary`)
    .then(response => response.json())
    .then(json => {
      this.setState({
        runningQueries: json.runningQueries,
        completedQueries: json.completedQueries
      });
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    return (
      <div>
        <Header appId={this.props.params.appId} active="sql" />
        <div className="container-fluid">
          <h2>SQL</h2>
          <SQLExecutionTable
            appId={this.props.params.appId}
            numRunningQueries={this.state.runningQueries}
            numCompletedQueries={this.state.completedQueries} />
        </div>
      </div>
    )
  }
}

class SQLExecutionQuery extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
    this.linkJobs = this.linkJobs.bind(this);
  }

  componentDidMount() {
    var appId = this.props.params.appId;
    var executionId = this.props.params.id;
    fetch(`/api/apps/${appId}/sql/${executionId}`)
    .then(response => response.json())
    .then(json => {
      if (json.error) throw new Error(`${json.msg}`);
      this.setState({query: json});
    })
    .catch(error => {
      console.error(error);
      this.setState({err: `${error}`});
    })
  }

  /** Generate list of jobs as navigation links */
  linkJobs(appId, jobs) {
    var elems = [];
    for (var i = 0; i < jobs.length; i++) {
      var jobId = jobs[i];
      var elem = (
        <span key={jobId}>
          <Link to={`/apps/${appId}/jobs/${jobId}`}>{jobId}</Link>
          <span>{(i < jobs.length - 1) ? ", " : ""}</span>
        </span>
      );
      elems.push(elem);
    }
    return elems;
  }

  render() {
    var body = null;
    if (this.state.query) {
      body = (
        <div>
          <ul className="list-unstyled">
            <li className="margin-bottom-small">
              <strong>Description: </strong>
              <code>{this.state.query.description}</code>
            </li>
            <li className="margin-bottom-small">
              <strong>Submitted: </strong>
              <span>{Util.displayTime(this.state.query.starttime)}</span>
            </li>
            <li className="margin-bottom-small">
              <strong>Completed: </strong>
              <span>{Util.displayTime(this.state.query.endtime)}</span>
            </li>
            <li className="margin-bottom-small">
              <strong>Duration: </strong>
              <span>{Util.displayTimeDiff(this.state.query.duration)}</span>
            </li>
            <li className="margin-bottom-small">
              <strong>Status: </strong>
              <span>{this.state.query.status}</span>
            </li>
            <li className="margin-bottom-small">
              <strong>Jobs: </strong>
              <span>{this.linkJobs(this.props.params.appId, this.state.query.jobIds)}</span>
            </li>
          </ul>
          <h4 className="margin-top-large">Query plan</h4>
          <pre>{this.state.query.physicalPlan}</pre>
        </div>
      );
    } else {
      body = <p>{this.state.err}</p>;
    }
    return (
      <div>
        <Header appId={this.props.params.appId} active="sql" />
        <div className="container-fluid">
          <h2>Details for Query {this.props.params.id}</h2>
          {body}
        </div>
      </div>
    );
  }
}

module.exports = {SQLExecution, SQLExecutionQuery};
