import React from "react";
import {Link} from "react-router";
import Header from "./header";
import ProgressBar from "./progress";
import Table from "./table";
import Util from "./util";

// Generic table for jobs
class JobsTable extends React.Component {
  constructor(props) {
    super(props);
    this.state = {data: []};
    this.tableSpec = this.tableSpec.bind(this);
    this.formatData = this.formatData.bind(this);
    this.updateData = this.updateData.bind(this);
  }

  tableSpec(jobStatus, title, numJobs, showError) {
    var tableTitle = (numJobs > 0) ? `${title} (${numJobs})` : `${title}`;

    var spec = {
      info: {
        title: tableTitle,
        equalColumnWidth: false,
        showMetadata: false,
        sortCol: "jobId",
        ascending: false,
        paging: true,
        pageSize: 100
      },
      cols: [
        {name: "jobId", desc: "Job", sortable: true, hidden: false},
        {name: "jobName", desc: "Description", sortable: true, hidden: false},
        {name: "starttime", desc: "Submitted", sortable: true, hidden: false},
        {name: "endtime", desc: "Finished", sortable: true, hidden: true},
        {name: "duration", desc: "Duration", sortable: true, hidden: false},
        // show stages info
        {name: "pendingStages", desc: "Pending Stages", sortable: false, hidden: true},
        {name: "activeStages", desc: "Active Stages", sortable: false, hidden: true},
        {name: "failedStages", desc: "Failed Stages", sortable: false, hidden: true},
        {name: "skippedStages", desc: "Skipped Stages", sortable: false, hidden: true},
        {name: "completedStages", desc: "Completed Stages", sortable: false, hidden: false},
        // error description (exists for failed jobs only)
        {name: "errorDescription", desc: "Errors", sortable: false, hidden: !showError},
        // tasks bar
        {name: "progressBar", desc: "Tasks (for all stages): Completed/Total",
          sortable: false, hidden: false}
      ]
    };
    return spec;
  }

  /**
   * Common function to update data consistently betweeen different job statuses.
   * List is updated in place.
   * @param json list of job objects
   * @return reference to the input list
   */
  formatData(appId, json) {
    for (var i = 0; i < json.length; i++) {
      var jobId = json[i].jobId;
      json[i].jobId = (json[i].jobGroup) ? `${jobId} (${json[i].jobGroup})` : jobId;
      json[i].jobName = <Link to={`/apps/${appId}/jobs/${jobId}`}>{json[i].jobName}</Link>;
      json[i].starttime = Util.displayTime(json[i].starttime);
      json[i].endtime = Util.displayTime(json[i].endtime);
      json[i].duration = Util.displayTimeDiff(json[i].duration);
      json[i].pendingStages = json[i].pendingStages.length;
      json[i].activeStages = json[i].activeStages.length;
      json[i].completedStages = json[i].completedStages.length;
      json[i].failedStages = json[i].failedStages.length;
      json[i].skippedStages = json[i].skippedStages.length;
      json[i].progressBar = (
        <ProgressBar
          running={json[i].activeTasks}
          completed={json[i].completedTasks}
          failed={json[i].failedTasks}
          skipped={json[i].skippedTasks}
          total={json[i].totalTasks} />
      );
    }
    return json;
  }

  updateData(page, pageSize, sortBy, asc) {
    var url = Util.urlGet(`/api/apps/${this.props.appId}/jobs`, {
      status: this.props.jobStatus,
      page: page,
      pageSize: pageSize,
      sortBy: sortBy,
      asc: asc
    });
    fetch(url)
    .then(response => response.json())
    .then(json => {
      this.setState({data: this.formatData(this.props.appId, json)});
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    // table id must be unique for each job table
    return (
      <Table
        id={`jobs-table-${this.props.jobStatus}`}
        spec={this.tableSpec(this.props.jobStatus, this.props.title, this.props.numJobs,
          this.props.showError)}
        data={this.state.data}
        updateData={this.updateData} />
    );
  }
}

class Jobs extends React.Component {
  constructor(props) {
    super(props);
    this.state = {runningJobs: 0, succeededJobs: 0, failedJobs: 0};
  }

  componentDidMount() {
    fetch(`/api/apps/${this.props.params.appId}/summary`)
    .then(response => response.json())
    .then(json => {
      this.setState({
        runningJobs: Util.size(json.runningJobs),
        succeededJobs: Util.size(json.succeededJobs),
        failedJobs: Util.size(json.failedJobs)
      });
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    var runningJobsTable = null;
    if (this.state.runningJobs > 0) {
      runningJobsTable = (
        <JobsTable
          appId={this.props.params.appId}
          title="Running Jobs"
          jobStatus="RUNNING"
          numJobs={this.state.runningJobs} />
      );
    }
    var succeededJobsTable = null;
    if (this.state.succeededJobs > 0) {
      succeededJobsTable = (
        <JobsTable
          appId={this.props.params.appId}
          title="Succeeded Jobs"
          jobStatus="SUCCEEDED"
          numJobs={this.state.succeededJobs} />
      );
    }
    var failedJobsTable = null;
    if (this.state.failedJobs > 0) {
      failedJobsTable = (
        <JobsTable
          appId={this.props.params.appId}
          title="Failed Jobs"
          jobStatus="FAILED"
          showError={true}
          numJobs={this.state.failedJobs} />
      );
    }

    return (
      <div>
        <Header appId={this.props.params.appId} active="jobs" />
        <div className="container-fluid">
          <h2>Spark Jobs</h2>
          <div>
            <ul className="list-unstyled">
              <li className="margin-bottom-small">
                <strong>Running Jobs: </strong>
                <span>{this.state.runningJobs}</span>
              </li>
              <li className="margin-bottom-small">
                <strong>Succeeded Jobs: </strong>
                <span>{this.state.succeededJobs}</span>
              </li>
              <li className="margin-bottom-small">
                <strong>Failed Jobs: </strong>
                <span>{this.state.failedJobs}</span>
              </li>
            </ul>
          </div>
          {runningJobsTable}
          {succeededJobsTable}
          {failedJobsTable}
        </div>
      </div>
    );
  }
}

module.exports = Jobs;
