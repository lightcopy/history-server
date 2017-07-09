import React from "react";
import {Link} from "react-router";
import Details from "./details";
import Header from "./header";
import ProgressBar from "./progress";
import Table from "./table";
import Util from "./util";

class StagesTable extends React.Component {
  constructor(props) {
    super(props);
    this.state = {data: []};
    this.tableSpec = this.tableSpec.bind(this);
    this.formatData = this.formatData.bind(this);
    this.updateData = this.updateData.bind(this);
  }

  tableSpec(stageStatus, title, numStages, showError) {
    var tableTitle = (numStages > 0) ? `${title} (${numStages})` : `${title}`;

    var spec = {
      info: {
        title: tableTitle,
        equalColumnWidth: false,
        sortCol: "uniqueStageId",
        ascending: false,
        paging: true,
        pageSize: 100
      },
      cols: [
        {name: "uniqueStageId", desc: "Stage", sortable: true, hidden: false},
        {name: "stageName", desc: "Description", sortable: true, hidden: false},
        {name: "starttime", desc: "Submitted", sortable: true, hidden: false},
        {name: "endtime", desc: "Finished", sortable: true, hidden: true},
        {name: "duration", desc: "Duration", sortable: true, hidden: false},
        // tasks bar
        {name: "progressBar", desc: "Tasks: Completed/Total", sortable: false, hidden: false},
        // stage metrics
        {name: "metricsInput", desc: "Input", sortable: false, hidden: false},
        {name: "metricsOutput", desc: "Output", sortable: false, hidden: false},
        {name: "metricsShuffleRead", desc: "Shuffle Read", sortable: false, hidden: false},
        {name: "metricsShuffleWrite", desc: "Shuffle Write", sortable: false, hidden: false},
        // failure reason (for failed stages only)
        {name: "failureReason", desc: "Failure Reason", sortable: false, hidden: !showError}
      ]
    };
    return spec;
  }

  formatData(appId, json) {
    for (var i = 0; i < json.length; i++) {
      // stage id, if attempt > 0, then display attempt
      json[i].uniqueStageId = json[i].stageId;
      if (json[i].stageAttemptId > 0) {
        json[i].uniqueStageId += ` (retry ${json[i].stageAttemptId})`;
      }

      // stage name, redirects to stage details
      json[i].stageName = (
        <Details
          to={`/apps/${appId}/stages/${json[i].stageId}/attempt/${json[i].stageAttemptId}`}
          desc={json[i].stageName}
          details={json[i].details}
          minWidth={300} />
      );

      json[i].starttime = Util.displayTime(json[i].starttime);
      json[i].endtime = Util.displayTime(json[i].endtime);
      json[i].duration = Util.displayTimeDiff(json[i].duration);
      // stage does not skip tasks, but can be marked as skipped altogether
      // if stage is skipped, assign all tasks to be skipped
      var skippedTasks = 0;
      if (json[i].activeTasks == 0 && json[i].completedTasks == 0 && json[i].failedTasks == 0) {
        skippedTasks = json[i].totalTasks;
      }
      json[i].progressBar = (
        <ProgressBar
          running={json[i].activeTasks}
          completed={json[i].completedTasks}
          failed={json[i].failedTasks}
          skipped={skippedTasks}
          total={json[i].totalTasks} />
      );

      // metrics
      var inputBytes = json[i].metrics.inputMetrics.inputBytesRead;
      var outputBytes = json[i].metrics.outputMetrics.outputBytesWritten;
      var shuffleReadBytes =
        json[i].metrics.shuffleReadMetrics.shuffleLocalBytesRead +
        json[i].metrics.shuffleReadMetrics.shuffleRemoteBytesRead;
      var shuffleWriteBytes = json[i].metrics.shuffleWriteMetrics.shuffleBytesWritten;

      json[i].metricsInput = (inputBytes > 0) ? Util.displayBytes(inputBytes) : "";
      json[i].metricsOutput = (outputBytes > 0) ? Util.displayBytes(outputBytes) : "";
      json[i].metricsShuffleRead = (shuffleReadBytes > 0) ? Util.displayBytes(shuffleReadBytes) : "";
      json[i].metricsShuffleWrite = (shuffleWriteBytes > 0) ? Util.displayBytes(shuffleWriteBytes) : "";

      // failure reason, only shown or failed stages, we display as details view
      json[i].failureReason = (
        <Details
          desc={json[i].errorDescription}
          details={json[i].errorDetails}
          minWidth={200} />
      );
    }
    return json;
  }

  updateData(page, pageSize, sortBy, asc) {
    // if jobId is defined we fetch for that job only
    var baseUrl;
    if (this.props.jobId) {
      baseUrl = `/api/apps/${this.props.appId}/jobs/${this.props.jobId}/stages`;
    } else {
      baseUrl = `/api/apps/${this.props.appId}/stages`;
    }
    var url = Util.urlGet(baseUrl, {
      status: this.props.stageStatus,
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
    return (
      <Table
        spec={this.tableSpec(this.props.stageStatus, this.props.title, this.props.numStages,
          this.props.showError)}
        data={this.state.data}
        updateData={this.updateData} />
    );
  }
}

class Stages extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      pendingStages: 0,
      activeStages: 0,
      completedStages: 0,
      failedStages: 0,
      skippedStages: 0
    };
    this.stagesCount = this.stagesCount.bind(this);
  }

  stagesCount(json, statusStages) {
    var count = 0, key;
    for (key in json.runningJobs) {
      count += json.runningJobs[key][statusStages] || 0;
    }
    for (key in json.succeededJobs) {
      count += json.succeededJobs[key][statusStages] || 0;
    }
    for (key in json.failedJobs) {
      count += json.failedJobs[key][statusStages] || 0;
    }
    return count;
  }

  componentDidMount() {
    // if job id is available, infer information from it, otherwise fetch application summary
    if (this.props.params.jobId) {
      fetch(`/api/apps/${this.props.params.appId}/jobs/${this.props.params.jobId}`)
      .then(response => response.json())
      .then(json => {
        this.setState({
          jobStatus: json.status,
          pendingStages: json.pendingStages.length,
          activeStages: json.activeStages.length,
          completedStages: json.completedStages.length,
          failedStages: json.failedStages.length,
          skippedStages: json.skippedStages.length
        });
      })
      .catch(error => {
        console.error(error);
      })
    } else {
      // for summary we need to iterate over all jobs and add stages for the same status
      fetch(`/api/apps/${this.props.params.appId}/summary`)
      .then(response => response.json())
      .then(json => {
        this.setState({
          pendingStages: this.stagesCount(json, "pendingStages"),
          activeStages: this.stagesCount(json, "activeStages"),
          completedStages: this.stagesCount(json, "completedStages"),
          failedStages: this.stagesCount(json, "failedStages"),
          skippedStages: this.stagesCount(json, "skippedStages")
        });
      })
      .catch(error => {
        console.error(error);
      })
    }
  }

  render() {
    var jobId = this.props.params.jobId;
    var title = (jobId) ? `Details for job ${jobId}` : "Spark Stages (for all jobs)";
    var activeTab = (jobId) ? "jobs" : "stages";

    var jobStatus = null;
    if (this.state.jobStatus) {
      jobStatus = (
        <li className="margin-bottom-small">
          <strong>Job Status: </strong>
          <span>{this.state.jobStatus}</span>
        </li>
      );
    }

    var activeStagesTable = null;
    if (this.state.activeStages > 0) {
      activeStagesTable = (
        <StagesTable
          appId={this.props.params.appId}
          jobId={jobId}
          title="Active Stages"
          stageStatus="ACTIVE"
          numStages={this.state.activeStages} />
      );
    }

    var pendingStagesTable = null;
    if (this.state.pendingStages > 0) {
      pendingStagesTable = (
        <StagesTable
          appId={this.props.params.appId}
          jobId={jobId}
          title="Pending Stages"
          stageStatus="PENDING"
          numStages={this.state.pendingStages} />
      );
    }

    var completedStagesTable = null;
    if (this.state.completedStages > 0) {
      completedStagesTable = (
        <StagesTable
          appId={this.props.params.appId}
          jobId={jobId}
          title="Completed Stages"
          stageStatus="COMPLETED"
          numStages={this.state.completedStages} />
      );
    }

    var failedStagesTable = null;
    if (this.state.failedStages > 0) {
      failedStagesTable = (
        <StagesTable
          appId={this.props.params.appId}
          jobId={jobId}
          title="Failed Stages"
          stageStatus="FAILED"
          numStages={this.state.failedStages}
          showError={true} />
      );
    }

    var skippedStagesTable = null;
    if (this.state.skippedStages > 0) {
      skippedStagesTable = (
        <StagesTable
          appId={this.props.params.appId}
          jobId={jobId}
          title="Skipped Stages"
          stageStatus="SKIPPED"
          numStages={this.state.skippedStages} />
      )
    }

    return (
      <div>
        <Header appId={this.props.params.appId} active={activeTab} />
        <div className="container-fluid">
          <h2>{title}</h2>
          <div>
            <ul className="list-unstyled">
              {jobStatus}
              <li className="margin-bottom-small">
                <strong>Active Stages: </strong>
                <span>{this.state.activeStages}</span>
              </li>
              <li className="margin-bottom-small">
                <strong>Pending Stages: </strong>
                <span>{this.state.pendingStages}</span>
              </li>
              <li className="margin-bottom-small">
                <strong>Completed Stages: </strong>
                <span>{this.state.completedStages}</span>
              </li>
              <li className="margin-bottom-small">
                <strong>Failed Stages: </strong>
                <span>{this.state.failedStages}</span>
              </li>
              <li className="margin-bottom-small">
                <strong>Skipped Stages: </strong>
                <span>{this.state.skippedStages}</span>
              </li>
            </ul>
          </div>
          {activeStagesTable}
          {pendingStagesTable}
          {completedStagesTable}
          {failedStagesTable}
          {skippedStagesTable}
        </div>
      </div>
    );
  }
}

module.exports = Stages;
