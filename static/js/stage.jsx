import React from "react";
import Details from "./details";
import Header from "./header";
import Table from "./table";
import Util from "./util";

class StageSummaryTable extends React.Component {
  constructor(props) {
    super(props);
    this.state = {numTasks: 0, data: []};
    this.tableSpec = this.tableSpec.bind(this);
    this.formatRecord = this.formatRecord.bind(this);
    this.formatData = this.formatData.bind(this);
  }

  tableSpec(completedTasks) {
    var spec = {
      info: {
        title: `Summary Metrics for ${completedTasks} Completed Tasks`,
        equalColumnWidth: false,
        showMetadata: false
      },
      cols: [
        {name: "metric", desc: "Metric", sortable: false, hidden: false},
        {name: "min", desc: "Min", sortable: false, hidden: false},
        {name: "prc25", desc: "25th percentile", sortable: false, hidden: false},
        {name: "median", desc: "Median", sortable: false, hidden: false},
        {name: "prc75", desc: "75th percentile", sortable: false, hidden: false},
        {name: "max", desc: "Max", sortable: false, hidden: false}
      ]
    };
    return spec;
  }

  formatRecord(metric, obj, func) {
    var key;
    for (key in obj) {
      if (obj.hasOwnProperty(key) && func) {
        obj[key] = func(obj[key]);
      }
    }
    obj.metric = <strong>{metric}</strong>;
    return obj;
  }

  formatData(json) {
    json.taskDuration = this.formatRecord(
      "Duration", json.taskDuration, Util.displayTimeDiff.bind(Util));
    json.taskDeserializationTime = this.formatRecord(
      "Task Deserialization Time", json.taskDeserializationTime, Util.displayTimeDiff.bind(Util));
    json.gcTime = this.formatRecord(
      "GC Time", json.gcTime, Util.displayTimeDiff.bind(Util));
    json.resultSerializationTime = this.formatRecord(
      "Result Serialization Time", json.resultSerializationTime, Util.displayTimeDiff.bind(Util));
    json.shuffleFetchWaitTime = this.formatRecord(
      "Shuffle Fetch Wait Time", json.shuffleFetchWaitTime, Util.displayTimeDiff.bind(Util));
    json.shuffleRemoteBytesRead = this.formatRecord(
      "Shuffle Remote Bytes Read", json.shuffleRemoteBytesRead, Util.displayBytes.bind(Util));
    json.shuffleLocalBytesRead = this.formatRecord(
      "Shuffle Local Bytes Read", json.shuffleLocalBytesRead, Util.displayBytes.bind(Util));
    json.shuffleTotalRecordsRead = this.formatRecord(
      "Shuffle Total Records Read", json.shuffleTotalRecordsRead);
    json.shuffleWriteTime = this.formatRecord(
      "Shuffle Write Time", json.shuffleWriteTime, Util.displayTimeDiff.bind(Util));
    json.shuffleBytesWritten = this.formatRecord(
      "Shuffle Bytes Written", json.shuffleBytesWritten, Util.displayBytes.bind(Util));
    json.shuffleRecordsWritten = this.formatRecord(
      "Shuffle Records Written", json.shuffleRecordsWritten, false);

    return [
      json.taskDuration,
      json.taskDeserializationTime,
      json.gcTime,
      json.resultSerializationTime,
      json.shuffleFetchWaitTime,
      json.shuffleRemoteBytesRead,
      json.shuffleLocalBytesRead,
      json.shuffleTotalRecordsRead,
      json.shuffleWriteTime,
      json.shuffleBytesWritten,
      json.shuffleRecordsWritten
    ];
  }

  componentDidMount() {
    var appId = this.props.appId;
    var stageId = this.props.stageId;
    var attempt = this.props.attempt;

    fetch(`/api/apps/${appId}/stages/${stageId}/attempt/${attempt}/summary`)
    .then(response => response.json())
    .then(json => {
      this.setState({numTasks: json.numTasks});
      this.setState({data: this.formatData(json)});
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    return (
      <Table
        id="stage-summary-table"
        spec={this.tableSpec(this.state.numTasks)}
        data={this.state.data}
        updateData={this.updateData} />
    );
  }
}

class TaskTable extends React.Component {
  constructor(props) {
    super(props);
    this.state = {data: []};
    this.tableSpec = this.tableSpec.bind(this);
    this.formatData = this.formatData.bind(this);
    this.updateData = this.updateData.bind(this);
  }

  tableSpec(activeTasks, completedTasks, failedTasks, totalTasks) {
    // construct dynamic table title based on number of tasks
    var clauses = [];
    if (totalTasks > 0) {
      clauses.push(`${totalTasks} scheduled in total`);
    }
    if (activeTasks > 0) {
      clauses.push(`${activeTasks} active`);
    }
    if (completedTasks > 0) {
      clauses.push(`${completedTasks} succeeded`);
    }
    if (failedTasks > 0) {
      clauses.push(`${failedTasks} failed`);
    }

    var tableTitle = `Tasks (${clauses.join(", ")})`;
    var spec = {
      info: {
        title: tableTitle,
        equalColumnWidth: false,
        showMetadata: true,
        sortCol: "index",
        ascending: true,
        paging: true,
        pageSize: 100
      },
      cols: [
        {name: "index", desc: "Index", sortable: true, hidden: false},
        {name: "taskId", desc: "ID", sortable: true, hidden: false},
        {name: "attempt", desc: "Attempt", sortable: true, hidden: false},
        {name: "status", desc: "Status", sortable: true, hidden: false},
        {name: "locality", desc: "Locality Level", sortable: true, hidden: false},
        {name: "executorId", desc: "Executor ID", sortable: true, hidden: true},
        {name: "host", desc: "Host", sortable: true, hidden: true},
        {name: "starttime", desc: "Launch Time", sortable: true, hidden: false},
        {name: "duration", desc: "Duration", sortable: true, hidden: false},
        // metrics
        {name: "deserializationTime", desc: "Task Deserialization Time", sortable: false, hidden: true},
        {name: "gcTime", desc: "GC Time", sortable: false, hidden: false},
        {name: "resultSerializationTime", desc: "Result Serialization Time", sortable: false, hidden: true},
        {name: "shuffleReadInfo", desc: "Shuffle Read Size/Records", sortable: false, hidden: false},
        {name: "shuffleWriteInfo", desc: "Shuffle Write Size/Records", sortable: false, hidden: false},
        {name: "shuffleWriteTime", desc: "Shuffle Write Time", sortable: false, hidden: false},
        // error description, modified to include details
        {name: "errorDescription", desc: "Errors", sortable: true, hidden: false}
      ]
    };
    return spec;
  }

  formatData(json) {
    for (var i = 0; i < json.length; i++) {
      json[i].starttime = Util.displayTime(json[i].starttime);
      json[i].duration = Util.displayTimeDiff(json[i].duration);

      // metrics
      json[i].deserializationTime =
        `${Util.displayTimeDiff(json[i].metrics.executorMetrics.executorDeserializeTime)}`;
      json[i].gcTime =
        `${Util.displayTimeDiff(json[i].metrics.jvmGcTime)}`;
      json[i].resultSerializationTime =
        `${Util.displayTimeDiff(json[i].metrics.resultSerializationTime)}`;

      // shuffle read is computed as local + remote
      var shuffleReadBytes =
        json[i].metrics.shuffleReadMetrics.shuffleLocalBytesRead +
        json[i].metrics.shuffleReadMetrics.shuffleRemoteBytesRead;
      var shuffleReadRecords = json[i].metrics.shuffleReadMetrics.shuffleTotalRecordsRead;
      json[i].shuffleReadInfo = `${Util.displayBytes(shuffleReadBytes)} / ${shuffleReadRecords}`;

      // shuffle write
      var shuffleWriteBytes = json[i].metrics.shuffleWriteMetrics.shuffleBytesWritten;
      var shuffleWriteRecords = json[i].metrics.shuffleWriteMetrics.shuffleRecordsWritten;
      json[i].shuffleWriteInfo = `${Util.displayBytes(shuffleWriteBytes)} / ${shuffleWriteRecords}`;
      json[i].shuffleWriteTime =
        `${Util.displayTimeDiff(json[i].metrics.shuffleWriteMetrics.shuffleWriteTime)}`;

      // error description
      if (json[i].errorDescription) {
        json[i].errorDescription = (
          <Details
            desc={json[i].errorDescription}
            details={json[i].errorDetails}
            minWidth={100} />
        );
      }
    }
    return json;
  }

  updateData(page, pageSize, sortBy, asc) {
    var appId = this.props.appId;
    var stageId = this.props.stageId;
    var attempt = this.props.attempt;

    var url = Util.urlGet(`/api/apps/${appId}/stages/${stageId}/attempt/${attempt}/tasks`, {
      page: page,
      pageSize: pageSize,
      sortBy: sortBy,
      asc: asc
    });
    fetch(url)
    .then(response => response.json())
    .then(json => {
      this.setState({data: this.formatData(json)});
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    return (
      <Table
        id="tasks-table"
        spec={this.tableSpec(
          this.props.activeTasks,
          this.props.completedTasks,
          this.props.failedTasks,
          this.props.totalTasks)}
        data={this.state.data}
        updateData={this.updateData} />
    );
  }
}

/**
 * Display single stage attempt information including all tasks and metrics.
 */
class Stage extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  componentDidMount() {
    var appId = this.props.params.appId;
    var stageId = this.props.params.stageId;
    var attempt = this.props.params.attempt;
    fetch(`/api/apps/${appId}/stages/${stageId}/attempt/${attempt}`)
    .then(response => response.json())
    .then(json => {
      this.setState(json);
    })
    .catch(error => {
      console.error(error);
      this.setState({err: `${error}`});
    })
  }

  render() {
    var appId = this.props.params.appId;
    var stageId = this.props.params.stageId;
    var attempt = this.props.params.attempt;

    var stats, stageSummary, tasks;
    // if there is at least one key-value pair we display content
    if (Util.size(this.state) > 0) {
      var duration = Util.displayTimeDiff(this.state.duration);
      var totalTaskTime = Util.displayTimeDiff(this.state.taskTime);
      var shuffleReadBytes = Util.displayBytes(
        this.state.metrics.shuffleReadMetrics.shuffleLocalBytesRead +
        this.state.metrics.shuffleReadMetrics.shuffleRemoteBytesRead
      );
      var shuffleWriteBytes = Util.displayBytes(
        this.state.metrics.shuffleWriteMetrics.shuffleBytesWritten
      );

      stats = (
        <div>
          <ul className="list-unstyled">
            <li className="margin-bottom-small">
              <strong>Description: </strong>
              <code>{this.state.stageName}</code>
            </li>
            <li className="margin-bottom-small">
              <strong>Status: </strong>
              <span>{this.state.status}</span>
            </li>
            <li className="margin-bottom-small">
              <strong>Duration: </strong>
              <span>{duration}</span>
            </li>
            <li className="margin-bottom-small">
              <strong>Total Time Across All Tasks: </strong>
              <span>{totalTaskTime}</span>
            </li>
            <li className="margin-bottom-small">
              <strong>Shuffle Read: </strong>
              <span>{shuffleReadBytes}</span>
            </li>
            <li className="margin-bottom-small">
              <strong>Shuffle Write: </strong>
              <span>{shuffleWriteBytes}</span>
            </li>
          </ul>
        </div>
      );

      stageSummary = (
        <StageSummaryTable
          appId={appId}
          stageId={stageId}
          attempt={attempt} />
      );

      tasks = (
        <TaskTable
          appId={appId}
          stageId={stageId}
          attempt={attempt}
          activeTasks={this.state.activeTasks}
          completedTasks={this.state.completedTasks}
          failedTasks={this.state.failedTasks}
          totalTasks={this.state.totalTasks} />
      );
    } else {
      stats = <p>{this.state.err}</p>;
    }

    return (
      <div>
        <Header appId={appId} active="stages" />
        <div className="container-fluid">
          <h2>{`Details for Stage ${stageId} (Attempt ${attempt})`}</h2>
          {stats}
          {stageSummary}
          {tasks}
        </div>
      </div>
    );
  }
}

module.exports = Stage;
