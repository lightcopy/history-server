import React from "react";
import Header from "./header";
import Table from "./table";
import Util from "./util";

/**
 * Common function to update data consistently betweeen active and removed executors.
 * List is updated in place.
 * @param json list of executor objects
 * @return reference to the input list
 */
function formatExecutorData(json) {
  for (var i = 0; i < json.length; i++) {
    json[i].sortExecutorId = json[i].executorId;
    json[i].address = `${json[i].host}:${json[i].port}`;
    json[i].starttime = Util.displayTime(json[i].starttime);
    json[i].endtime = Util.displayTime(json[i].endtime);
    json[i].duration = Util.displayTimeDiff(json[i].duration);
    json[i].storageMemory = Util.displayBytes(json[i].maxMemory);
    json[i].taskTime = Util.displayTimeDiff(json[i].taskTime);
    json[i].metricGcTime = Util.displayTimeDiff(json[i].metrics.jvmGcTime);
    json[i].metricInput = Util.displayBytes(json[i].metrics.inputMetrics.inputBytesRead);
    // shuffle read bytes are displayed as total (local + remote)
    json[i].metricShuffleRead = Util.displayBytes(
      json[i].metrics.shuffleReadMetrics.shuffleLocalBytesRead +
      json[i].metrics.shuffleReadMetrics.shuffleRemoteBytesRead);
    json[i].metricShuffleWrite = Util.displayBytes(
      json[i].metrics.shuffleWriteMetrics.shuffleBytesWritten);
  }
  return json;
}

class ActiveExecutors extends React.Component {
  constructor(props) {
    super(props);

    this.spec = {
      info: {
        title: "Active Executors",
        equalColumnWidth: false,
        showMetadata: false,
        sortCol: "sortExecutorId",
        ascending: true,
        paging: true,
        pageSize: 100
      },
      cols: [
        {name: "sortExecutorId", desc: "Executor", sortable: true, hidden: false},
        {name: "address", desc: "Address", sortable: false, hidden: false},
        {name: "starttime", desc: "Start Time", sortable: false, hidden: false},
        {name: "status", desc: "Status", sortable: false, hidden: true},
        {name: "storageMemory", desc: "Storage Memory", sortable: false, hidden: false},
        {name: "cores", "desc": "Cores", sortable: false, hidden: false},
        {name: "activeTasks", "desc": "Active Tasks", sortable: false, hidden: false},
        {name: "completedTasks", "desc": "Completed Tasks", sortable: false, hidden: false},
        {name: "failedTasks", "desc": "Failed Tasks", sortable: false, hidden: false},
        {name: "totalTasks", "desc": "Total Tasks", sortable: false, hidden: false},
        {name: "taskTime", "desc": "Task Time", sortable: false, hidden: true},
        {name: "metricGcTime", "desc": "GC Time", sortable: false, hidden: true},
        {name: "metricInput", "desc": "Input", sortable: false, hidden: true},
        {name: "metricShuffleRead", "desc": "Shuffle Read", sortable: false, hidden: false},
        {name: "metricShuffleWrite", "desc": "Shuffle Write", sortable: false, hidden: false}
      ]
    };
    this.state = {data: []};
    this.updateData = this.updateData.bind(this);
    this.generateTableHeader = this.generateTableHeader.bind(this);
  }

  generateTableHeader(numActiveExecutors) {
    if (numActiveExecutors > 0) {
      return `Active Executors (${numActiveExecutors})`;
    } else {
      return "Active Executors";
    }
  }

  updateData(page, pageSize, sortBy, asc) {
    var url = Util.urlGet(`/api/apps/${this.props.appId}/executors`, {
      status: "ACTIVE",
      page: page,
      pageSize: pageSize,
      sortBy: sortBy,
      asc: asc
    });
    fetch(url)
    .then(response => response.json())
    .then(json => {
      this.setState({data: formatExecutorData(json)});
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    this.spec.info.title = this.generateTableHeader(this.props.numActiveExecutors);
    return (
      <Table
        id="executors-table-ACTIVE"
        spec={this.spec}
        data={this.state.data}
        updateData={this.updateData} />
    );
  }
}

class RemovedExecutors extends React.Component {
  constructor(props) {
    super(props);

    this.spec = {
      info: {
        title: "Removed Executors",
        equalColumnWidth: false,
        showMetadata: false,
        sortCol: "sortExecutorId",
        ascending: true,
        paging: true,
        pageSize: 100
      },
      cols: [
        {name: "sortExecutorId", desc: "Executor", sortable: true, hidden: false},
        {name: "address", desc: "Address", sortable: false, hidden: false},
        {name: "starttime", desc: "Start Time", sortable: false, hidden: false},
        {name: "endtime", desc: "End Time", sortable: false, hidden: false},
        {name: "duration", desc: "Duration", sortable: false, hidden: false},
        {name: "failureReason", desc: "Failure Reason", sortable: false, hidden: false},
        {name: "status", desc: "Status", sortable: false, hidden: true},
        {name: "storageMemory", desc: "Storage Memory", sortable: false, hidden: false},
        {name: "cores", "desc": "Cores", sortable: false, hidden: false},
        {name: "activeTasks", "desc": "Active Tasks", sortable: false, hidden: true},
        {name: "completedTasks", "desc": "Completed Tasks", sortable: false, hidden: true},
        {name: "failedTasks", "desc": "Failed Tasks", sortable: false, hidden: false},
        {name: "totalTasks", "desc": "Total Tasks", sortable: false, hidden: false},
        {name: "taskTime", "desc": "Task Time", sortable: false, hidden: true},
        {name: "metricGcTime", "desc": "GC Time", sortable: false, hidden: true},
        {name: "metricInput", "desc": "Input", sortable: false, hidden: true},
        {name: "metricShuffleRead", "desc": "Shuffle Read", sortable: false, hidden: true},
        {name: "metricShuffleWrite", "desc": "Shuffle Write", sortable: false, hidden: true}
      ]
    };
    this.state = {data: []};
    this.updateData = this.updateData.bind(this);
    this.generateTableHeader = this.generateTableHeader.bind(this);
  }

  generateTableHeader(numRemovedExecutors) {
    if (numRemovedExecutors > 0) {
      return `Removed Executors (${numRemovedExecutors})`;
    } else {
      return "Removed Executors";
    }
  }

  updateData(page, pageSize, sortBy, asc) {
    var url = Util.urlGet(`/api/apps/${this.props.appId}/executors`, {
      status: "REMOVED",
      page: page,
      pageSize: pageSize,
      sortBy: sortBy,
      asc: asc
    });
    fetch(url)
    .then(response => response.json())
    .then(json => {
      this.setState({data: formatExecutorData(json)});
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    this.spec.info.title = this.generateTableHeader(this.props.numRemovedExecutors);
    return (
      <Table
        id="executors-table-REMOVED"
        spec={this.spec}
        data={this.state.data}
        updateData={this.updateData} />
    );
  }
}

class Executors extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  componentDidMount() {
    fetch(`/api/apps/${this.props.params.appId}/summary`)
    .then(response => response.json())
    .then(json => {
      this.setState({
        activeExecutors: json.activeExecutors.length,
        removedExecutors: json.removedExecutors.length
      });
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    var activeExecutorsTable = null;
    if (this.state.activeExecutors > 0) {
      activeExecutorsTable = (
        <ActiveExecutors
          appId={this.props.params.appId}
          numActiveExecutors={this.state.activeExecutors} />
      );
    }
    var removedExecutorsTable = null;
    if (this.state.removedExecutors > 0) {
      removedExecutorsTable = (
        <RemovedExecutors
          appId={this.props.params.appId}
          numRemovedExecutors={this.state.removedExecutors} />
      );
    }

    return (
      <div>
        <Header appId={this.props.params.appId} active="executors" />
        <div className="container-fluid">
          <h2>Executors</h2>
          {activeExecutorsTable}
          {removedExecutorsTable}
        </div>
      </div>
    );
  }
}

module.exports = Executors;
