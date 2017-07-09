import React from "react";
import {Link} from "react-router";
import Header from "./header";
import ProgressBar from "./progress";
import Table from "./table";
import Util from "./util";

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
    return (
      <div>
        <Header appId={this.props.params.appId} active={activeTab} />
        <div className="container-fluid">
          <h2>{title}</h2>
        </div>
      </div>
    );
  }
}

module.exports = Stages;
