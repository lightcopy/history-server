import React from "react";
import Header from "./header";
import Table from "./table";
import Util from "./util";

class Environment extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  componentDidMount() {
    this.updateData(this.props.params.appId);
  }

  updateData(appId) {
    var url = Util.urlGet(`/api/apps/${appId}/environment`, {});
    fetch(url)
    .then(response => response.json())
    .then(json => {
      if (!json.app) throw new Error(`No application ${appId} found`);
      if (!json.env) throw new Error(`No environment found for ${appId}`);
      this.setState({app: json.app, env: json.env});
    })
    .catch(error => {
      console.error(error);
      this.setState({err: `${error}`});
    })
  }

  table(props, title, nameCol, valueCol) {
    // use the same spec, all tables just have different title and column names
    var spec = {
      info: {
        title: title,
        equalColumnWidth: true
      },
      cols: [
        {name: "name", desc: nameCol},
        {name: "value", desc: valueCol}
      ]
    };
    return <Table key={title} spec={spec} data={props} />;
  }

  render() {
    var ok = this.state.app && this.state.env;
    var appId = ok ? this.state.app.appId : null;
    var appName = ok ? this.state.app.appName : null;
    var body = null;
    if (ok) {
      body = [
        this.table(this.state.env.jvmInformation, "Runtime Information", "Name", "Value"),
        this.table(this.state.env.sparkProperties, "Spark Properties", "Name", "Value"),
        this.table(this.state.env.systemProperties, "System Properties", "Name", "Value"),
        this.table(this.state.env.classpathEntries, "Classpath Entries", "Resource", "Source")
      ];
    } else {
      body = <p>{this.state.err}</p>;
    }
    return (
      <div>
        <Header
          appId={appId}
          appName={appName}
          active="environment" />
        <div className="container-fluid">
          <h2>Environment</h2>
          {body}
        </div>
      </div>
    );
  }
}

module.exports = Environment;
