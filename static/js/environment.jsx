import React from "react";
import Header from "./header";
import Table from "./table";

class Environment extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  componentDidMount() {
    this.updateData(this.props.params.appId);
  }

  updateData(appId) {
    fetch(`/api/apps/${appId}/environment`)
    .then(response => response.json())
    .then(json => {
      if (json.error) throw new Error(`${json.msg}`);
      this.setState({env: json});
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
        equalColumnWidth: true,
        showMetadata: false
      },
      cols: [
        {name: "name", desc: nameCol},
        {name: "value", desc: valueCol}
      ]
    };
    return <Table id={`environment-table-${title}`} key={title} spec={spec} data={props} />;
  }

  render() {
    var body = null;
    if (this.state.env) {
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
        <Header appId={this.props.params.appId} active="environment" />
        <div className="container-fluid">
          <h2>Environment</h2>
          {body}
        </div>
      </div>
    );
  }
}

module.exports = Environment;
