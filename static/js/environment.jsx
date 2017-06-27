import React from "react";
import Header from "./header";

class Environment extends React.Component {
  render() {
    return (
      <div>
        <Header
          appId={this.props.params.appId}
          appName="Application"
          active="environment" />
        <div className="container-fluid">
          <h2>Environment</h2>
        </div>
      </div>
    );
  }
}

module.exports = Environment;

// Runtime Information (Name - Value)
// Spark Properties (Name - Value)
// System Properties (Name - Value)
// Classpath Entries (Resource - Source)
