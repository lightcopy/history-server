import React from "react";
import Header from "./header";

// TODO: implement Spark cache/persistence tracking and storage
class Storage extends React.Component {
  render() {
    return (
      <div>
        <Header appId={this.props.params.appId} active="storage" />
        <div className="container-fluid">
          <h2>Storage</h2>
        </div>
      </div>
    );
  }
}

module.exports = Storage;
