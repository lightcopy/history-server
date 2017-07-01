import React from "react";
import {Link} from "react-router";

class Tab extends React.Component {
  render() {
    var active = this.props.active ? "active" : "";
    return (
      <li className={active}>
        {this.props.link}
      </li>
    );
  }
}

/**
 * Menu header.
 * Props:
 * `active` - active tab name
 * `appId` - application id
 */
class Header extends React.Component {
  constructor(props) {
    super(props);
    this.tabNames = [
      {name: "jobs", desc: "Jobs"},
      {name: "stages", desc: "Stages"},
      {name: "storage", desc: "Storage"},
      {name: "environment", desc: "Environment"},
      {name: "executors", desc: "Executors"},
      {name: "sql", desc: "SQL"}
    ]
    this.tabs = this.tabs.bind(this);
    this.state = {};
  }

  tabs(active, appId) {
    var elems = [];
    for (var i = 0; i < this.tabNames.length; i++) {
      var link = (
        <Link to={`/apps/${appId}/${this.tabNames[i].name}`}>
          <span>{this.tabNames[i].desc}</span>
        </Link>
      );
      var elem = <Tab
        key={this.tabNames[i].name}
        name={this.tabNames[i].desc}
        link={link}
        active={this.tabNames[i].name == active} />
      elems.push(elem);
    }
    return elems;
  }

  componentDidMount() {
    if (this.props.appId) {
      fetch(`/api/apps/${this.props.appId}`)
      .then(response => response.json())
      .then(json => {
        if (json.error) throw new Error(`${json.msg}`);
        this.setState({app: json});
      })
      .catch(error => {
        console.error(error);
      })
    }
  }

  render() {
    // show tabs only if appId is defined
    var controls = null;
    if (this.state.app) {
      controls = (
        <div className="collapse navbar-collapse">
          <ul className="nav navbar-nav">
            {this.tabs(this.props.active, this.state.app.appId)}
          </ul>
          <ul className="nav navbar-nav navbar-right">
            <li>
              <strong className="navbar-text truncate">{this.state.app.appName}</strong>
            </li>
          </ul>
        </div>
      );
    }
    return (
      <nav className="navbar navbar-bgd navbar-default navbar-static-top">
        <div className="container-fluid">
          <div className="navbar-header">
            <Link className="brand" to="/">
              <img alt="Spark" src="/img/spark-logo-77x50px-hd.png" />
            </Link>
          </div>
          {controls}
        </div>
      </nav>
    );
  }
}

module.exports = Header;
