import React from "react";
import ReactDOM from "react-dom";

class Application extends React.Component {
  constructor() {
    super();
    this.state = {apps: []};
  }

  componentDidMount() {
    fetch("/api/apps")
    .then(response => response.json())
    .then(json => {
      this.setState({apps: json});
    })
    .catch(error => {
      console.error(error);
    })
  }

  render() {
    var apps = this.state.apps;
    return <h1>Hello world!</h1>;
  }
}

const element = <Application />;

ReactDOM.render(
  element,
  document.getElementById("main")
);
