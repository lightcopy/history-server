import React from "react";
import ReactDOM from "react-dom";
import {browserHistory, IndexRoute, Router, Route} from "react-router";
import Applications from "./applications";
import Environment from "./environment";

ReactDOM.render((
  <Router history={browserHistory}>
    <Route path="/" component={Applications} />
    <Route path="/apps/:appId">
      <Route path="environment" component={Environment} />
    </Route>
  </Router>
), document.getElementById("app"));
