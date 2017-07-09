import React from "react";
import ReactDOM from "react-dom";
import {browserHistory, IndexRedirect, IndexRoute, Router, Route} from "react-router";
import Applications from "./applications";
import Jobs from "./jobs";
import Storage from "./storage";
import Environment from "./environment";
import Executors from "./executors";
import {SQLExecution, SQLExecutionQuery} from "./sqlexecution";

ReactDOM.render((
  <Router history={browserHistory}>
    <Route path="/">
      <IndexRedirect to="apps" />
      <Route path="apps">
        <IndexRoute component={Applications} />
        <Route path=":appId">
          <IndexRedirect to="jobs" />
          <Route path="jobs" component={Jobs} />
          <Route path="storage" component={Storage} />
          <Route path="environment" component={Environment} />
          <Route path="executors" component={Executors} />
          <Route path="sql">
            <IndexRoute component={SQLExecution} />
            <Route path=":id" component={SQLExecutionQuery} />
          </Route>
        </Route>
      </Route>
    </Route>
  </Router>
), document.getElementById("app"));
