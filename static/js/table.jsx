import React from "react";

/*
== Table specification ==
Paging is enabled via flag "paging", pageSize is only considered when paging is enabled, and is
used to hide "next" button and add page size value as part of payload in requests.

const spec = {
  info: {
    title: "Applications",
    sortCol: "starttime",
    ascending: false,
    paging: true,
    pageSize: 100
  },
  cols: [
    {name: "appId", desc: "App ID", sortable: true},
    {name: "appName", desc: "App name", sortable: true},
    {name: "starttime", desc: "Start time", sortable: true},
    {name: "endtime", desc: "End time", sortable: true},
    {name: "user", desc: "User", sortable: true}
  ]
};

== Table data ==
Should contain columns mentioned in spec.

const data = [
  {appId: "app-1", appName: "A", starttime: "01/02/2015", endtime: "01/02/2015", user: "A"},
  {appId: "app-2", appName: "B", starttime: "04/02/2015", endtime: "05/02/2015", user: "B"},
  {appId: "app-3", appName: "C", starttime: "11/02/2015", endtime: "11/02/2015", user: "C"}
];
*/

class TableTitle extends React.Component {
  render() {
    // class name suffix for collapse/expand icon
    var visible = this.props.visible ? "bottom" : "right";
    return (
      <div className="table-title">
        <h4 className="table-title">{this.props.title}</h4>
        <div className="btn-group" role="group" aria-label="Settings">
          <button type="button" className="btn btn-link" aria-label="Expand/collapse" onClick={this.props.expandAction} >
            <span className={"glyphicon glyphicon-triangle-" + visible} aria-hidden="true"></span>
          </button>
        </div>
      </div>
    );
  }
}

class TableHeader extends React.Component {
  render() {
    var sortAction = () => {}, showSort = "", sortClass = "";
    if (this.props.col.sortable) {
      sortAction = () => this.props.sortAction(this.props.col);
      sortClass = "sortable";
      if (this.props.col.name == this.props.sortCol) {
        showSort = "pull-right glyphicon glyphicon-triangle-" + (this.props.sortAsc ? "top" : "bottom");
      }
    }
    return (
      <th className={sortClass} onClick={sortAction}>
        <span>{this.props.col.desc}</span>
        <span className={showSort}></span>
      </th>
    );
  }
}

class TableBody extends React.Component {
  /** Construct column header */
  header(cols, sortAction, sortCol, sortAsc) {
    // total elements and seen ids
    var elems = [], seen = [];
    for (var i = 0; i < cols.length; i++) {
      if (seen.indexOf(cols[i].name) >= 0) {
        throw new Error("Duplicate column " + JSON.stringify(cols[i]) + ", name is not unique");
      }
      seen.push(cols[i].name);
      elems.push(
        <TableHeader
          key={cols[i].name}
          col={cols[i]}
          sortAction={sortAction}
          sortCol={sortCol}
          sortAsc={sortAsc} />
      );
    }
    return <tr>{elems}</tr>;
  }

  /** Convert obj into row based on provided columns */
  row(index, cols, obj) {
    var elems = [];
    for (var i = 0; i < cols.length; i++) {
      // can be either simple string or react component
      var value = obj[cols[i].name];
      elems.push(<td key={i}>{value}</td>);
    }
    return <tr key={index}>{elems}</tr>;
  }

  /** Construct rows based on provided columns and array of data */
  rows(cols, data) {
    var elems = [];
    for (var i = 0; i < data.length; i++) {
      elems.push(this.row("row-" + i, cols, data[i]));
    }
    return elems;
  }

  render() {
    var visible = this.props.visible ? "" : "hidden";
    // header columns as "tr" tag of "th" elements
    var headerCols = this.header(this.props.cols,
      this.props.sortAction, this.props.sortCol, this.props.sortAsc);
    // table rows as "tr" tags
    var tableRows = this.rows(this.props.cols, this.props.data);
    return (
      <table className={"table table-bordered table-striped " + visible}>
        <thead>{headerCols}</thead>
        <tbody>{tableRows}</tbody>
      </table>
    );
  }
}

class Pagination extends React.Component {
  render() {
    var visible = this.props.visible ? "" : "hidden";
    var pagination = null;
    if (this.props.currentPage) {
      var prevDisabled = (this.props.previousPage == null) ? "disabled" : "";
      var prevAction = (this.props.previousPage == null) ? (() => {}) : this.props.previousAction;
      var nextDisabled = (this.props.nextPage == null) ? "disabled" : "";
      var nextAction = (this.props.nextPage == null) ? (() => {}) : this.props.nextAction;

      pagination = (
        <ul className="pagination">
          <li className={prevDisabled}>
            <a href="javascript:void(0);" aria-label="Previous" onClick={prevAction}>
              <span className="glyphicon glyphicon-chevron-left" aria-hidden="true"></span>
            </a>
          </li>
          <li className="active">
            <a href="javascript:void(0);" aria-label="Current">
              <span aria-hidden="true">{this.props.currentPage}</span>
            </a>
          </li>
          <li className={nextDisabled}>
            <a href="javascript:void(0);" aria-label="Next" onClick={nextAction}>
              <span className="glyphicon glyphicon-chevron-right" aria-hidden="true"></span>
            </a>
          </li>
        </ul>
      );
    }
    return <nav className={visible} aria-label="Paging">{pagination}</nav>;
  }
}

class Table extends React.Component {
  constructor(props) {
    super(props);
    // global available state for table:
    // - visible: table is either shown or hidden, title is always displayed
    // - sortCol: column name to sort by, null means no sorting
    // - ascending: if column name is not null, sorts in asc order, otherwise desc
    // - currentPage: if page is null then paging is disabled, otherwise it is current page number
    // - pageSize: number of records per page
    // See `componentDidMount` method
    this.state = {};
    // optional method to trigger when state has changed
    this.stateChanged = props.stateChanged;
    // bind internal methods
    this.toggleVisible = this.toggleVisible.bind(this);
    this.toggleSort = this.toggleSort.bind(this);
    this.togglePrevious = this.togglePrevious.bind(this);
    this.toggleNext = this.toggleNext.bind(this);
  }

  toggleVisible() {
    this.setState({visible: !this.state.visible})
  }

  /** toggle sort for column obj */
  toggleSort(col) {
    if (this.state.sortCol == col.name) {
      this.setState({ascending: !this.state.ascending});
    } else {
      this.setState({sortCol: col.name, ascending: true});
    }
  }

  /** Previous page, null means control is disabled, no pages before current */
  previousPage(currentPage) {
    return (currentPage && currentPage > 1) ? currentPage - 1 : null;
  }

  togglePrevious() {
    this.setState({currentPage: this.previousPage(this.state.currentPage)});
  }

  /** Next page, should always be enabled */
  nextPage(currentPage) {
    return currentPage ? currentPage + 1 : null;
  }

  toggleNext() {
    this.setState({currentPage: this.nextPage(this.state.currentPage)});
  }

  componentDidMount() {
    this.setState({
      visible: true,
      sortCol: this.props.spec.info.sortCol,
      ascending: this.props.spec.info.ascending,
      currentPage: (this.props.spec.info.paging ? 1 : null),
      pageSize: this.props.spec.info.pageSize
    });
  }

  componentDidUpdate(prevProps, prevState) {
    if (this.stateChanged) {
      this.stateChanged(prevState, this.state);
    }
  }

  render() {
    return (
      <div className="table-responsive">
        <TableTitle
          title={this.props.spec.info.title}
          expandAction={this.toggleVisible}
          visible={this.state.visible} />
        <TableBody
          cols={this.props.spec.cols}
          data={this.props.data}
          visible={this.state.visible}
          sortAction={this.toggleSort}
          sortCol={this.state.sortCol}
          sortAsc={this.state.ascending} />
        <Pagination
          visible={this.state.visible}
          previousPage={this.previousPage(this.state.currentPage)}
          previousAction={this.togglePrevious}
          currentPage={this.state.currentPage}
          nextPage={this.nextPage(this.state.currentPage)}
          nextAction={this.toggleNext}
        />
      </div>
    );
  }
}

module.exports = Table;
