import React from "react";

/*
== Table specification ==
Paging is enabled via flag "paging", pageSize is only considered when paging is enabled, and is
used to hide "next" button and add page size value as part of payload in requests.

const spec = {
  info: {
    title: "Applications",
    equalColumnWidth: false,
    sortCol: "starttime",
    ascending: false,
    paging: true,
    pageSize: 100
  },
  cols: [
    {name: "appId", desc: "App ID", sortable: true, hidden: false},
    {name: "appName", desc: "App name", sortable: true, hidden: false},
    {name: "starttime", desc: "Start time", sortable: true, hidden: false},
    {name: "endtime", desc: "End time", sortable: true, hidden: false},
    {name: "user", desc: "User", sortable: true, hidden: false}
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
        <button type="button" className="btn btn-link" aria-label="Expand/collapse" onClick={this.props.expandAction} >
          <span className={"glyphicon glyphicon-triangle-" + visible} aria-hidden="true"></span>
        </button>
        <h4 className="table-title">{this.props.title}</h4>
        <button type="button" className="btn btn-link" aria-label="Configure" onClick={this.props.configureAction}>
          <span className="glyphicon glyphicon-cog" aria-hidden="true"></span>
        </button>
      </div>
    );
  }
}

/** Element that shows list of column to show/hide */
class ColumnConfigator extends React.Component {
  constructor(props) {
    super(props);
    this.state = {visible: false};
    this.handleChange = this.handleChange.bind(this);
  }

  handleChange(event) {
    this.props.toggleColumn(event.target.value);
  }

  column(col, displayCols, toggleColumn) {
    var checked = (displayCols.indexOf(col.name) < 0) ? "" : "checked";
    return (
      <div key={col.name} className="checkbox">
        <label className="active">
          <input
            type="checkbox"
            value={col.name}
            checked={checked}
            onChange={this.handleChange} /> {col.desc}
        </label>
      </div>
    );
  }

  columns(cols, displayCols, toggleColumn) {
    // array with initial header
    var arr = [<h5 key="__title">Show/hide columns</h5>];
    for (var i = 0; i < cols.length; i++) {
      arr.push(this.column(cols[i], displayCols, toggleColumn));
    }
    return arr;
  }

  render() {
    var visible = this.props.displayColsEnabled ? "" : "hidden";
    return (
      <div className={visible}>
        {this.columns(this.props.cols, this.props.displayCols, this.props.toggleColumn)}
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
    // filter out columns based on display columns
    var cols = [];
    for (var i = 0; i < this.props.cols.length; i++) {
      if (this.props.displayCols.indexOf(this.props.cols[i].name) >= 0) {
        cols.push(this.props.cols[i]);
      }
    }
    // header columns as "tr" tag of "th" elements
    var headerCols = this.header(cols,
      this.props.sortAction, this.props.sortCol, this.props.sortAsc);
    var equalWidth = this.props.equalColumnWidth ? "equal-width" : "";
    // table rows as "tr" tags
    var tableRows = this.rows(cols, this.props.data);
    return (
      <table className={`table table-bordered table-striped ${equalWidth} ${visible}`}>
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
        <ul className="pagination pagination-sm">
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
    this.updateData = props.updateData;
    // bind internal methods
    this.toggleVisible = this.toggleVisible.bind(this);
    this.toggleSort = this.toggleSort.bind(this);
    this.togglePrevious = this.togglePrevious.bind(this);
    this.toggleNext = this.toggleNext.bind(this);
    this.toggleColumnPanel = this.toggleColumnPanel.bind(this);
    this.toggleColumn = this.toggleColumn.bind(this);
    // `displayCols` is internal state for showing/hiding columns
    // user should use "hidden" attribute for each column in spec
    var displayCols = [];
    for (var i = 0; i < this.props.spec.cols.length; i++) {
      if (this.props.spec.cols[i] && !this.props.spec.cols[i].hidden) {
        displayCols.push(this.props.spec.cols[i].name);
      }
    }
    this.state.displayCols = displayCols;
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

  toggleColumnPanel() {
    this.setState({displayColsEnabled: !this.state.displayColsEnabled})
  }

  toggleColumn(colName) {
    var arr = this.state.displayCols;
    if (arr.indexOf(colName) < 0) {
      arr.push(colName);
    } else {
      arr.splice(arr.indexOf(colName), 1);
    }
    this.setState({displayCols: arr});
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
    // whether or not we should update table data
    var shouldUpdate =
      prevState.currentPage != this.state.currentPage ||
      prevState.sortCol != this.state.sortCol ||
      prevState.ascending != this.state.ascending ||
      prevState.pageSize != this.state.pageSize;
    if (shouldUpdate) {
      // in some cases `updateData` is not defined, therefore no call is made
      if (this.updateData) {
        this.updateData(this.state.currentPage, this.state.pageSize, this.state.sortCol,
          this.state.ascending);
      }
      console.log(this.state);
    }
  }

  render() {
    return (
      <div className="table-responsive">
        <TableTitle
          title={this.props.spec.info.title}
          expandAction={this.toggleVisible}
          configureAction={this.toggleColumnPanel}
          visible={this.state.visible} />
        <ColumnConfigator
          cols={this.props.spec.cols}
          displayColsEnabled={this.state.displayColsEnabled}
          displayCols={this.state.displayCols}
          toggleColumn={this.toggleColumn} />
        <TableBody
          cols={this.props.spec.cols}
          displayCols={this.state.displayCols}
          equalColumnWidth={this.props.spec.info.equalColumnWidth}
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
