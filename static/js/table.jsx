import React from "react";

TableHeader = React.createClass({
  onClick(e) {
    var extantSort = Cookie.get(this.props.tableSortKey);
    var newSort =
          (extantSort && extantSort.id == this.props.id) ?
          {id: this.props.id, dir: -extantSort.dir} :
          {id: this.props.id, dir: this.props.defaultSort || 1};

    Cookie.set(this.props.tableSortKey, newSort);

    var extantOpts = Cookie.get(this.props.tableOptsKey) || {};
    var newSortOpt = {};
    this.props.sortKeys.forEach((k) => {
      newSortOpt[k] = newSort.dir;
    });
    var newOpts = jQuery.extend(extantOpts, { sort: newSortOpt });
    Cookie.set(this.props.tableOptsKey, newOpts);
  },
  isCurrentSort() {
    var extantSort = Cookie.get(this.props.tableSortKey);
    return extantSort && extantSort.id == this.props.id ? extantSort.dir : 0;
  },
  render() {
    var opts = Cookie.get(this.props.tableOptsKey) || {};
    var sort = opts.sort;
    var isCurrentSort = this.isCurrentSort();
    return <th onClick={this.onClick}>
      {
        this.props.label +
        (
              (isCurrentSort == 1) ?
                    ' ▴' :
                    ((isCurrentSort == -1) ?
                          ' ▾' :
                          ''
                    )
        )
      }
    </th>
  }
});
