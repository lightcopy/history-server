import React from "react";
import {Link} from "react-router";

/**
 * Component to show details button on the right that is expandable.
 * Link is optional, and rendered if "to" is provided
 * Props: "to", "desc", "details", "minWidth" (as int, will be converted into px)
 */
class Details extends React.Component {
  constructor(props) {
    super(props);
    this.state = {on: false};
    this.toggleDetails = this.toggleDetails.bind(this);
  }

  toggleDetails() {
    this.setState({on: !this.state.on});
  }

  render() {
    var appId = this.props.appId;
    var hidden = this.state.on ? "" : "hidden";
    var style = (this.props.minWidth) ? {minWidth: `${this.props.minWidth}px`} : {};
    return (
      <div style={style}>
        {(this.props.to) ? <Link to={this.props.to}>{this.props.desc}</Link> : this.props.desc}
        <div className="details pull-right" onClick={this.toggleDetails}>+ details</div>
        <pre className={`details-content ${hidden}`}>{this.props.details}</pre>
      </div>
    );
  }
}

module.exports = Details;
