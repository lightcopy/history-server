import React from "react";

/**
 * Display progress bar for tasks.
 * Props: running, completed, failed, skipped, total
 */
class ProgressBar extends React.Component {
  render() {
    var running = this.props.running;
    var completed = this.props.completed;
    var failed = this.props.failed;
    var skipped = this.props.skipped;
    var total = this.props.total;

    if (total - running - completed - failed - skipped < 0) {
      throw new Error(`Mismatch: ${total} < ${running} + ${completed} + ${failed} + ${skipped}`);
    }

    // build label
    var clauses = [];
    if (skipped > 0) {
      clauses.push(`${skipped} skipped`);
    }
    if (failed > 0) {
      clauses.push(`${failed} failed`);
    }
    if (running > 0) {
      clauses.push(`${running} running`);
    }

    var label = `${completed}/${total}`;
    if (clauses.length > 0) {
      label += ` (${clauses.join(", ")})`;
    }

    return (
      <div className="progress">
        <span className="progress-label">{label}</span>
        <BarStripe bar="progress-bar-skipped" ratio={skipped / total} />
        <BarStripe bar="progress-bar-completed" ratio={completed / total} />
        <BarStripe bar="progress-bar-failed" ratio={failed / total} />
        <BarStripe bar="progress-bar-running" ratio={running / total} />
      </div>
    );
  }
}

/**
 * Individual colour stripe for each task.
 * Props: ratio - double value (0 <= x <= 1) for width.
 */
class BarStripe extends React.Component {
  constructor(props) {
    super(props);
    this.width = this.width.bind(this);
  }

  /** Get "width" attribute for ratio */
  width(ratio) {
    return `${(ratio || 0) * 100}%`;
  }

  render() {
    var bar = this.props.bar;
    var style = {width: this.width(this.props.ratio)};
    return (
      <div className={`progress-bar ${bar}`} style={style}></div>
    )
  }
}

module.exports = ProgressBar;
