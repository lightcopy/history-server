module.exports = {
  /**
   * Return size (number of keys) for javascript object.
   * @param obj javascript object
   * @return length as integer
   */
  size: function(obj) {
    if (!obj) return 0;
    var size = 0, key;
    for (key in obj) {
      if (obj.hasOwnProperty(key)) {
        size++;
      }
    }
    return size;
  },

  /**
   * Construct GET request url with params.
   * If value for key is null, parameter is removed from final result.
   * @param url base url string
   * @param params object with key-value pairs
   * @return url with included parameters
   */
  urlGet: function(url, params) {
    if (params) {
      var arr = [];
      for (var key in params) {
        if (params.hasOwnProperty(key) && params[key] != null) {
          arr.push(decodeURIComponent(key) + "=" + decodeURIComponent(params[key]));
        }
      }
      url = url + "?" + arr.join("&");
    }
    return url;
  },

  /**
   * Round number to certain decimal places.
   * @param num number to round
   * @param decimal decimal places
   * @return updated number
   */
  rnd: function(num, decimal) {
    return Math.round(num * Math.pow(10, decimal)) / Math.pow(10, decimal);
  },

  /**
   * Display milliseconds as datetime in human readable format.
   * By default "en-nz" locale is used to display time.
   * @param timestamp unix timestamp in milliseconds
   * @return datetime as string
   */
  displayTime: function(timestamp) {
    // unknown time
    if (timestamp < 0) {
      return "-";
    }
    var date = new Date(timestamp), locale = "en-nz";
    return date.toLocaleString(locale);
  },

  /**
   * Display human readable time difference.
   * @param diff difference in milliseconds
   * @return time difference as string
   */
  displayTimeDiff: function(diff) {
    // unknown difference
    if (diff < 0) {
      return "-";
    }
    var secondsFloat = diff / 1000;
    var seconds = Math.floor(secondsFloat);
    var hours = Math.floor(seconds / 3600);
    var minutes = Math.floor(seconds / 60);
    if (hours > 0) {
      return `${hours} h ${minutes % 60} min`;
    } else if (minutes > 0) {
      return `${minutes} min ${seconds % 60} s`;
    } else if (secondsFloat > 0.1) {
      // report seconds as floating point numbers
      return `${this.rnd(secondsFloat, 1)} s`;
    } else {
      // report milliseconds directly
      return `${diff} ms`;
    }
  },

  /**
   * Display bytes in human readable format.
   * @param bytes number of bytes
   * @return string representing bytes
   */
  displayBytes: function(bytes) {
    if (bytes <= 1024) {
      return `${bytes} B`;
    } else if (bytes <= 1024 * 1024) {
      return `${this.rnd(bytes / 1024, 2)} KB`;
    } else if (bytes <= 1024 * 1024 * 1024) {
      return `${this.rnd(bytes / 1024 / 1024, 2)} MB`;
    } else {
      return `${this.rnd(bytes / 1024 / 1024 / 1024, 2)} GB`;
    }
  },

  /**
   * Display fraction as percentage value, e.g. 0.234 => 23.4%
   * Value must be between 0.0 and 1.0, otherwise returns "NaN%"
   * @param fraction double value between 0.0 and 1.0
   * @return percentage value as string
   */
  displayPercent: function(fraction) {
    if (fraction >= 0.0 && fraction <= 1.0) {
      return `${this.rnd(fraction, 2) * 100}%`;
    } else {
      return "NaN%";
    }
  }
}
