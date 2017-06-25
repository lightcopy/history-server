module.exports = {
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
    var date = new Date(timestamp), locale = "en-nz";
    return date.toLocaleString(locale);
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
  }
}