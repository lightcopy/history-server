package com.github.lightcopy.history.conf;

import java.io.IOException;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe configuration for application, uses concurrent hashmap internally, therefore only
 * non-null keys and values are allowed. All keys and values are trimmed to remove whitespaces. When
 * fetching with default value, exception is not propagated, but default value is returned instead.
 */
public class AppConf {
  private static final Logger LOG = LoggerFactory.getLogger(AppConf.class);

  // Available options and default parameters
  // Web server settings (host, port, scheme)
  public static final String HTTP_HOST_KEY = "http.host";
  public static final String HTTP_HOST_DEFAULT = "0.0.0.0";

  public static final String HTTP_PORT_KEY = "http.port";
  public static final int HTTP_PORT_DEFAULT = 8080;

  public static final String HTTP_SCHEME = "http";

  // MongoDB settings (connection string)
  public static final String MONGO_CONN_KEY = "mongo.address";
  public static final String MONGO_CONN_DEFAULT = "mongodb://localhost:27017";

  // Keep all keys above registered in the set, used to extract relevant entries from properties
  public static final HashSet<String> REGISTERED_KEYS = new HashSet<String>();
  static {
    REGISTERED_KEYS.add(HTTP_HOST_KEY);
    REGISTERED_KEYS.add(HTTP_PORT_KEY);
    REGISTERED_KEYS.add(MONGO_CONN_KEY);
  }

  private ConcurrentHashMap<String, String> options;
  private String workingDirectory;

  public AppConf(Properties props) {
    this.options = new ConcurrentHashMap<String, String>();
    for (String key : props.stringPropertyNames()) {
      if (REGISTERED_KEYS.contains(key)) {
        String value = props.getProperty(key);
        LOG.debug("Insert entry " + key + "->" + value);
        options.put(key, value);
      }
    }
    // current working directory
    try {
      this.workingDirectory = new File(".").getCanonicalPath();
    } catch (IOException err) {
      throw new RuntimeException(err);
    }
  }

  public AppConf() {
    this(new Properties());
  }

  /** Clean up key, check for null and trim whitespaces */
  private String cleanupKey(String key) {
    String orig = (key == null) ? key : key.trim();
    if (orig == null || orig.length() == 0) {
      throw new IllegalArgumentException("Invalid key '" + key + "', non empty key is expected");
    }
    return orig;
  }

  /** Clean up value, check for null and trim whitespaces */
  private String cleanupValue(String value) {
    if (value == null) {
      throw new IllegalArgumentException(
        "Invalid value '" + value + "', non-null value is expected");
    }
    return value.trim();
  }

  /** Whether or not key exists in configuration */
  private boolean containsKey(String key) {
    return this.options.containsKey(key);
  }

  /** Assert that key exists in configuration */
  private void assertKey(String key) {
    if (!containsKey(key)) {
      throw new IllegalArgumentException("Key '" + key + "' is not found in options");
    }
  }

  /** Set value for key */
  public void set(String key, String value) {
    this.options.put(cleanupKey(key), cleanupValue(value));
  }

  /** Set integer value for key */
  public void set(String key, int value) {
    set(key, Integer.toString(value));
  }

  /** Get value for key */
  public String get(String key) {
    String orig = cleanupKey(key);
    assertKey(orig);
    return this.options.get(orig);
  }

  /** Get value for key, use default if key does not exist */
  public String get(String key, String defaultValue) {
    String orig = cleanupKey(key);
    if (containsKey(orig)) {
      return this.options.get(orig);
    } else {
      return defaultValue;
    }
  }

  /** Get base-10 integer for key */
  public int getInt(String key) {
    String orig = cleanupKey(key);
    assertKey(orig);
    try {
      return Integer.parseInt(this.options.get(orig));
    } catch (NumberFormatException err) {
      throw new IllegalArgumentException("Could not parse value for key '" + key + "'", err);
    }
  }

  /** Get base-10 integer for key, use default if key does not exist, of parsing fails */
  public int getInt(String key, int defaultValue) {
    String orig = cleanupKey(key);
    if (!containsKey(orig)) return defaultValue;
    try {
      return Integer.parseInt(this.options.get(orig));
    } catch (NumberFormatException err) {
      LOG.warn("Failed to parse value for key " + key + ", " + err);
      return defaultValue;
    }
  }

  /** Configuration methods **/

  public String httpHost() {
    return get(HTTP_HOST_KEY, HTTP_HOST_DEFAULT);
  }

  public int httpPort() {
    return getInt(HTTP_PORT_KEY, HTTP_PORT_DEFAULT);
  }

  public String scheme() {
    return HTTP_SCHEME;
  }

  public String mongoConnectionString() {
    return get(MONGO_CONN_KEY, MONGO_CONN_DEFAULT);
  }

  public String workingDirectory() {
    return workingDirectory;
  }
}
