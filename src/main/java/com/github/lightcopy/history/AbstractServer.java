/*
 * Copyright 2017 Lightcopy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.lightcopy.history;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Properties;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lightcopy.history.conf.AppConf;

/**
 * Abstract class that is shared between production implementation and test server for frontend.
 */
public abstract class AbstractServer {
  protected static final Logger LOG = LoggerFactory.getLogger(Server.class);

  protected final String scheme;
  protected final String host;
  protected final int port;
  protected AppConf conf;
  private HttpServer server;
  private ArrayList<Runnable> events;

  /**
   * Shutdown hook to gracefully stop web server.
   */
  static class ServerShutdown implements Runnable {
    private final AbstractServer server;

    ServerShutdown(AbstractServer server) {
      this.server = server;
    }

    @Override
    public void run() {
      this.server.shutdown();
    }

    @Override
    public String toString() {
      return "ServerShutdown" + this.server;
    }
  }

  /**
   * Initialize server from properties.
   * Use zero-argument constructor in subclasses, server will be initialized with system
   * properties by default.
   */
  private AbstractServer(Properties props) {
    this.conf = new AppConf(props);
    this.scheme = this.conf.scheme();
    this.host = this.conf.httpHost();
    this.port = this.conf.httpPort();
    // initialize events list and internal server
    this.events = new ArrayList<Runnable>();
  }

  public AbstractServer() {
    this(System.getProperties());
  }

  /** Get current host */
  public String getHost() {
    return this.host;
  }

  /** Get current port */
  public int getPort() {
    return this.port;
  }

  /**
   * Start web server using provided options. As part of initialization registers all shutdown
   * hooks, including one for the server.
   */
  public void launch() throws IOException, InterruptedException {
    this.server = createHttpServer(this.conf);
    // register shutdown hook for server after all events
    registerShutdownHook(new ServerShutdown(this));
    for (Runnable event : this.events) {
      LOG.info("Register shutdown event {}", event);
      Runtime.getRuntime().addShutdownHook(new Thread(event));
    }
    LOG.info("Start server {}", this);
    this.server.start();
    afterLaunch();
  }

  /** Create http server from initialized properties */
  protected HttpServer createHttpServer(AppConf conf) {
    URI endpoint = createEndpoint();
    ApiProvider provider = apiProvider();
    LOG.info("Register API provider {}", provider);
    ApplicationContext context = new ApplicationContext(conf, provider);
    return GrizzlyHttpServerFactory.createHttpServer(endpoint, context);
  }

  /** Create endpoint uri from initialized properties */
  protected URI createEndpoint() {
    return UriBuilder.fromPath("")
      .scheme(this.scheme)
      .host(this.host)
      .port(this.port)
      .build();
  }

  /**
   * Launch functionality provided by subclasses, if required.
   * This gets invoked after server is up and running.
   */
  public void afterLaunch() {
    /* no-op */
  }

  /**
   * Return API provider for this server to serve REST.
   * By default, is null, which will throw NPE when not set.
   * @return ApiProvider instance
   */
  public ApiProvider apiProvider() {
    /* no-op, should be overwritten in subclasses */
    return null;
  }

  /**
   * Register shutdown hook to call when server is about to be stopped. These events are always
   * called before server shutdown.
   */
  public void registerShutdownHook(Runnable event) {
    this.events.add(event);
  }

  /**
   * Shutdown server. This should not be used directly, instead call launch() method. It will
   * add shutdown hook to gracefully stop server.
   */
  protected void shutdown() {
    LOG.info("Stop server {}", this);
    this.server.shutdown();
  }

  @Override
  public String toString() {
    return "[" + getClass().getSimpleName() + " @ " + createEndpoint() + "]";
  }
}
