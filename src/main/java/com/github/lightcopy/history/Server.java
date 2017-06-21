package com.github.lightcopy.history;

/** Simple development server for frontend */
public class Server extends AbstractServer {

  public Server() {
    super();
  }

  public static void main(String[] args) {
    try {
      LOG.info("Initialize web server");
      Server server = new Server();
      LOG.info("Created server {}", server);
      server.launch();
    } catch (Exception err) {
      LOG.error("Exception occurred", err);
      System.exit(1);
    }
  }
}
