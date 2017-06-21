package com.github.lightcopy.history;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import com.github.lightcopy.history.conf.AppConf;

/**
 * Application context.
 * Only manual lookup is enabled, provider discovery is disabled.
 */
public class ApplicationContext extends ResourceConfig {
  // current working directory
  private static final String WORKING_DIRECTORY = "working.directory";

  private final transient AppConf conf;

  public ApplicationContext(AppConf conf) {
    this.conf = conf;
    register(ContextProvider.class);
    property(ServerProperties.METAINF_SERVICES_LOOKUP_DISABLE, true);
    property(WORKING_DIRECTORY, conf.workingDirectory());
  }

  @Path("/")
  public static class ContextProvider {
    // html
    private static final String INDEX_HTML_PATH = "index.html";

    @Context
    Configuration config;

    /** Get current working directory from context */
    private File workingDirectory() {
      return new File((String) config.getProperty(WORKING_DIRECTORY));
    }

    /** Build resource path to components */
    private File dir(String... children) {
      File path = workingDirectory();
      for (String component : children) {
        path = new File(path, component);
      }
      return path;
    }

    /** Open file path and return input stream */
    private InputStream open(File path) {
      try {
        return new FileInputStream(path);
      } catch (FileNotFoundException err) {
        throw new RuntimeException(err);
      }
    }

    @GET
    @Produces("text/html")
    public Response getIndex() {
      InputStream index = open(dir("static", INDEX_HTML_PATH));
      return Response.ok(index).build();
    }
  }
}
