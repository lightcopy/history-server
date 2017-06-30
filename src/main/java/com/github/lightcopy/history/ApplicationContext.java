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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import com.github.lightcopy.history.conf.AppConf;
import com.github.lightcopy.history.model.Application;
import com.github.lightcopy.history.model.Environment;
import com.github.lightcopy.history.model.SQLExecution;

/**
 * Application context.
 * Only manual lookup is enabled, provider discovery is disabled.
 */
public class ApplicationContext extends ResourceConfig {
  // current working directory
  private static final String WORKING_DIRECTORY = "working.directory";
  private static final String API_PROVIDER = "api.provider";
  private static final Gson gson = new Gson();

  private final AppConf conf;

  public ApplicationContext(AppConf conf, ApiProvider provider) {
    this.conf = conf;
    register(ContextProvider.class);
    property(ServerProperties.METAINF_SERVICES_LOOKUP_DISABLE, true);
    property(WORKING_DIRECTORY, conf.workingDirectory());
    property(API_PROVIDER, provider);
  }

  /** API methdo to return error message with provided code */
  private static Response apiError(Response.Status status, String msg) {
    JsonObject obj = new JsonObject();
    obj.addProperty("code", status.getStatusCode());
    obj.addProperty("msg", msg);
    return Response.accepted(gson.toJson(obj)).status(status).build();
  }

  /** API method to return 400 error as JSON */
  private static Response apiError400(String msg) {
    return apiError(Response.Status.BAD_REQUEST, msg);
  }

  /** API method to return 404 error as JSON */
  private static Response apiError404(String msg) {
    return apiError(Response.Status.NOT_FOUND, msg);
  }

  @Path("/")
  public static class ContextProvider {
    @Context
    Configuration config;
    ApiProvider provider;

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

    /**
     * Get API provider for context.
     * @return API provider
     */
    private ApiProvider getProvider() {
      if (provider == null) {
        provider = (ApiProvider) config.getProperty(API_PROVIDER);
      }
      return provider;
    }

    @GET
    @Produces("text/html")
    public Response getIndex() {
      InputStream index = open(dir("dist", "index.html"));
      return Response.ok(index).build();
    }

    @GET
    @Path("apps")
    @Produces("text/html")
    public Response getAppsIndex() {
      return getIndex();
    }

    @GET
    @Path("apps/{default: .*}")
    @Produces("text/html")
    public Response getAppsRoute() {
      return getIndex();
    }

    @GET
    @Path("css/{path}.css")
    @Produces("text/css")
    public Response getCSS(@PathParam("path") String path) {
      InputStream css = open(dir("dist", "css", path + ".css"));
      return Response.ok(css).build();
    }

    @GET
    @Path("js/{path}.js")
    @Produces("text/javascript")
    public Response getJS(@PathParam("path") String path) {
      InputStream js = open(dir("dist", "js", path + ".js"));
      return Response.ok(js).build();
    }

    @GET
    @Path("img/{path}.png")
    @Produces("image/png")
    public Response getPNG(@PathParam("path") String path) {
      InputStream img = open(dir("dist", "img", path + ".png"));
      return Response.ok(img).build();
    }

    @GET
    @Path("fonts/{path}.eot")
    @Produces("application/vnd.ms-fontobject")
    public Response getEOT(@PathParam("path") String path) {
      InputStream in = open(dir("dist", "fonts", path + ".eot"));
      return Response.ok(in).build();
    }

    @GET
    @Path("fonts/{path}.svg")
    @Produces("image/svg+xml")
    public Response getSVG(@PathParam("path") String path) {
      InputStream in = open(dir("dist", "fonts", path + ".svg"));
      return Response.ok(in).build();
    }

    @GET
    @Path("fonts/{path}.ttf")
    @Produces("application/font-sfnt")
    public Response getTTF(@PathParam("path") String path) {
      InputStream in = open(dir("dist", "fonts", path + ".ttf"));
      return Response.ok(in).build();
    }

    @GET
    @Path("fonts/{path}.woff")
    @Produces("application/font-woff")
    public Response getWOFF(@PathParam("path") String path) {
      InputStream in = open(dir("dist", "fonts", path + ".woff"));
      return Response.ok(in).build();
    }

    @GET
    @Path("fonts/{path}.woff2")
    @Produces("font/woff2")
    public Response getWOFF2(@PathParam("path") String path) {
      InputStream in = open(dir("dist", "fonts", path + ".woff2"));
      return Response.ok(in).build();
    }

    // == REST API ==

    @GET
    @Path("api/apps")
    @Produces("application/json")
    public Response listApplications(
        @DefaultValue("1") @QueryParam("page") int page,
        @DefaultValue("100") @QueryParam("pageSize") int pageSize,
        @DefaultValue("") @QueryParam("sortBy") String sortBy,
        @DefaultValue("true") @QueryParam("asc") boolean asc) {
      try {
        return Response.ok(
          gson.toJson(getProvider().applications(page, pageSize, sortBy, asc))).build();
      } catch (Exception err) {
        return apiError400(err.getMessage());
      }
    }

    @GET
    @Path("api/apps/{appId}/environment")
    @Produces("application/json")
    public Response getAppEnvironment(@PathParam("appId") String appId) {
      try {
        Application app = getProvider().application(appId);
        if (app == null) {
          return apiError404("Application " + appId + " is not found");
        } else {
          Environment env = getProvider().environment(appId);
          // build json that has 2 keys, one for application, another for environment.
          // if environment is not found, we will return empty object as value.
          JsonObject obj = new JsonObject();
          obj.add("app", gson.toJsonTree(app));
          obj.add("env", gson.toJsonTree(env));
          return Response.ok(gson.toJson(obj)).build();
        }
      } catch (Exception err) {
        return apiError400(err.getMessage());
      }
    }

    @GET
    @Path("api/apps/{appId}/sql")
    @Produces("application/json")
    public Response listSQLExecutions(
        @PathParam("appId") String appId,
        @DefaultValue("1") @QueryParam("page") int page,
        @DefaultValue("100") @QueryParam("pageSize") int pageSize,
        @DefaultValue("") @QueryParam("sortBy") String sortBy,
        @DefaultValue("true") @QueryParam("asc") boolean asc) {
      try {
        return Response.ok(
          gson.toJson(getProvider().sqlExecutions(appId, page, pageSize, sortBy, asc))).build();
      } catch (Exception err) {
        return apiError400(err.getMessage());
      }
    }

    @GET
    @Path("api/apps/{appId}/sql/query/{id}")
    @Produces("application/json")
    public Response sqlExecution(@PathParam("appId") String appId, @PathParam("id") int id) {
      try {
        Application app = getProvider().application(appId);
        if (app == null) {
          return apiError404("Application " + appId + " is not found");
        } else {
          SQLExecution sql = getProvider().sqlExecution(appId, id);
          JsonObject obj = new JsonObject();
          obj.add("app", gson.toJsonTree(app));
          obj.add("sql", gson.toJsonTree(sql));
          return Response.ok(gson.toJson(obj)).build();
        }
      } catch (Exception err) {
        return apiError400(err.getMessage());
      }
    }
  }
}
