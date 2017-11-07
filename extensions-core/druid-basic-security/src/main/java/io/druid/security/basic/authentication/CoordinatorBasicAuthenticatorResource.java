/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.security.basic.authentication;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.security.basic.BasicSecurityResourceFilter;
import io.druid.security.basic.db.BasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.db.entity.BasicAuthenticatorUser;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Configuration resource for authenticator users and credentials.
 */
@Path("/druid/coordinator/v1/security/authentication")
public class CoordinatorBasicAuthenticatorResource
{
  private static final Logger log = new Logger(CoordinatorBasicAuthenticatorResource.class);

  private final BasicAuthenticatorMetadataStorageUpdater storageUpdater;
  private final Map<String, BasicHTTPAuthenticator> authenticatorMap;

  @Inject
  public CoordinatorBasicAuthenticatorResource(
      BasicAuthenticatorMetadataStorageUpdater storageUpdater,
      AuthenticatorMapper authenticatorMapper
  )
  {
    this.storageUpdater = storageUpdater;

    this.authenticatorMap = Maps.newHashMap();
    for (Map.Entry<String, Authenticator> authenticatorEntry : authenticatorMapper.getAuthenticatorMap().entrySet()) {
      final String authenticatorName = authenticatorEntry.getKey();
      final Authenticator authenticator = authenticatorEntry.getValue();
      if (authenticator instanceof BasicHTTPAuthenticator) {
        authenticatorMap.put(
            authenticatorName,
            (BasicHTTPAuthenticator) authenticator
        );
      }
    }

    log.info("Created COORDINATOR basic auth resource");
  }

  /**
   * @param req HTTP request
   *
   * @return List of all users
   */
  @GET
  @Path("/{authenticatorName}/users")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getAllUsers(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName
  )
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    Map<String, BasicAuthenticatorUser> userMap = storageUpdater.deserializeUserMap(
        storageUpdater.getCurrentUserMapBytes(authenticatorName)
    );

    return Response.ok(userMap.keySet()).build();
  }

  /**
   * @param req      HTTP request
   * @param userName Name of user to retrieve information about
   *
   * @return Name and credentials of the user with userName, 400 error response if user doesn't exist
   */
  @GET
  @Path("/{authenticatorName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getUser(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName,
      @PathParam("userName") final String userName
  )
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    Map<String, BasicAuthenticatorUser> userMap = storageUpdater.deserializeUserMap(
        storageUpdater.getCurrentUserMapBytes(authenticatorName)
    );

    try {
      BasicAuthenticatorUser user = userMap.get(userName);
      if (user == null) {
        throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
      }
      return Response.ok(user).build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  /**
   * Create a new user with name userName
   *
   * @param req      HTTP request
   * @param userName Name to assign the new user
   *
   * @return OK response, or 400 error response if user already exists
   */
  @POST
  @Path("/{authenticatorName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response createUser(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName,
      @PathParam("userName") String userName
  )
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    try {
      storageUpdater.createUser(authenticatorName, userName);
      return Response.ok().build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  /**
   * Delete a user
   *
   * @param req      HTTP request
   * @param userName Name of user to delete
   *
   * @return OK response, or 400 error response if user doesn't exist
   */
  @DELETE
  @Path("/{authenticatorName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response deleteUser(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName,
      @PathParam("userName") String userName
  )
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    try {
      storageUpdater.deleteUser(authenticatorName, userName);
      return Response.ok().build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  /**
   * Assign credentials for a user
   *
   * @param req      HTTP request
   * @param userName Name of user
   * @param password Password to assign
   *
   * @return OK response, 400 error if user doesn't exist
   */
  @POST
  @Path("/{authenticatorName}/users/{userName}/credentials")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response updateUserCredentials(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName,
      @PathParam("userName") String userName,
      String password
  )
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    try {
      storageUpdater.setUserCredentials(authenticatorName, userName, password.toCharArray());
      return Response.ok().build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  /**
   * @param req HTTP request
   *
   * @return serialized user map
   */
  @GET
  @Path("/{authenticatorName}/cachedSerializedUserMap")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getCachedSerializedUserMap(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName
  )
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    return Response.ok(storageUpdater.getCachedSerializedUserMap(authenticatorName)).build();
  }

  private static Response makeResponseForAuthenticatorNotFound(String authenticatorName)
  {
    return Response.status(Response.Status.BAD_REQUEST)
                   .entity(ImmutableMap.<String, Object>of(
                       "error",
                       StringUtils.format("Basic authenticator with name [%s] does not exist.", authenticatorName)
                   ))
                   .build();
  }

  private static Response makeResponseForBasicSecurityDBResourceException(BasicSecurityDBResourceException bsre)
  {
    return Response.status(Response.Status.BAD_REQUEST)
                   .entity(ImmutableMap.<String, Object>of(
                       "error", bsre.getMessage()
                   ))
                   .build();
  }
}
