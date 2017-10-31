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

package io.druid.security.basic;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.java.util.common.StringUtils;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.db.BasicAuthorizerStorageConnector;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.ResourceAction;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;

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
import java.util.List;
import java.util.Map;

/**
 * Configuration resource for authorizer users/roles/permissions
 */
@Path("/druid/coordinator/v1/security/authorization")
public class BasicAuthorizerResource
{
  private final Map<String, BasicAuthorizerStorageConnector> dbConnectors;

  @Inject
  public BasicAuthorizerResource(
      AuthorizerMapper authorizerMapper
  )
  {
    this.dbConnectors = Maps.newHashMap();
    for (Map.Entry<String, Authorizer> authorizerEntry : authorizerMapper.getAuthorizerMap().entrySet()) {
      final String authorizerName = authorizerEntry.getKey();
      final Authorizer authorizer = authorizerEntry.getValue();
      if (authorizer instanceof BasicRoleBasedAuthorizer) {
        dbConnectors.put(authorizerName, ((BasicRoleBasedAuthorizer) authorizer).getDbConnector());
      }
    }
  }

  /**
   * @param req HTTP request
   *
   * @return List of all users
   */
  @GET
  @Path("/{authorizerName}/users")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getAllUsers(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    List<Map<String, Object>> users = dbConnector.getAllUsers();
    return Response.ok(users).build();
  }

  /**
   * @param req      HTTP request
   * @param userName Name of user to retrieve information about
   *
   * @return Name, roles, and permissions of the user with userName, 400 error response if user doesn't exist
   */
  @GET
  @Path("/{authorizerName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getUser(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("userName") final String userName
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      Map<String, Object> user = dbConnector.getUser(userName);
      List<Map<String, Object>> roles = dbConnector.getRolesForUser(userName);
      List<Map<String, Object>> permissions = dbConnector.getPermissionsForUser(userName);

      Map<String, Object> userInfo = ImmutableMap.of(
          "user", user,
          "roles", roles,
          "permissions", permissions
      );

      return Response.ok(userInfo).build();
    }
    catch (CallbackFailedException cfe) {
      return makeResponseForCallbackFailedException(cfe);
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
  @Path("/{authorizerName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response createUser(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("userName") String userName
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      dbConnector.createUser(userName);
      return Response.ok().build();
    }
    catch (CallbackFailedException cfe) {
      return makeResponseForCallbackFailedException(cfe);
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
  @Path("/{authorizerName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response deleteUser(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("userName") String userName
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      dbConnector.deleteUser(userName);
      return Response.ok().build();
    }
    catch (CallbackFailedException cfe) {
      return makeResponseForCallbackFailedException(cfe);
    }
  }

  /**
   * @param req HTTP request
   *
   * @return List of all roles
   */
  @GET
  @Path("/{authorizerName}/roles")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getAllRoles(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    List<Map<String, Object>> roles = dbConnector.getAllRoles();
    return Response.ok(roles).build();
  }

  /**
   * Get info about a role
   *
   * @param req      HTTP request
   * @param roleName Name of role
   *
   * @return Role name, users with role, and permissions of role. 400 error if role doesn't exist.
   */
  @GET
  @Path("/{authorizerName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getRole(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("roleName") final String roleName
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      Map<String, Object> role = dbConnector.getRole(roleName);
      List<Map<String, Object>> users = dbConnector.getUsersWithRole(roleName);
      List<Map<String, Object>> permissions = dbConnector.getPermissionsForRole(roleName);

      Map<String, Object> roleInfo = ImmutableMap.of(
          "role", role,
          "users", users,
          "permissions", permissions
      );

      return Response.ok(roleInfo).build();
    }
    catch (CallbackFailedException cfe) {
      return makeResponseForCallbackFailedException(cfe);
    }
  }

  /**
   * Create a new role.
   *
   * @param req      HTTP request
   * @param roleName Name of role
   *
   * @return OK response, 400 error if role already exists
   */
  @POST
  @Path("/{authorizerName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response createRole(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("roleName") final String roleName
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      dbConnector.createRole(roleName);
      return Response.ok().build();
    }
    catch (CallbackFailedException cfe) {
      return makeResponseForCallbackFailedException(cfe);
    }
  }

  /**
   * Delete a role.
   *
   * @param req      HTTP request
   * @param roleName Name of role
   *
   * @return OK response, 400 error if role doesn't exist.
   */
  @DELETE
  @Path("/{authorizerName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response deleteRole(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("roleName") String roleName
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      dbConnector.deleteRole(roleName);
      return Response.ok().build();
    }
    catch (CallbackFailedException cfe) {
      return makeResponseForCallbackFailedException(cfe);
    }
  }

  /**
   * Assign a role to a user.
   *
   * @param req      HTTP request
   * @param userName Name of user
   * @param roleName Name of role
   *
   * @return OK response. 400 error if user/role don't exist, or if user already has the role
   */
  @POST
  @Path("/{authorizerName}/users/{userName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response assignRoleToUser(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("userName") String userName,
      @PathParam("roleName") String roleName
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      dbConnector.assignRole(userName, roleName);
      return Response.ok().build();
    }
    catch (CallbackFailedException cfe) {
      return makeResponseForCallbackFailedException(cfe);
    }
  }

  /**
   * Remove a role from a user.
   *
   * @param req      HTTP request
   * @param userName Name of user
   * @param roleName Name of role
   *
   * @return OK response. 400 error if user/role don't exist, or if user does not have the role.
   */
  @DELETE
  @Path("/{authorizerName}/users/{userName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response unassignRoleFromUser(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("userName") String userName,
      @PathParam("roleName") String roleName
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      dbConnector.unassignRole(userName, roleName);
      return Response.ok().build();
    }
    catch (CallbackFailedException cfe) {
      return makeResponseForCallbackFailedException(cfe);
    }
  }

  /**
   * Add permissions to a role.
   *
   * @param req             HTTP request
   * @param roleName        Name of role
   * @param resourceActions Permissions to add
   *
   * @return OK response. 400 error if role doesn't exist.
   */
  @POST
  @Path("/{authorizerName}/roles/{roleName}/permissions")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response addPermissionsToRole(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("roleName") String roleName,
      List<ResourceAction> resourceActions
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      for (ResourceAction resourceAction : resourceActions) {
        dbConnector.addPermission(roleName, resourceAction);
      }

      return Response.ok().build();
    }
    catch (CallbackFailedException cfe) {
      return makeResponseForCallbackFailedException(cfe);
    }
  }

  /**
   * Delete a permission.
   *
   * @param req    HTTP request
   * @param permId ID of permission to delete
   *
   * @return OK response. 400 error if permission doesn't exist.
   */
  @DELETE
  @Path("/{authorizerName}/permissions/{permId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response deletePermission(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("permId") Integer permId
  )
  {
    final BasicAuthorizerStorageConnector dbConnector = dbConnectors.get(authorizerName);
    if (dbConnector == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      dbConnector.deletePermission(permId);
      return Response.ok().build();
    }
    catch (CallbackFailedException cfe) {
      return makeResponseForCallbackFailedException(cfe);
    }
  }

  private static Response makeResponseForAuthorizerNotFound(String authorizerName)
  {
    return Response.status(Response.Status.BAD_REQUEST)
                   .entity(ImmutableMap.<String, Object>of(
                       "error",
                       StringUtils.format("Basic authorizer with name [%s] does not exist.", authorizerName)
                   ))
                   .build();
  }

  private static Response makeResponseForCallbackFailedException(CallbackFailedException cfe)
  {
    Throwable cause = cfe.getCause();
    if (cause instanceof BasicSecurityDBResourceException) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of(
                         "error", cause.getMessage()
                     ))
                     .build();
    } else {
      throw cfe;
    }
  }
}
