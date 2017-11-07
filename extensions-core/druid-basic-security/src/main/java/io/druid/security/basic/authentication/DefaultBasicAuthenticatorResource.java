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
import io.druid.security.basic.BasicSecurityResourceFilter;
import io.druid.security.basic.db.cache.BasicAuthenticatorCacheManager;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Path("/druid/security/internal/authentication")
public class DefaultBasicAuthenticatorResource implements BasicAuthenticatorResource
{
  private static final Logger log = new Logger(DefaultBasicAuthenticatorResource.class);

  private final BasicAuthenticatorCacheManager cacheManager;
  private final Map<String, BasicHTTPAuthenticator> authenticatorMap;

  @Inject
  public DefaultBasicAuthenticatorResource(
      BasicAuthenticatorCacheManager cacheManager,
      AuthenticatorMapper authenticatorMapper
  )
  {
    this.cacheManager = cacheManager;

    this.authenticatorMap = Maps.newHashMap();
    for (Map.Entry<String, Authenticator> authenticatorEntry : authenticatorMapper.getAuthenticatorMap().entrySet()) {
      final String authenticatorName = authenticatorEntry.getKey();
      final Authenticator authenticator = authenticatorEntry.getValue();
      if (authenticator instanceof BasicHTTPAuthenticator) {
        authenticatorMap.put(
            ((BasicHTTPAuthenticator) authenticator).getDBPrefix(),
            (BasicHTTPAuthenticator) authenticator
        );
      }
    }
  }

  /**
   * Listen for update notifications for the auth storage
   *
   * @param req      HTTP request
   * @param userName Name to assign the new user
   *
   * @return OK response, or 400 error response if user already exists
   */
  @POST
  @Path("/listen/{authenticatorName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response createUser(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName
  )
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      String errMsg = StringUtils.format("Received update for unknown authenticator[%s]", authenticatorName);
      log.error(errMsg);
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of(
                         "error",
                         StringUtils.format(errMsg)
                     ))
                     .build();
    }

    cacheManager.addAuthenticatorToUpdate(authenticatorName);
    return Response.ok().build();
  }
}
