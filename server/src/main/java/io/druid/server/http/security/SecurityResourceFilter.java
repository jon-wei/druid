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

package io.druid.server.http.security;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ContainerRequest;
import io.druid.server.security.Access;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationManager;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.util.List;

public class SecurityResourceFilter extends AbstractResourceFilter
{
  @Inject
  public SecurityResourceFilter(
      AuthConfig authConfig,
      AuthorizationManager authorizationManager
  )
  {
    super(authConfig, authorizationManager);
  }

  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    if (getAuthConfig().isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
      final ResourceAction resourceAction = new ResourceAction(
          new Resource("security", ResourceType.CONFIG),
          getAction(request)
      );

      final Access authResult = AuthorizationUtils.authorizeResourceAction(
          getReq(),
          resourceAction,
          getAuthorizationManager()
      );

      if (!authResult.isAllowed()) {
        throw new WebApplicationException(
            Response.status(Response.Status.FORBIDDEN)
                    .entity(String.format("Access-Check-Result: %s", authResult.toString()))
                    .build()
        );
      }
    }
    return request;
  }

  @Override
  public boolean isApplicable(String requestPath)
  {
    List<String> applicablePaths = ImmutableList.of("druid/coordinator/v1/security/");
    for (String path : applicablePaths) {
      if(requestPath.startsWith(path) && !requestPath.equals(path)) {
        return true;
      }
    }
    return false;
  }
}
