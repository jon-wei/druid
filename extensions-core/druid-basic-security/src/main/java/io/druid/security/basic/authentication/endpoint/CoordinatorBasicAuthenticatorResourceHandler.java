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

package io.druid.security.basic.authentication.endpoint;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.authentication.db.entity.BasicAuthenticatorUser;
import io.druid.security.basic.authentication.db.updater.BasicAuthenticatorMetadataStorageUpdater;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Map;

public class CoordinatorBasicAuthenticatorResourceHandler implements BasicAuthenticatorResourceHandler
{
  private static final Logger log = new Logger(CoordinatorBasicAuthenticatorResourceHandler.class);

  private final BasicAuthenticatorMetadataStorageUpdater storageUpdater;
  private final Map<String, BasicHTTPAuthenticator> authenticatorMap;

  @Inject
  public CoordinatorBasicAuthenticatorResourceHandler(
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

  @Override
  public Response getAllUsers(
      final String authenticatorName
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

  @Override
  public Response getUser(String authenticatorName, String userName)
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

  @Override
  public Response createUser(String authenticatorName, String userName)
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

  @Override
  public Response deleteUser(String authenticatorName, String userName)
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

  @Override
  public Response updateUserCredentials(String authenticatorName, String userName, String password)
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

  @Override
  public Response getCachedSerializedUserMap(String authenticatorName)
  {
    final BasicHTTPAuthenticator authenticator = authenticatorMap.get(authenticatorName);
    if (authenticator == null) {
      return makeResponseForAuthenticatorNotFound(authenticatorName);
    }

    return Response.ok(storageUpdater.getCachedSerializedUserMap(authenticatorName)).build();
  }

  @Override
  public Response authenticatorUpdateListener(HttpServletRequest req, String authenticatorName)
  {
    throw new UnsupportedOperationException("Listener update is not applicable to coordinator nodes.");
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
