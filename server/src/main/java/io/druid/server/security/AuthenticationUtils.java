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

package io.druid.server.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.ISE;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class AuthenticationUtils
{
  public static Authenticator[] getAuthenticatorChainFromConfig(
      String filterChainPath,
      Injector injector
  )
  {
    try {
      ObjectMapper mapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
      String filterChainJson = new String(Files.readAllBytes(Paths.get(filterChainPath)));
      Authenticator[] authenticators = mapper.readValue(filterChainJson, Authenticator[].class);
      return authenticators;
    } catch (IOException ioe) {
      throw new ISE("Could not create authenticator chain due to IOException: [%s]", ioe.getMessage());
    }
  }

  public static void addAuthenticationFilterChain(
      ServletContextHandler root,
      Authenticator[] authenticators
  )
  {
    for (Authenticator authenticator : authenticators) {
      FilterHolder holder = new FilterHolder(authenticator.getFilter());
      if (authenticator.getInitParameters() != null) {
        holder.setInitParameters(authenticator.getInitParameters());
      }
      root.addFilter(
          holder,
          "/*",
          null
      );
    }
  }

  public static void addNoopAuthorizationFilters(ServletContextHandler root, List<String> unsecuredPaths)
  {
    for (String unsecuredPath : unsecuredPaths) {
      root.addFilter(new FilterHolder(new UnsecuredResourceFilter()), unsecuredPath, null);
    }
  }

  public static void addSecuritySanityCheckFilter(
      ServletContextHandler root,
      ObjectMapper jsonMapper
  )
  {
    root.addFilter(
        new FilterHolder(
            new SecuritySanityCheckFilter(jsonMapper)
        ),
        "/*",
        null
    );
  }

  public static void addPreResponseAuthorizationCheckFilter(
      ServletContextHandler root,
      Authenticator[] authenticators,
      ObjectMapper jsonMapper,
      AuthConfig authConfig
  )
  {
    root.addFilter(
        new FilterHolder(
            new PreResponseAuthorizationCheckFilter(authConfig, authenticators, jsonMapper)
        ),
        "/*",
        null
    );
  }
}
