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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.java.util.common.ISE;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Static utility functions for performing authorization checks.
 */
public class AuthorizationUtils
{
  public final static Access ACCESS_OK = new Access(true, "All resource-actions authorized.");

  /**
   * Check a resource-action using the AuthorizationInfo from the request.
   *
   * Otherwise, if the resource-actions is authorized, return ACCESS_OK.
   *
   * This function will set the DRUID_AUTH_TOKEN_CHECKED attribute in the request.
   *
   * If this attribute is already set when this function is called, an exception is thrown.
   *
   * @param request HTTP request to be authorized
   * @param resourceAction A resource identifier and the action to be taken the resource.
   * @return ACCESS_OK or the failed Access object returned by the request's AuthorizationInfo.
   */
  public static Access authorizeResourceAction(
      final HttpServletRequest request,
      final ResourceAction resourceAction,
      final AuthorizationManager authorizationManager
  )
  {
    return authorizeAllResourceActions(
        request,
        Lists.newArrayList(resourceAction),
        authorizationManager
    );
  }


  /**
   * Check a list of resource-actions using the AuthorizationInfo from the request.
   *
   * If one of the resource-actions fails the authorization check, this method returns the failed
   * Access object from the check.
   *
   * Otherwise, return ACCESS_OK if all resource-actions were successfully authorized.
   *
   * This function will set the DRUID_AUTH_TOKEN_CHECKED attribute in the request.
   *
   * If this attribute is already set when this function is called, an exception is thrown.
   * @param request HTTP request to be authorized
   * @param resourceActions A list of resource-actions to authorize
   * @return ACCESS_OK or the Access object from the first failed check
   */
  public static Access authorizeAllResourceActions(
      final HttpServletRequest request,
      final List<ResourceAction> resourceActions,
      final AuthorizationManager authorizationManager
  )
  {
    final String identity = (String) request.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
    if (identity == null) {
      throw new ISE("Null identity.");
    }

    for (ResourceAction resourceAction : resourceActions) {
      final Access access = authorizationManager.authorize(
          identity,
          resourceAction.getResource(),
          resourceAction.getAction()
      );
      if (!access.isAllowed()) {
        request.setAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED, false);
        return access;
      }
    }

    request.setAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED, true);
    return ACCESS_OK;
  }


  /**
   * Check a list of caller-defined resources, after converting them into a list of resource-actions
   * using a caller provided function.
   *
   * If one of the resource-actions fails the authorization check, this method returns the failed
   * Access object from the check.
   *
   * Otherwise, return ACCESS_OK if all resource-actions were successfully authorized.
   *
   * This function will set the DRUID_AUTH_TOKEN_CHECKED attribute in the request.
   *
   * If this attribute is already set when this fImmutableList.<Class<?>>of(SupervisorManager.class, AuthorizationManager.class)unction is called, an exception is thrown.
   *
   * @param request HTTP request to be generated
   * @param resources List of resources
   * @param raGenerator Function that creates a resource-action from a resource
   * @param <ResType> Type of the resources in the resource list
   * @return ACCESS_OK or the Access object from the first failed check
   */
  public static <ResType> Access authorizeAllResourceActions(
      final HttpServletRequest request,
      final Collection<ResType> resources,
      final Function<? super ResType, ResourceAction> raGenerator,
      final AuthorizationManager authorizationManager
  )
  {
    final String identity = (String) request.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
    if (identity == null) {
      throw new ISE("Null identity.");
    }

    for (ResType resource : resources) {
      final ResourceAction resourceAction = raGenerator.apply(resource);
      final Access access = authorizationManager.authorize(
          identity,
          resourceAction.getResource(),
          resourceAction.getAction()
      );
      if (!access.isAllowed()) {
        request.setAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED, false);
        return access;
      }
    }

    request.setAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED, true);
    return ACCESS_OK;
  }

  /**
   * Check a list of caller-defined resources, after converting them into a list of resource-actions
   * using a caller provided function.
   *
   * If one of the resource-actions fails the authorization check, this method returns the failed
   * Access object from the check.
   *
   * Otherwise, return ACCESS_OK if all resource-actions were successfully authorized.
   *
   * @param resources List of resources
   * @param raGenerator Function that creates a resource-action from a resource
   * @param <ResType> Type of the resources in the resource list
   * @return ACCESS_OK or the Access object from the first failed check
   */
  public static <ResType> Access authorizeAllResourceActions(
      final Collection<ResType> resources,
      final Function<? super ResType, ResourceAction> raGenerator,
      final String user,
      final AuthorizationManager authorizationManager
  )
  {
    for (ResType resource : resources) {
      final ResourceAction resourceAction = raGenerator.apply(resource);
      final Access access = authorizationManager.authorize(
          user,
          resourceAction.getResource(),
          resourceAction.getAction()
      );
      if (!access.isAllowed()) {
        return access;
      }
    }

    return ACCESS_OK;
  }

  /**
   * Check a list of resource-actions using the AuthorizationInfo from the request.
   *
   * If one of the resource-actions fails the authorization check, this method returns the failed
   * Access object from the check.
   *
   * Otherwise, return ACCESS_OK if all resource-actions were successfully authorized.
   *
   * @param resourceActions A list of resource-actions to authorize
   * @return ACCESS_OK or the Access object from the first failed check
   */
  public static Access authorizeAllResourceActions(
      final String user,
      final AuthorizationManager authorizationManager,
      final List<ResourceAction> resourceActions
  )
  {
    for (ResourceAction resourceAction : resourceActions) {
      final Access access = authorizationManager.authorize(
          user,
          resourceAction.getResource(),
          resourceAction.getAction()
      );
      if (!access.isAllowed()) {
        return access;
      }
    }

    return ACCESS_OK;
  }

  /**
   * Filter a list of resource-actions using the request's AuthorizationInfo, returning a new list of
   * resource-actions that were authorized.
   *
   * This function will set the DRUID_AUTH_TOKEN_CHECKED attribute in the request.
   *
   * If this attribute is already set when this function is called, an exception is thrown.
   *
   * @param request HTTP request to be authorized
   * @param resources List of resources to be processed into resource-actions
   * @param resourceActionGenerator Function that creates a resource-action from a resource
   * @return A list containing the resource-actions from the resourceParser that were successfully authorized.
   */

  public static <ResType> List<ResType> filterAuthorizedResources(
      final HttpServletRequest request,
      final Collection<ResType> resources,
      final Function<? super ResType, ResourceAction> resourceActionGenerator,
      final AuthorizationManager authorizationManager
  )
  {
    final String identity = (String) request.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
    if (identity == null) {
      throw new ISE("Null identity.");
    }

    List<ResType> filteredResources = new ArrayList<>();
    for (ResType resource : resources) {
      final ResourceAction resourceAction = resourceActionGenerator.apply(resource);
      final Access access = authorizationManager.authorize(
          identity,
          resourceAction.getResource(),
          resourceAction.getAction()
      );
      if (access.isAllowed()) {
        filteredResources.add(resource);
      }
    }

    request.setAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED, filteredResources.size() > 0);
    return filteredResources;
  }

  /**
   * Function for the common pattern of generating a resource-action for reading from a datasource, using the
   * datasource name.
   */
  public static Function<String, ResourceAction> DATASOURCE_READ_RA_GENERATOR = new Function<String, ResourceAction>()
  {
    @Override
    public ResourceAction apply(String input)
    {
      return new ResourceAction(
          new Resource(input, ResourceType.DATASOURCE),
          Action.READ
      );
    }
  };


  /**
   * Function for the common pattern of generating a resource-action for reading from a datasource, using the
   * datasource name.
   */
  public static Function<String, ResourceAction> DATASOURCE_WRITE_RA_GENERATOR = new Function<String, ResourceAction>()
  {
    @Override
    public ResourceAction apply(String input)
    {
      return new ResourceAction(
          new Resource(input, ResourceType.DATASOURCE),
          Action.WRITE
      );
    }
  };
}
