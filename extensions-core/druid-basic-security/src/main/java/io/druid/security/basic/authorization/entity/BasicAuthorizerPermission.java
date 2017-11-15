package io.druid.security.basic.authorization.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.server.security.ResourceAction;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class BasicAuthorizerPermission
{
  private final ResourceAction resourceAction;
  private final Pattern resourceNamePattern;

  @JsonCreator
  public BasicAuthorizerPermission(
      @JsonProperty("resourceAction") ResourceAction resourceAction,
      @JsonProperty("resourceNamePattern") Pattern resourceNamePattern
  )
  {
    this.resourceAction = resourceAction;
    this.resourceNamePattern = resourceNamePattern;
  }

  public BasicAuthorizerPermission(
      ResourceAction resourceAction
  )
  {
    this.resourceAction = resourceAction;
    try {
      this.resourceNamePattern = Pattern.compile(resourceAction.getResource().getName());
    }
    catch (PatternSyntaxException pse) {
      throw new BasicSecurityDBResourceException(
          "Invalid permission, resource name regex[%s] does not compile.",
          resourceAction.getResource().getName()
      );
    }
  }

  @JsonProperty
  public ResourceAction getResourceAction()
  {
    return resourceAction;
  }

  @JsonProperty
  public Pattern getResourceNamePattern()
  {
    return resourceNamePattern;
  }

  public static List<BasicAuthorizerPermission> makePermissionList(List<ResourceAction> resourceActions) {
    List<BasicAuthorizerPermission> permissions = new ArrayList<>();

    if (resourceActions == null) {
      return permissions;
    }

    for (ResourceAction resourceAction : resourceActions) {
      permissions.add(new BasicAuthorizerPermission(resourceAction));
    }
    return  permissions;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BasicAuthorizerPermission that = (BasicAuthorizerPermission) o;

    if (getResourceAction() != null
        ? !getResourceAction().equals(that.getResourceAction())
        : that.getResourceAction() != null) {
      return false;
    }
    return getResourceNamePattern() != null
           ? getResourceNamePattern().pattern().equals(that.getResourceNamePattern().pattern())
           : that.getResourceNamePattern() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getResourceAction() != null ? getResourceAction().hashCode() : 0;
    result = 31 * result + (getResourceNamePattern().pattern() != null
                            ? getResourceNamePattern().pattern().hashCode()
                            : 0);
    return result;
  }
}
