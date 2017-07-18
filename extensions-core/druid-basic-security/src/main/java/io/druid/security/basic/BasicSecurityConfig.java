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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BasicSecurityConfig
{
  @JsonCreator
  public BasicSecurityConfig(
      @JsonProperty("systemPrincipal") String systemPrincipal,
      @JsonProperty("systemPrincipalSecret") String systemPrincipalSecret,
      @JsonProperty("remapAuthNames") boolean remapAuthNames
  ){
    this.systemPrincipal = systemPrincipal;
    this.systemPrincipalSecret = systemPrincipalSecret;
    this.remapAuthNames = remapAuthNames;
  }

  @JsonProperty
  private final String systemPrincipal;

  @JsonProperty
  private final String systemPrincipalSecret;

  @JsonProperty
  private final boolean remapAuthNames;

  public String getSystemPrincipal()
  {
    return systemPrincipal;
  }

  public String getSystemPrincipalSecret()
  {
    return systemPrincipalSecret;
  }

  public boolean isRemapAuthNames()
  {
    return remapAuthNames;
  }

  @Override
  public String toString()
  {
    return "BasicSecurityConfig{" +
           "systemPrincipal='" + systemPrincipal + '\'' +
           ", remapAuthNames=" + remapAuthNames +
           '}';
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

    BasicSecurityConfig that = (BasicSecurityConfig) o;

    if (isRemapAuthNames() != that.isRemapAuthNames()) {
      return false;
    }
    if (getSystemPrincipal() != null
        ? !getSystemPrincipal().equals(that.getSystemPrincipal())
        : that.getSystemPrincipal() != null) {
      return false;
    }
    return getSystemPrincipalSecret() != null
           ? getSystemPrincipalSecret().equals(that.getSystemPrincipalSecret())
           : that.getSystemPrincipalSecret() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getSystemPrincipal() != null ? getSystemPrincipal().hashCode() : 0;
    result = 31 * result + (getSystemPrincipalSecret() != null ? getSystemPrincipalSecret().hashCode() : 0);
    result = 31 * result + (isRemapAuthNames() ? 1 : 0);
    return result;
  }
}
