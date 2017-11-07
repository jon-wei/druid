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

package io.druid.security.basic.db.cache;

import com.google.inject.Inject;
import io.druid.java.util.common.logger.Logger;
import io.druid.security.basic.db.BasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.db.entity.BasicAuthenticatorUser;

import java.util.Map;

public class CoordinatorBasicAuthenticatorCacheManager implements BasicAuthenticatorCacheManager
{
  private static final Logger log = new Logger(CoordinatorBasicAuthenticatorCacheManager.class);

  private final BasicAuthenticatorMetadataStorageUpdater storageUpdater;

  @Inject
  public CoordinatorBasicAuthenticatorCacheManager(
      BasicAuthenticatorMetadataStorageUpdater storageUpdater
  )
  {
    this.storageUpdater = storageUpdater;

    log.info("Starting COORDINATOR basic auth cache manager.");
  }

  @Override
  public void addAuthenticatorToUpdate(String authenticatorPrefix)
  {
  }

  @Override
  public Map<String, BasicAuthenticatorUser> getUserMap(String authenticatorPrefix)
  {
    return storageUpdater.getCachedUserMap(authenticatorPrefix);
  }
}
