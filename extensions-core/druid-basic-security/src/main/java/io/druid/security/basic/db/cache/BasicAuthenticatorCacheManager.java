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
import com.google.inject.Injector;
import io.druid.client.coordinator.Coordinator;
import io.druid.discovery.DruidLeaderClient;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.db.BasicAuthDBConfig;
import io.druid.security.basic.db.entity.BasicAuthenticatorUser;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BasicAuthenticatorCacheManager
{
  private final DruidLeaderClient druidLeaderClient;
  private final ConcurrentHashMap<String, Map<String, BasicAuthenticatorUser>> userMaps;

  @Inject
  public BasicAuthenticatorCacheManager(
      @Coordinator DruidLeaderClient druidLeaderClient,
      Injector injector
  )
  {
    this.druidLeaderClient = druidLeaderClient;
    this.userMaps = new ConcurrentHashMap<>();
  }

  @LifecycleStart
  public void start()
  {
    AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

    for (Map.Entry<String, Authenticator> entry : authenticatorMapper.getAuthenticatorMap().entrySet()) {
      Authenticator authenticator = entry.getValue();
      if (authenticator instanceof BasicHTTPAuthenticator) {
        String authenticatorName = entry.getKey();
        BasicHTTPAuthenticator basicHTTPAuthenticator = (BasicHTTPAuthenticator) authenticator;
        BasicAuthDBConfig dbConfig = basicHTTPAuthenticator.getDbConfig();

        retryTransaction(
            new TransactionCallback<Void>()
            {
              @Override
              public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
              {
                if (tableExists(handle, getPrefixedTableName(dbConfig.getDbPrefix(), USERS))) {
                  return null;
                }

                createUserTable(dbConfig.getDbPrefix());
                createUserCredentialsTable(dbConfig.getDbPrefix());

                makeDefaultSuperuser(
                    dbConfig.getDbPrefix(),
                    DEFAULT_ADMIN_NAME,
                    dbConfig.getInitialAdminPassword()
                );

                makeDefaultSuperuser(
                    dbConfig.getDbPrefix(),
                    DEFAULT_SYSTEM_USER_NAME,
                    dbConfig.getInitialInternalClientPassword()
                );

                return null;
              }
            },
            3,
            10
        );
      }
    }
  }

}
