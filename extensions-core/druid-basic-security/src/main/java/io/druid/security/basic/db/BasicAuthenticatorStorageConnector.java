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

package io.druid.security.basic.db;

import java.util.List;
import java.util.Map;

public interface BasicAuthenticatorStorageConnector
{
  void createUser(String userName);

  void deleteUser(String userName);

  List<Map<String, Object>> getAllUsers();

  Map<String, Object> getUser(String userName);

  void setUserCredentials(String userName, char[] password);

  boolean checkCredentials(String userName, char[] password);

  Map<String, Object> getUserCredentials(String userName);

  void createUserTable();

  void createUserCredentialsTable();
}
