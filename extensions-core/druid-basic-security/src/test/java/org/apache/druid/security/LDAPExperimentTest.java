/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.security;

import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.security.basic.authentication.validator.LDAPCredentialsValidator;
import org.apache.druid.server.security.AuthenticationResult;
import org.junit.runner.RunWith;
import org.junit.Test;

@RunWith(FrameworkRunner.class)
@CreateDS(name = "myDS",
    partitions = {
        @CreatePartition(name = "test", suffix = "dc=myorg,dc=com")
    })
@CreateLdapServer(transports = { @CreateTransport(protocol = "LDAP", address = "localhost", port = 23456)})
@ApplyLdifFiles({"users.ldif"})
public class LDAPExperimentTest extends AbstractLdapTestUnit {

  @Test
  public void test() throws Exception
  {
    //do whatever you need with `ldapServer`

    LdapServer myLdapserver = ldapServer;
    System.out.println(ldapServer);

    LDAPCredentialsValidator ldapCredentialsValidator = new LDAPCredentialsValidator(
        "ldap://localhost:23456",
        //"uid=ldaptest1,sn=Ldap,cn=Test1Ldap,ou=Users,dc=myorg,dc=com",
        "uid=admin2,ou=system",
        new DefaultPasswordProvider("secret2"),
        "cn=Test1Ldap,ou=Users,dc=myorg,dc=com",
        "sn=Ldap",
        "uid",
        null,
        null,
        null,
        null
    );

    AuthenticationResult authResult = ldapCredentialsValidator.validateCredentials(
        "a",
        "b",
        "ldaptest1",
        "12345".toCharArray()
    );

    System.out.println(authResult);

    //Thread.sleep(200000);
  }
}
