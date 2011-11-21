/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.security;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

@Category(SmallTests.class)
public class TestUser {
  private static Log LOG = LogFactory.getLog(TestUser.class);

  @Test
  public void testBasicAttributes() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    User user = User.createUserForTesting(conf, "simple", new String[]{"foo"});
    assertEquals("Username should match", "simple", user.getName());
    assertEquals("Short username should match", "simple", user.getShortName());
    // don't test shortening of kerberos names because regular Hadoop doesn't support them
  }

  @Test
  public void testRunAs() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    final User user = User.createUserForTesting(conf, "testuser", new String[]{"foo"});
    final PrivilegedExceptionAction<String> action = new PrivilegedExceptionAction<String>(){
      public String run() throws IOException {
          User u = User.getCurrent();
          return u.getName();
      }
    };

    String username = user.runAs(action);
    assertEquals("Current user within runAs() should match",
        "testuser", username);

    // ensure the next run is correctly set
    User user2 = User.createUserForTesting(conf, "testuser2", new String[]{"foo"});
    String username2 = user2.runAs(action);
    assertEquals("Second username should match second user",
        "testuser2", username2);

    // check the exception version
    username = user.runAs(new PrivilegedExceptionAction<String>(){
      public String run() throws Exception {
        return User.getCurrent().getName();
      }
    });
    assertEquals("User name in runAs() should match", "testuser", username);

    // verify that nested contexts work
    user2.runAs(new PrivilegedExceptionAction(){
      public Object run() throws IOException, InterruptedException{
        String nestedName = user.runAs(action);
        assertEquals("Nest name should match nested user", "testuser", nestedName);
        assertEquals("Current name should match current user",
            "testuser2", User.getCurrent().getName());
        return null;
      }
    });
  }

  /**
   * Make sure that we're returning a result for the current user.
   * Previously getCurrent() was returning null if not initialized on
   * non-secure Hadoop variants.
   */
  @Test
  public void testGetCurrent() throws Exception {
    User user1 = User.getCurrent();
    assertNotNull(user1.ugi);
    LOG.debug("User1 is "+user1.getName());

    for (int i =0 ; i< 100; i++) {
      User u = User.getCurrent();
      assertNotNull(u);
      assertEquals(user1.getName(), u.getName());
    }
  }

  /**
   * Test security configuration setup
   */
  @Test
  public void testInitialize() throws Exception {
    // test basic settings for authentication
    Configuration conf = new Configuration();
    conf.set(User.SECURITY_AUTHENTICATION_CONF_KEY, "kerberos");
    User.initialize(conf);
    assertTrue(User.isHBaseSecurityEnabled(conf));
    assertEquals("kerberos", conf.get(User.SECURITY_AUTHENTICATION_CONF_KEY));
    assertEquals("org.apache.hadoop.hbase.ipc.SecureRpcEngine",
        conf.get(HBaseRPC.RPC_ENGINE_PROP));
    assertEquals("org.apache.hadoop.hbase.security.token.TokenProvider",
        conf.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY));

    // test basic settings for authorization
    conf = new Configuration();
    conf.setBoolean(User.SECURITY_AUTHORIZATION_CONF_KEY, true);
    User.initialize(conf);
    assertTrue(User.isHBaseAuthorizationEnabled(conf));
    assertTrue(conf.getBoolean(User.SECURITY_AUTHORIZATION_CONF_KEY, false));
    assertEquals("org.apache.hadoop.hbase.security.access.AccessController",
        conf.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY));
    assertEquals("org.apache.hadoop.hbase.security.access.AccessController",
        conf.get(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY));

    // verify that coprocessors are correctly added to existing coprocessor config
    conf = new Configuration();
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, "com.example.CP1,com.example.CP2");
    conf.set(User.SECURITY_AUTHENTICATION_CONF_KEY, "kerberos");
    conf.setBoolean(User.SECURITY_AUTHORIZATION_CONF_KEY, true);
    User.initialize(conf);
    String[] regionCPs =
        conf.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, "")
            .split(",");
    assertNotNull(regionCPs);
    assertEquals(4, regionCPs.length);
    // AccessController should be ordered first, followed by TokenProvider
    assertEquals("org.apache.hadoop.hbase.security.access.AccessController",
        regionCPs[0]);
    assertEquals("org.apache.hadoop.hbase.security.token.TokenProvider",
        regionCPs[1]);
    assertEquals("com.example.CP1", regionCPs[2]);
    assertEquals("com.example.CP2", regionCPs[3]);
  }
}
