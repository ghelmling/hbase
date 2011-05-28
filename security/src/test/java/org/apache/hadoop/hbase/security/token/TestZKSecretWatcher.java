/*
 * Copyright 2011 The Apache Software Foundation
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

package org.apache.hadoop.hbase.security.token;

import static org.junit.Assert.*;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the synchronization of token authentication master keys through
 * ZKSecretWatcher
 */
public class TestZKSecretWatcher {
  private static Log LOG = LogFactory.getLog(TestZKSecretWatcher.class);
  private static HBaseTestingUtility TEST_UTIL;
  private static AuthenticationTokenSecretManager KEY_MASTER;
  private static AuthenticationTokenSecretManager KEY_SLAVE;
  private static AuthenticationTokenSecretManager KEY_SLAVE2;

  private static class MockAbortable implements Abortable {
    private boolean abort;
    public void abort(String reason, Throwable e) {
      LOG.info("Aborting: "+reason, e);
      abort = true;
    }
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();

    ZooKeeperWatcher zk = newZK(conf, "server1", new MockAbortable());
    AuthenticationTokenSecretManager[] tmp = new AuthenticationTokenSecretManager[2];
    tmp[0] = new AuthenticationTokenSecretManager(
        conf, zk, "server1", 60*60*1000, 60*1000);
    tmp[0].start();

    zk = newZK(conf, "server2", new MockAbortable());
    tmp[1] = new AuthenticationTokenSecretManager(
        conf, zk, "server2", 60*60*1000, 60*1000);
    tmp[1].start();

    while (KEY_MASTER == null) {
      for (int i=0; i<2; i++) {
        if (tmp[i].isMaster()) {
          KEY_MASTER = tmp[i];
          KEY_SLAVE = tmp[ i+1 % 2 ];
          break;
        }
      }
      Thread.sleep(500);
    }
    LOG.info("Master is "+KEY_MASTER.getName()+
        ", slave is "+KEY_SLAVE.getName());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testKeyUpdate() throws Exception {
    // sanity check
    assertTrue(KEY_MASTER.isMaster());
    assertFalse(KEY_SLAVE.isMaster());

    KEY_MASTER.rollCurrentKey();
    AuthenticationKey key1 = KEY_MASTER.getCurrentKey();
    assertNotNull(key1);

    // wait for slave to update
    Thread.sleep(1000);
    AuthenticationKey slaveCurrent = KEY_SLAVE.getCurrentKey();
    assertNotNull(slaveCurrent);
    assertEquals(key1, slaveCurrent);

    // generate two more keys then expire the original
    KEY_MASTER.rollCurrentKey();
    AuthenticationKey key2 = KEY_MASTER.getCurrentKey();
    KEY_MASTER.rollCurrentKey();
    AuthenticationKey key3 = KEY_MASTER.getCurrentKey();

    // force expire the original key
    key1.setExpiration(EnvironmentEdgeManager.currentTimeMillis() - 1000);
    KEY_MASTER.removeExpiredKeys();
    // verify removed from master
    assertNull(KEY_MASTER.getKey(key1.getKeyId()));

    // wait for slave to catch up
    Thread.sleep(1000);
    // make sure the slave has both new keys
    AuthenticationKey slave2 = KEY_SLAVE.getKey(key2.getKeyId());
    assertNotNull(slave2);
    assertEquals(key2, slave2);
    AuthenticationKey slave3 = KEY_SLAVE.getKey(key3.getKeyId());
    assertNotNull(slave3);
    assertEquals(key3, slave3);
    slaveCurrent = KEY_SLAVE.getCurrentKey();
    assertEquals(key3, slaveCurrent);

    // verify that the expired key has been removed
    assertNull(KEY_SLAVE.getKey(key1.getKeyId()));

    // bring up a new slave
    Configuration conf = TEST_UTIL.getConfiguration();
    ZooKeeperWatcher zk = newZK(conf, "server3", new MockAbortable());
    KEY_SLAVE2 = new AuthenticationTokenSecretManager(
        conf, zk, "server3", 60*60*1000, 60*1000);
    KEY_SLAVE2.start();

    Thread.sleep(1000);
    // verify the new slave has current keys (and not expired)
    slave2 = KEY_SLAVE2.getKey(key2.getKeyId());
    assertNotNull(slave2);
    assertEquals(key2, slave2);
    slave3 = KEY_SLAVE2.getKey(key3.getKeyId());
    assertNotNull(slave3);
    assertEquals(key3, slave3);
    slaveCurrent = KEY_SLAVE2.getCurrentKey();
    assertEquals(key3, slaveCurrent);
    assertNull(KEY_SLAVE2.getKey(key1.getKeyId()));

    // test leader failover
    KEY_MASTER.stop();

    // wait for master to stop
    Thread.sleep(1000);
    assertFalse(KEY_MASTER.isMaster());

    // check for a new master
    AuthenticationTokenSecretManager[] mgrs =
        new AuthenticationTokenSecretManager[]{ KEY_SLAVE, KEY_SLAVE2 };
    AuthenticationTokenSecretManager newMaster = null;
    int tries = 0;
    while (newMaster == null && tries++ < 5) {
      for (AuthenticationTokenSecretManager mgr : mgrs) {
        if (mgr.isMaster()) {
          newMaster = mgr;
          break;
        }
      }
      if (newMaster == null) {
        Thread.sleep(500);
      }
    }
    assertNotNull(newMaster);
  }

  private static ZooKeeperWatcher newZK(Configuration conf, String name,
      Abortable abort) throws Exception {
    ZooKeeperWatcher zk = new ZooKeeperWatcher(conf, name, abort);
    return zk;
  }
}
