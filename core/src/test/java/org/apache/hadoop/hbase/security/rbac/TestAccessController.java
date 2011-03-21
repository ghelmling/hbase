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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.rbac;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.coprocessor.Coprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Performs authorization checks for common operations, according to different
 * levels of authorized users.
 */
public class TestAccessController {
  private static Log LOG = LogFactory.getLog(TestAccessController.class);
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  // user with all permissions
  private static User SUPERUSER;
  // table owner user
  private static User USER_OWNER;
  // user with rw permissions
  private static User USER_RW;
  // user with read-only permissions
  private static User USER_RO;
  // user with no permissions
  private static User USER_NONE;

  private static byte[] TEST_TABLE = Bytes.toBytes("testtable");
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static MasterCoprocessorEnvironment CP_ENV;
  private static AccessController ACCESS_CONTROLLER;
  private static ZooKeeperWatcher ZKW;
  private static CatalogTracker CT;
  private final static Abortable ABORTABLE = new Abortable() {
    private final AtomicBoolean abort = new AtomicBoolean(false);

    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
      abort.set(true);
    }
  };

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set("hadoop.security.authorization", "true");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hbase.superuser", "admin");
    TEST_UTIL.startMiniCluster();
    ZKW = new ZooKeeperWatcher(conf, "TestMetaReaderEditor", ABORTABLE);
    HConnection connection =
      HConnectionManager.getConnection(conf);
    CT = new CatalogTracker(ZKW, connection, ABORTABLE);
    CT.start();
    MasterCoprocessorHost cpHost = TEST_UTIL.getMiniHBaseCluster()
        .getMaster().getCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.Priority.HIGHEST);
    ACCESS_CONTROLLER = (AccessController)cpHost.findCoprocessor(
        AccessController.class.getName());
    CP_ENV = cpHost.createEnvironment(AccessController.class, ACCESS_CONTROLLER,
        Coprocessor.Priority.HIGHEST);


    // create a set of test users
    SUPERUSER = createUser("admin", new String[]{"supergroup"});
    USER_OWNER = createUser("owner", new String[0]);
    USER_RW = createUser("rwuser", new String[0]);
    USER_RO = createUser("rouser", new String[0]);
    USER_NONE = createUser("nouser", new String[0]);

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
    htd.setOwnerString(USER_OWNER.getShortName());
    admin.createTable(htd);

    List<HRegionInfo> regions = MetaReader.getTableRegions(CT, TEST_TABLE);
    assertTrue(regions.size() > 0);
    // perms only stored against the first region
    HRegionInfo firstRegion = regions.get(0);

    AccessControlLists.addTablePermission(CT, firstRegion,
        USER_RW.getShortName(),
        new TablePermission(TEST_TABLE, null, Permission.Action.READ,
            Permission.Action.WRITE));
    AccessControlLists.addTablePermission(CT, firstRegion,
        USER_RO.getShortName(),
        new TablePermission(TEST_TABLE, null, Permission.Action.READ));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public static User createUser(String name, String[] groups) {
    // generate a test user
    User user = User.createUserForTesting(TEST_UTIL.getConfiguration(),
        name, groups);
    return user;
  }

  public static void enableAccessController(Configuration conf) {
    conf.set("hbase.coprocessor.master.classes", AccessController.class.getName());
    conf.set("hbase.coprocessor.region.classes", AccessController.class.getName());
  }

  public void verifyAllowed(User user, PrivilegedExceptionAction action)
    throws Exception {
    try {
      user.runAs(action);
    } catch (AccessDeniedException ade) {
      fail("Expected action to pass for user '" + user.getShortName() +
          "' but was denied");
    }
  }

  public void verifyDenied(User user, PrivilegedExceptionAction action)
    throws Exception {
    try {
      user.runAs(action);
      fail("Expected AccessDeniedException for user '" + user.getShortName() + "'");
    } catch (AccessDeniedException ade) {
      // expected result
    }
  }

  @Test
  public void testTableCreate() throws Exception {
    PrivilegedExceptionAction createTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor("testnewtable");
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        ACCESS_CONTROLLER.preCreateTable(CP_ENV, htd, null);
        return null;
      }
    };

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, createTable);

    // all others should be denied
    verifyDenied(USER_OWNER, createTable);
    verifyDenied(USER_RW, createTable);
    verifyDenied(USER_RO, createTable);
    verifyDenied(USER_NONE, createTable);
  }

  @Test
  public void testTableModify() throws Exception {
    PrivilegedExceptionAction disableTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
        htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
        htd.addFamily(new HColumnDescriptor("fam_"+User.getCurrent().getShortName()));
        ACCESS_CONTROLLER.preModifyTable(CP_ENV, TEST_TABLE, htd);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, disableTable);
    verifyDenied(USER_RW, disableTable);
    verifyDenied(USER_RO, disableTable);
    verifyDenied(USER_NONE, disableTable);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, disableTable);
  }

  @Test
  public void testTableDelete() throws Exception {
    PrivilegedExceptionAction disableTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteTable(CP_ENV, TEST_TABLE);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, disableTable);
    verifyDenied(USER_RW, disableTable);
    verifyDenied(USER_RO, disableTable);
    verifyDenied(USER_NONE, disableTable);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, disableTable);
  }

  @Test
  public void testAddColumn() throws Exception {
    final HColumnDescriptor hcd = new HColumnDescriptor("fam_new");
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preAddColumn(CP_ENV, TEST_TABLE, hcd);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testModifyColumn() throws Exception {
    final HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY);
    hcd.setMaxVersions(10);
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preModifyColumn(CP_ENV, TEST_TABLE, hcd);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testDeleteColumn() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDeleteColumn(CP_ENV, TEST_TABLE, TEST_FAMILY);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testTableDisable() throws Exception {
    PrivilegedExceptionAction disableTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preDisableTable(CP_ENV, TEST_TABLE);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, disableTable);
    verifyDenied(USER_RW, disableTable);
    verifyDenied(USER_RO, disableTable);
    verifyDenied(USER_NONE, disableTable);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, disableTable);
  }

  @Test
  public void testTableEnable() throws Exception {
    PrivilegedExceptionAction enableTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preEnableTable(CP_ENV, TEST_TABLE);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, enableTable);
    verifyDenied(USER_RW, enableTable);
    verifyDenied(USER_RO, enableTable);
    verifyDenied(USER_NONE, enableTable);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, enableTable);
  }

  @Test
  public void testMove() throws Exception {
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TEST_TABLE);
    Map<HRegionInfo,HServerAddress> regions = table.getRegionsInfo();
    final Map.Entry<HRegionInfo,HServerAddress> firstRegion =
        regions.entrySet().iterator().next();
    final HServerInfo server = TEST_UTIL.getHBaseCluster().getRegionServer(0).getHServerInfo();

    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preMove(CP_ENV, firstRegion.getKey(), server, server);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testAssign() throws Exception {
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TEST_TABLE);
    Map<HRegionInfo,HServerAddress> regions = table.getRegionsInfo();
    final Map.Entry<HRegionInfo,HServerAddress> firstRegion =
        regions.entrySet().iterator().next();

    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preAssign(CP_ENV,
            firstRegion.getKey().getRegionName(), false);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testUnassign() throws Exception {
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TEST_TABLE);
    Map<HRegionInfo,HServerAddress> regions = table.getRegionsInfo();
    final Map.Entry<HRegionInfo,HServerAddress> firstRegion =
        regions.entrySet().iterator().next();

    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preUnassign(CP_ENV, firstRegion.getKey().getRegionName(),
            false);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testBalance() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBalance(CP_ENV);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testBalanceSwitch() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preBalanceSwitch(CP_ENV, true);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testShutdown() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preShutdown(CP_ENV);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }

  @Test
  public void testStopMaster() throws Exception {
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preStopMaster(CP_ENV);
        return null;
      }
    };

    // all others should be denied
    verifyDenied(USER_OWNER, action);
    verifyDenied(USER_RW, action);
    verifyDenied(USER_RO, action);
    verifyDenied(USER_NONE, action);

    // verify that superuser can create tables
    verifyAllowed(SUPERUSER, action);
  }
}
