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

package org.apache.hadoop.hbase.security.rbac;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Test the reading and writing of rbac permission in {@link org.apache.hadoop.hbase.catalog.MetaReader} and {@link org.apache.hadoop.hbase.catalog.MetaEditor}.
 */
public class TestTablePermissions {
  private static final Log LOG = LogFactory.getLog(TestTablePermissions.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
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

  private static byte[] TEST_TABLE = Bytes.toBytes("perms_test");
  private static byte[] TEST_TABLE2 = Bytes.toBytes("perms_test2");
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static byte[] TEST_QUALIFIER = Bytes.toBytes("col1");

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    ZKW = new ZooKeeperWatcher(UTIL.getConfiguration(),
      "TestMetaReaderEditor", ABORTABLE);
    HConnection connection =
      HConnectionManager.getConnection(UTIL.getConfiguration());
    CT = new CatalogTracker(ZKW, connection, ABORTABLE);
    CT.start();

    UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    UTIL.createTable(TEST_TABLE2, TEST_FAMILY);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBasicWrite() throws Exception {
    List<HRegionInfo> regions = MetaReader.getTableRegions(CT, TEST_TABLE);
    assertTrue(regions.size() > 0);
    // perms only stored against the first region
    HRegionInfo firstRegion = regions.get(0);

    // add some permissions
    AccessControlLists.addTablePermission(CT, firstRegion,
        "george", new TablePermission(TEST_TABLE, null,
            TablePermission.Action.READ, TablePermission.Action.WRITE));
    AccessControlLists.addTablePermission(CT, firstRegion,
        "hubert", new TablePermission(TEST_TABLE, null,
            TablePermission.Action.READ));
    AccessControlLists.addTablePermission(CT, firstRegion,
        "humphrey", new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_QUALIFIER,
            TablePermission.Action.READ));

    // retrieve the same
    ListMultimap<String,TablePermission> perms =
        AccessControlLists.getTablePermissions(CT, TEST_TABLE);
    List<TablePermission> userPerms = perms.get("george");
    assertNotNull("Should have read permissions for george", userPerms);
    assertEquals("Should have 1 permission for george", 1, userPerms.size());
    TablePermission permission = userPerms.get(0);
    assertTrue("Permission should be for " + TEST_TABLE,
        Bytes.equals(TEST_TABLE, permission.getTable()));
    assertNull("Column family should be empty", permission.getFamily());

    // check actions
    assertNotNull(permission.getActions());
    assertEquals(2, permission.getActions().length);
    List<TablePermission.Action> actions = Arrays.asList(permission.getActions());
    assertTrue(actions.contains(TablePermission.Action.READ));
    assertTrue(actions.contains(TablePermission.Action.WRITE));

    userPerms = perms.get("hubert");
    assertNotNull("Should have read permissions for hubert", userPerms);
    assertEquals("Should have 1 permission for hubert", 1, userPerms.size());
    permission = userPerms.get(0);
    assertTrue("Permission should be for " + TEST_TABLE,
        Bytes.equals(TEST_TABLE, permission.getTable()));
    assertNull("Column family should be empty", permission.getFamily());

    // check actions
    assertNotNull(permission.getActions());
    assertEquals(1, permission.getActions().length);
    actions = Arrays.asList(permission.getActions());
    assertTrue(actions.contains(TablePermission.Action.READ));
    assertFalse(actions.contains(TablePermission.Action.WRITE));

    userPerms = perms.get("humphrey");
    assertNotNull("Should have read permissions for humphrey", userPerms);
    assertEquals("Should have 1 permission for humphrey", 1, userPerms.size());
    permission = userPerms.get(0);
    assertTrue("Permission should be for " + TEST_TABLE,
        Bytes.equals(TEST_TABLE, permission.getTable()));
    assertTrue("Permission should be for family " + TEST_FAMILY,
        Bytes.equals(TEST_FAMILY, permission.getFamily()));
    assertTrue("Permission should be for qualifier " + TEST_QUALIFIER,
        Bytes.equals(TEST_QUALIFIER, permission.getQualifier()));

    // table 2 permissions
    List<HRegionInfo> table2regions = MetaReader.getTableRegions(CT, TEST_TABLE2);
    assertTrue(regions.size() > 0);
    // perms only stored against the first region
    HRegionInfo first = table2regions.get(0);
    AccessControlLists.addTablePermission(CT, first, "hubert",
        new TablePermission(TEST_TABLE2, null,
            TablePermission.Action.READ, TablePermission.Action.WRITE));

    // check full load
    Map<byte[],ListMultimap<String,TablePermission>> allPerms =
        AccessControlLists.loadAll(CT);
    assertEquals("Full permission map should have entries for both test tables",
        2, allPerms.size());

    userPerms = allPerms.get(TEST_TABLE).get("hubert");
    assertNotNull(userPerms);
    assertEquals(1, userPerms.size());
    permission = userPerms.get(0);
    assertTrue(Bytes.equals(TEST_TABLE, permission.getTable()));
    assertEquals(1, permission.getActions().length);
    assertEquals(TablePermission.Action.READ, permission.getActions()[0]);

    userPerms = allPerms.get(TEST_TABLE2).get("hubert");
    assertNotNull(userPerms);
    assertEquals(1, userPerms.size());
    permission = userPerms.get(0);
    assertTrue(Bytes.equals(TEST_TABLE2, permission.getTable()));
    assertEquals(2, permission.getActions().length);
    actions = Arrays.asList(permission.getActions());
    assertTrue(actions.contains(TablePermission.Action.READ));
    assertTrue(actions.contains(TablePermission.Action.WRITE));
  }

  @Test
  public void testSerialization() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    ListMultimap<String,TablePermission> permissions = ArrayListMultimap.create();
    permissions.put("george", new TablePermission(TEST_TABLE, null,
        TablePermission.Action.READ));
    permissions.put("george", new TablePermission(TEST_TABLE, TEST_FAMILY,
        TablePermission.Action.WRITE));
    permissions.put("george", new TablePermission(TEST_TABLE2, null,
        TablePermission.Action.READ));
    permissions.put("hubert", new TablePermission(TEST_TABLE2, null,
        TablePermission.Action.READ, TablePermission.Action.WRITE));

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    AccessControlLists.writePermissions(new DataOutputStream(bos),
        permissions, conf);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    ListMultimap<String,TablePermission> copy =
        AccessControlLists.readPermissions(new DataInputStream(bis), conf);

    checkMultimapEqual(permissions, copy);
  }

  public void checkMultimapEqual(ListMultimap<String,TablePermission> first,
      ListMultimap<String,TablePermission> second) {
    assertEquals(first.size(), second.size());
    for (String key : first.keySet()) {
      List<TablePermission> firstPerms = first.get(key);
      List<TablePermission> secondPerms = second.get(key);
      assertNotNull(secondPerms);
      assertEquals(firstPerms.size(), secondPerms.size());
      LOG.info("First permissions: "+firstPerms.toString());
      LOG.info("Second permissions: "+secondPerms.toString());
      for (TablePermission p : firstPerms) {
        assertTrue("Permission "+p.toString()+" not found", secondPerms.contains(p));
      }
    }
  }

  @Test
  public void testEquals() throws Exception {
    TablePermission p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    TablePermission p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ, TablePermission.Action.WRITE);
    p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.WRITE, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, TEST_FAMILY, TablePermission.Action.READ, TablePermission.Action.WRITE);
    p2 = new TablePermission(TEST_TABLE, TEST_FAMILY, TablePermission.Action.WRITE, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_QUALIFIER, TablePermission.Action.READ, TablePermission.Action.WRITE);
    p2 = new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_QUALIFIER, TablePermission.Action.WRITE, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    p2 = new TablePermission(TEST_TABLE, TEST_FAMILY, TablePermission.Action.READ);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.WRITE);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));
    p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ, TablePermission.Action.WRITE);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    p2 = new TablePermission(TEST_TABLE2, null, TablePermission.Action.READ);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p2 = new TablePermission(TEST_TABLE, null);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));
  }
}
