/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserverCoprocessor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.SecureServer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSecretManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.*;

public class AccessController extends BaseRegionObserverCoprocessor
    implements MasterObserver, AccessControllerProtocol {
  public static final Log LOG = LogFactory.getLog(AccessController.class);

  /**
   * Version number for AccessControllerProtocol
   */
  private static final long PROTOCOL_VERSION = 28L;

  TableAuthManager authManager = null;

  boolean isMetaRegion = false;

  // defined only for Endpoint implementation, so it can have way to
  // access region services.
  private RegionCoprocessorEnvironment regionEnv;

  private AuthenticationTokenSecretManager secretManager;

  /** Mapping of scanner instances to the user who created them */
  private Map<InternalScanner,String> scannerOwners =
      new MapMaker().weakKeys().makeMap();

  void openMetaRegion(RegionCoprocessorEnvironment e) throws IOException {
    final HRegion region = e.getRegion();

    Map<byte[],ListMultimap<String,TablePermission>> tables =
        AccessControlLists.loadAll(region);
    // For each table, write out the table's permissions to the respective
    // znode for that table.
    for (Map.Entry<byte[],ListMultimap<String,TablePermission>> t:
      tables.entrySet()) {
      byte[] table = t.getKey();
      String tableName = Bytes.toString(table);
      ListMultimap<String,TablePermission> perms = t.getValue();
      byte[] serialized = AccessControlLists.writePermissionsAsBytes(perms,
          e.getRegion().getConf());
      this.authManager.getZKPermissionWatcher().writeToZookeeper(tableName,
        serialized);
    }
  }

  /**
   * Writes all table ACLs for the tables in the given Map up into ZooKeeper
   * znodes.
   */
  void updateACL(RegionCoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap) {
    Set<String> tableSet = new HashSet<String>();
    for (Map.Entry<byte[], List<KeyValue>> f : familyMap.entrySet()) {
      List<KeyValue> kvs = f.getValue();
      for (KeyValue kv: kvs) {
        if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(),
            kv.getFamilyLength(), HConstants.ACL_FAMILY, 0,
            HConstants.ACL_FAMILY.length) == 0) {
          String row = Bytes.toString(kv.getRow());
          String tableName = row.substring(0, row.indexOf(","));
          tableSet.add(tableName);
        }
      }
    }
    CatalogTracker ct = e.getRegionServerServices().getCatalogTracker();
    for (String tableName: tableSet) {
      try {
        ListMultimap<String,TablePermission> perms =
          AccessControlLists.getTablePermissions(ct, Bytes.toBytes(tableName));
        byte[] serialized = AccessControlLists.writePermissionsAsBytes(
            perms, e.getRegion().getConf());
        this.authManager.getZKPermissionWatcher().writeToZookeeper(tableName,
          serialized);
      } catch (IOException ex) {
        LOG.error("Failed updating permissions mirror for '" + tableName +
          "'", ex);
      }
    }
  }

  void updateACL(RegionCoprocessorEnvironment e, final KeyValue kv) {
    if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(),
        kv.getFamilyLength(), HConstants.ACL_FAMILY, 0,
        HConstants.ACL_FAMILY.length) == 0) {
      String row = Bytes.toString(kv.getRow());
      String tableName = row.substring(0, row.indexOf(","));
      CatalogTracker ct = e.getRegionServerServices().getCatalogTracker();
      try {
        ListMultimap<String,TablePermission> perms =
          AccessControlLists.getTablePermissions(ct, Bytes.toBytes(tableName));
        byte[] serialized = AccessControlLists.writePermissionsAsBytes(perms,
            e.getRegion().getConf());
        this.authManager.getZKPermissionWatcher().writeToZookeeper(tableName,
          serialized); 
      } catch (IOException ex) {
        LOG.error("Failed updating permissions mirror for '" + tableName +
          "'", ex);
      }
    }
  }

  boolean permissionGranted(TablePermission.Action permRequest,
      RegionCoprocessorEnvironment e, Collection<byte[]> families) {
    HRegionInfo hri = e.getRegion().getRegionInfo();
    HTableDescriptor htd = hri.getTableDesc();

    // 1. All users need read access to .META. and -ROOT- tables; also, this is a very
    // common call to permissionGranted(), so deal with it quickly.
    if ((isMetaRegion || (htd.isRootRegion())) &&
        (permRequest == TablePermission.Action.READ)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("All users are allowed to " + permRequest.toString() +
          " the table '" + htd.getNameAsString() + "'");
      }
      return true;
    }

    // 2. Get owner of this table: owners can do anything, (including the 
    // specific permRequest requested).
    // Note that .META. and -ROOT- set on creation to be owned by the system
    // user: (see MasterFileSystem.java:bootstrap()), so that only system user
    // may write to them.  Of course, other users may be later granted write
    // access to these tables if desired.
    String owner = htd.getOwnerString();
    if (owner == null) {
      LOG.debug("Owner of '" + htd.getNameAsString() + " is (incorrectly) null.");
    }

    UserGroupInformation user = RequestContext.getRequestUser();
    if (user == null) {
      LOG.info("No user associated with request.  Permission denied!");
      return false;
    }

    if (user.getShortUserName().equals(owner)) {
      // owner of table can do anything to the table.
      if (LOG.isDebugEnabled()) {
        LOG.debug("User '" + user.getShortUserName() + "' is owner: allowed to " +
          permRequest.toString() + " the table '" + htd.getNameAsString() +
          "'");
      }
      return true;
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("User '" + user.getShortUserName() +
        "' is not owner of the table '" + htd.getNameAsString() +
        "' (owner is : '" + owner + "')");
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Owner-based authorization did not succeed, " +
        "continuing with user-based authorization check");
    }
    boolean result = false;

    // 3. get permissions for this user for table with desc tableDesc.
    if (families != null && families.size() > 0) {
      // all families must pass
      result = true;
      for (byte[] family : families) {
        result = result &&
            authManager.authorize(user, htd.getName(), family, permRequest);
        if (!result) {
          break;  //failed
        }
      }
    } else {
      // just check for the table-level
      result = authManager.authorize(user, htd.getName(), (byte[])null, permRequest);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("User '" + user.getShortUserName() + "' is " +
        (result ? "" : "not ") + "allowed to " +
        permRequest.toString() + " the table '" + htd.getNameAsString() +
        "'");
    }

    return result;
  }

  public void requirePermission(Permission.Action perm,
      MasterCoprocessorEnvironment env) throws IOException {
    UserGroupInformation user = RequestContext.getRequestUser();
    if (!RequestContext.isInRequestContext()) {
      // for non-rpc handling, fallback to system user
      user = UserGroupInformation.getCurrentUser();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking authorization of user '" +
          (user != null ? user.getShortUserName() : "NULL") + "' for action " +
          perm.toString());
    }
    if (!authManager.authorize(user, perm)) {
      throw new AccessDeniedException("Insufficient permissions for user '" +
          (user != null ? user.getShortUserName() : "null") +"' (global, action=" +
          perm.toString() + ")");
    }
  }

  /**
   * Authorizes that the current user has permission to perform the given
   * action on the set of table column families.
   * @param perm Action that is required
   * @param env The current coprocessor environment
   * @param families The set of column families present/required in the request
   * @throws AccessDeniedException if the authorization check failed
   */
  public void requirePermission(Permission.Action perm,
        RegionCoprocessorEnvironment env, Collection<byte[]> families)
      throws IOException {
    if (!permissionGranted(perm, env, families)) {
      throw new AccessDeniedException("Insufficient permissions (table=" +
        env.getRegion().getTableDesc().getNameAsString()+", action=" +
        perm.toString() + ")");
    }
  }

  /**
   * Returns <code>true</code> if the current user is allowed the given action
   * over at least one of the column qualifiers in the given column families.
   */
  public boolean hasFamilyQualifierPermission(TablePermission.Action perm,
      RegionCoprocessorEnvironment env, Collection<byte[]> families)
    throws IOException {
    HRegionInfo hri = env.getRegion().getRegionInfo();
    HTableDescriptor htd = hri.getTableDesc();
    byte[] tableName = htd.getName();

    UserGroupInformation user = RequestContext.getRequestUser();
    if (user == null) {
      LOG.info("No user associated with request.  Permission denied!");
      return false;
    }

    if (families != null && families.size() > 0) {
      // at least one family must be allowed
      for (byte[] fam : families) {
        if (authManager.matchFamilyPermission(user, tableName, fam, perm)) {
          return true;
        }
      }
    }

    return false;
  }

  /* ---- MasterObserver implementation ---- */
  public void start(CoprocessorEnvironment env) {
    // if running on HMaster
    if (env instanceof MasterCoprocessorEnvironment) {
      MasterCoprocessorEnvironment e = (MasterCoprocessorEnvironment)env;
      this.authManager = TableAuthManager.get(
          e.getMasterServices().getZooKeeperWatcher(),
          e.getConf());
    }

    // if running at region
    if (env instanceof RegionCoprocessorEnvironment) {
      regionEnv = (RegionCoprocessorEnvironment)env;
      RpcServer server = regionEnv.getRegionServerServices().getRpcServer();
      if (server instanceof SecureServer) {
        SecretManager mgr = ((SecureServer)server).getSecretManager();
        if (mgr instanceof AuthenticationTokenSecretManager) {
          secretManager = (AuthenticationTokenSecretManager)mgr;
        }
      }
    }
  }

  public void stop(CoprocessorEnvironment env) {

  }

  @Override
  public void preCreateTable(MasterCoprocessorEnvironment env,
      HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    requirePermission(Permission.Action.CREATE, env);
  }
  @Override
  public void postCreateTable(MasterCoprocessorEnvironment env,
      HRegionInfo[] regions, boolean sync) throws IOException {}

  @Override
  public void preDeleteTable(MasterCoprocessorEnvironment env, byte[] tableName)
      throws IOException {
    requirePermission(Permission.Action.CREATE, env);
  }
  @Override
  public void postDeleteTable(MasterCoprocessorEnvironment env,
      byte[] tableName) throws IOException {}


  @Override
  public void preModifyTable(MasterCoprocessorEnvironment env, byte[] tableName,
      HTableDescriptor htd) throws IOException {
    requirePermission(Permission.Action.CREATE, env);
  }
  @Override
  public void postModifyTable(MasterCoprocessorEnvironment env,
      byte[] tableName, HTableDescriptor htd) throws IOException {}


  @Override
  public void preAddColumn(MasterCoprocessorEnvironment env, byte[] tableName,
      HColumnDescriptor column) throws IOException {
    requirePermission(Permission.Action.CREATE, env);
  }
  @Override
  public void postAddColumn(MasterCoprocessorEnvironment env, byte[] tableName,
      HColumnDescriptor column) throws IOException {}


  @Override
  public void preModifyColumn(MasterCoprocessorEnvironment env,
      byte[] tableName, HColumnDescriptor descriptor) throws IOException {
    requirePermission(Permission.Action.CREATE, env);
  }
  @Override
  public void postModifyColumn(MasterCoprocessorEnvironment env,
      byte[] tableName, HColumnDescriptor descriptor) throws IOException {}


  @Override
  public void preDeleteColumn(MasterCoprocessorEnvironment env,
      byte[] tableName, byte[] c) throws IOException {
    requirePermission(Permission.Action.CREATE, env);
  }
  @Override
  public void postDeleteColumn(MasterCoprocessorEnvironment env,
      byte[] tableName, byte[] c) throws IOException {}


  @Override
  public void preEnableTable(MasterCoprocessorEnvironment env, byte[] tableName)
      throws IOException {
    // TODO: enable/disable required to alter a table, should ADMIN be required here?
    requirePermission(Permission.Action.ADMIN, env);
  }
  @Override
  public void postEnableTable(MasterCoprocessorEnvironment env,
      byte[] tableName) throws IOException {}

  @Override
  public void preDisableTable(MasterCoprocessorEnvironment env,
      byte[] tableName) throws IOException {
    // TODO: enable/disable required to alter a table, should ADMIN be required here?
    requirePermission(Permission.Action.ADMIN, env);
  }
  @Override
  public void postDisableTable(MasterCoprocessorEnvironment env,
      byte[] tableName) throws IOException {}

  @Override
  public void preMove(MasterCoprocessorEnvironment env, HRegionInfo region,
      HServerInfo srcServer, HServerInfo destServer)
    throws IOException {
    requirePermission(Permission.Action.ADMIN, env);
  }
  @Override
  public void postMove(MasterCoprocessorEnvironment env, HRegionInfo region,
      HServerInfo srcServer, HServerInfo destServer)
    throws UnknownRegionException {}

  @Override
  public void preAssign(MasterCoprocessorEnvironment env, byte[] regionName,
      boolean force) throws IOException {
    requirePermission(Permission.Action.ADMIN, env);
  }
  @Override
  public void postAssign(MasterCoprocessorEnvironment env,
      HRegionInfo regionInfo) throws IOException {}

  @Override
  public void preUnassign(MasterCoprocessorEnvironment env, byte[] regionName,
      boolean force) throws IOException {
    requirePermission(Permission.Action.ADMIN, env);
  }
  @Override
  public void postUnassign(MasterCoprocessorEnvironment env,
      HRegionInfo regionInfo, boolean force) throws IOException {}

  @Override
  public void preBalance(MasterCoprocessorEnvironment env) throws IOException {
    requirePermission(Permission.Action.ADMIN, env);
  }
  @Override
  public void postBalance(MasterCoprocessorEnvironment env)
      throws IOException {}

  @Override
  public boolean preBalanceSwitch(MasterCoprocessorEnvironment env,
      boolean newValue) throws IOException {
    requirePermission(Permission.Action.ADMIN, env);
    return newValue;
  }
  @Override
  public void postBalanceSwitch(MasterCoprocessorEnvironment env,
      boolean oldValue, boolean newValue) throws IOException {}

  @Override
  public void preShutdown(MasterCoprocessorEnvironment env) throws IOException {
    requirePermission(Permission.Action.ADMIN, env);
  }

  @Override
  public void preStopMaster(MasterCoprocessorEnvironment env)
      throws IOException {
    requirePermission(Permission.Action.ADMIN, env);
  }

  /* ---- RegionObserver implementation ---- */

  @Override
  public void postOpen(RegionCoprocessorEnvironment e) {
    final HRegion region = e.getRegion();
    HRegionInfo regionInfo = null;
    HTableDescriptor tableDesc = null;
    if (region != null) {
      regionInfo = region.getRegionInfo();
      if (regionInfo != null) {
        tableDesc = regionInfo.getTableDesc();
      }
    }

    this.authManager = TableAuthManager.get(
        e.getRegionServerServices().getZooKeeperWatcher(),
        e.getRegion().getConf());

    if (regionInfo.isRootRegion()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Opening -ROOT-, no op");
      }
      return;
    }

    if (tableDesc.isMetaRegion()) {
      isMetaRegion = true;
      try {
        openMetaRegion(e);
      } catch (IOException ex) {
        LOG.error("Failed to initialize permissions mirror", ex);
      }
    }
  }

  @Override
  public void preGetClosestRowBefore(final RegionCoprocessorEnvironment e,
      final byte [] row, final byte [] family, final Result result)
      throws IOException {
    requirePermission(TablePermission.Action.READ, e,
        (family != null ? Lists.newArrayList(family) : null));
  }

  @Override
  public void preGet(final RegionCoprocessorEnvironment e, final Get get,
      final List<KeyValue> result) throws IOException {
    /*
     if column family level checks fail, check for a qualifier level permission
     in one of the families.  If it is present, then continue with the AccessControlFilter.
      */
    if (!permissionGranted(TablePermission.Action.READ, e, get.familySet())) {
      if (hasFamilyQualifierPermission(TablePermission.Action.READ, e,
          get.familySet())) {
        byte[] table = getTableName(e);
        AccessControlFilter filter = new AccessControlFilter(authManager,
            UserGroupInformation.getCurrentUser(), table);

        // wrap any existing filter
        if (get.getFilter() != null) {
          FilterList wrapper = new FilterList(FilterList.Operator.MUST_PASS_ALL,
              Lists.newArrayList(filter, get.getFilter()));
          get.setFilter(wrapper);
        } else {
          get.setFilter(filter);
        }
      }
    }
  }

  @Override
  public boolean preExists(final RegionCoprocessorEnvironment e, final Get get,
      final boolean exists) throws IOException {
    requirePermission(TablePermission.Action.READ, e, get.familySet());
    return exists;
  }

  @Override
  public void prePut(final RegionCoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
      throws IOException {
    requirePermission(TablePermission.Action.WRITE, e, familyMap.keySet());
  }

  @Override
  public void postPut(final RegionCoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL) {
    if (isMetaRegion) {
      updateACL(e, familyMap);
    }
  }

  @Override
  public void preDelete(final RegionCoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
      throws IOException {
    requirePermission(TablePermission.Action.WRITE, e, familyMap.keySet());
  }

  @Override
  public void postDelete(final RegionCoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
      throws IOException {
    if (isMetaRegion) {
      updateACL(e, familyMap);
    }
  }

  @Override
  public boolean preCheckAndPut(final RegionCoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Put put, final boolean result)
      throws IOException {
    requirePermission(TablePermission.Action.READ, e, 
        Arrays.asList(new byte[][]{family}));
    return result;
  }

  @Override
  public boolean preCheckAndDelete(final RegionCoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Delete delete, final boolean result)
      throws IOException {
    requirePermission(TablePermission.Action.READ, e, 
        Arrays.asList( new byte[][] {family}));
    return result;
  }

  @Override
  public long preIncrementColumnValue(final RegionCoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL)
      throws IOException {
    requirePermission(TablePermission.Action.READ, e, 
        Arrays.asList(new byte[][]{family}));
    return -1;
  }

  @Override
  public void preIncrement(final RegionCoprocessorEnvironment e,
      final Increment increment, final Result result)
      throws IOException {
    requirePermission(TablePermission.Action.READ, e,
        increment.getFamilyMap().keySet());
  }

  @Override
  public InternalScanner preScannerOpen(final RegionCoprocessorEnvironment e,
      final Scan scan, final InternalScanner s) throws IOException {
    /*
     if column family level checks fail, check for a qualifier level permission
     in one of the families.  If it is present, then continue with the AccessControlFilter.
      */
    UserGroupInformation user = RequestContext.getRequestUser();
    List<byte[]> families = Arrays.asList(scan.getFamilies());
    if (!permissionGranted(TablePermission.Action.READ, e, families)) {
      if (hasFamilyQualifierPermission(TablePermission.Action.READ, e, families)) {
        byte[] table = getTableName(e);
        AccessControlFilter filter = new AccessControlFilter(authManager,
            user, table);

        // wrap any existing filter
        if (scan.hasFilter()) {
          FilterList wrapper = new FilterList(FilterList.Operator.MUST_PASS_ALL,
              Lists.newArrayList(filter, scan.getFilter()));
          scan.setFilter(wrapper);
        } else {
          scan.setFilter(filter);
        }
      } else {
        // no table/family level perms and no qualifier level perms, reject
        throw new AccessDeniedException("Insufficient permissions for user '"+
            (user != null ? user.getShortUserName() : "null")+"' "+
            "for scanner open on table "+getTableName(e));
      }
    }
    return s;
  }

  @Override
  public InternalScanner postScannerOpen(final RegionCoprocessorEnvironment e,
      final Scan scan, final InternalScanner s) throws IOException {
    UserGroupInformation user = RequestContext.getRequestUser();
    if (user != null && user.getShortUserName() != null) {
      // store reference to scanner owner for later checks
      scannerOwners.put(s, user.getShortUserName());
    }
    return s;
  }

  @Override
  public boolean preScannerNext(final RegionCoprocessorEnvironment e,
      final InternalScanner s, final List<KeyValue> result,
      final int limit, final boolean hasNext) throws IOException {
    // verify that requesting user matches the user who created the scanner
    // if so, we assume that access control is correctly enforced based on
    // the checks performed in preScannerOpen()
    if (RequestContext.isInRequestContext()) {
      String owner = scannerOwners.get(s);
      if (owner != null && !owner.equals(RequestContext.getRequestUserName())) {
        throw new AccessDeniedException("User '"+
            RequestContext.getRequestUserName()+"' is not the scanner owner!");
      }
    }
    return hasNext;
  }

  @Override
  public void preScannerClose(final RegionCoprocessorEnvironment e,
      final InternalScanner s) throws IOException {
    // Verify, when called through RPC, that the caller is the scanner owner
    if (RequestContext.isInRequestContext()) {
      String owner = scannerOwners.get(s);
      if (owner != null && !owner.equals(RequestContext.getRequestUserName())) {
        throw new AccessDeniedException("User '"+
            RequestContext.getRequestUserName()+"' is not the scanner owner!");
      }
    }
  }

  @Override
  public void postScannerClose(final RegionCoprocessorEnvironment e,
      final InternalScanner s) throws IOException {
    // clean up any associated owner mapping
    scannerOwners.remove(s);
  }

  // the following endpoint methods are provided for grant/revoke/list
  // permissions from client side. They're suppose to be executed only
  // at the .META. region. This restriction will be applied by both client
  // side and endpoint implementation.
  @Override
  public boolean grant(byte[] user, TablePermission permission)
      throws IOException {
    // verify it's only running at.META.
    if (isMetaRegion) {
      LOG.info("Receive request to grant access permission to '"
          + Bytes.toString(user) + "'. "
          + permission.toString());

      CatalogTracker tracker = this.getEnvironment().
          getRegionServerServices().getCatalogTracker();
      List<HRegionInfo> regions = MetaReader.getTableRegions(tracker,
          permission.getTable());

      // perms only stored against the first region
      HRegionInfo firstRegion = regions.get(0);

      AccessControlLists.addTablePermission(tracker, firstRegion,
          Bytes.toString(user), permission);
      LOG.info("Grant permission successfully.");
    } else {
      throw new CoprocessorException(AccessController.class, "This method " +
          "can only execute at " +
          Bytes.toString(HConstants.META_TABLE_NAME) + " table.");
    }
    return true;
  }

  @Override
  public boolean revoke(byte[] user, TablePermission permission)
      throws IOException{
    // verify it's only for .META.
    if (isMetaRegion) {
      LOG.info("Receive request to revoke access permission for '"
          + Bytes.toString(user) + "'. "
          + permission.toString());

      CatalogTracker tracker = this.getEnvironment().
          getRegionServerServices().getCatalogTracker();
      List<HRegionInfo> regions = MetaReader.getTableRegions(tracker,
          permission.getTable());

      // perms only stored against the first region
      HRegionInfo firstRegion = regions.get(0);

      AccessControlLists.removeTablePermission(tracker, firstRegion,
          Bytes.toString(user), permission);
      LOG.info("Revoke permission successfully.");
    } else {
      throw new CoprocessorException(AccessController.class, "This method " +
          "can only execute at " +
          Bytes.toString(HConstants.META_TABLE_NAME) + " table.");
    }
    return true;
  }

  @Override
  public List<UserPermission> getUserPermissions(final byte[] tableName)
      throws IOException {
    // verify it's only for .META.
    if (isMetaRegion) {
      CatalogTracker tracker = this.getEnvironment().
          getRegionServerServices().getCatalogTracker();

      List<UserPermission> perms = AccessControlLists.getUserPermissions
          (tracker, tableName);
      return perms;
    } else {
      throw new CoprocessorException(AccessController.class, "This method " +
          "can only execute at " +
          Bytes.toString(HConstants.META_TABLE_NAME) + " table.");
    }
  }

  @Override
  public Token<AuthenticationTokenIdentifier> getAuthenticationToken()
      throws IOException {
    if (secretManager == null) {
      throw new IOException(
          "No secret manager configured for token authentication");
    }

    UserGroupInformation currentUser = RequestContext.getRequestUser();
    if (currentUser == null) {
      throw new AccessDeniedException("No authenticated user for request!");
    } else if (currentUser.getAuthenticationMethod() !=
        UserGroupInformation.AuthenticationMethod.KERBEROS) {
      LOG.warn("Token generation denied for user="+currentUser.getUserName()
          +", authMethod="+currentUser.getAuthenticationMethod());
      throw new AccessDeniedException(
          "Token generation only allowed for Kerberos authenticated clients");
    }

    return secretManager.generateToken(currentUser.getUserName());
  }

  @Override
  public String whoami() {
    return RequestContext.getRequestUserName();
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return PROTOCOL_VERSION;
  }

  /**
   * @param e Coprocessor environment.
   */
  private void setEnvironment(RegionCoprocessorEnvironment e) {
    regionEnv = e;
  }

  /**
   * @return env Coprocessor environment.
   */
  public RegionCoprocessorEnvironment getEnvironment() {
    return regionEnv;
  }

  public byte[] getTableName(RegionCoprocessorEnvironment e) {
    HRegion region = e.getRegion();
    byte[] tableName = null;

    if (region != null) {
      HRegionInfo regionInfo = region.getRegionInfo();
      if (regionInfo != null) {
        HTableDescriptor tableDesc = regionInfo.getTableDesc();
        tableName = tableDesc.getName();
      }
    }
    return tableName;
  }
}
