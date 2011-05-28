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

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Synchronizes token encryption keys across cluster nodes.
 */
public class ZKSecretWatcher extends ZooKeeperListener {
  private static final String DEFAULT_ROOT_NODE = "tokenauth";
  private static final String DEFAULT_KEYS_PARENT = "keys";
  private static Log LOG = LogFactory.getLog(ZKSecretWatcher.class);

  private AuthenticationTokenSecretManager secretManager;
  private String baseKeyZNode;
  private String keysParentZNode;

  public ZKSecretWatcher(Configuration conf,
      ZooKeeperWatcher watcher,
      AuthenticationTokenSecretManager secretManager) {
    super(watcher);
    this.secretManager = secretManager;
    String keyZNodeParent = conf.get("zookeeper.znode.tokenauth.parent", DEFAULT_ROOT_NODE);
    this.baseKeyZNode = ZKUtil.joinZNode(watcher.baseZNode, keyZNodeParent);
    this.keysParentZNode = ZKUtil.joinZNode(baseKeyZNode, DEFAULT_KEYS_PARENT);
  }

  public void start() throws KeeperException {
    watcher.registerListener(this);
    // make sure the base node exists
    ZKUtil.createWithParents(watcher, keysParentZNode);

    if (ZKUtil.watchAndCheckExists(watcher, keysParentZNode)) {
      List<ZKUtil.NodeAndData> nodes =
          ZKUtil.watchAndGetNewChildren(watcher, keysParentZNode);
      refreshNodes(nodes);
    }
  }

  @Override
  public void nodeCreated(String path) {
    if (path.equals(keysParentZNode)) {
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.watchAndGetNewChildren(watcher, keysParentZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        LOG.error("Error reading data from zookeeper", ke);
      }
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (keysParentZNode.equals(ZKUtil.getParent(path))) {
      String keyId = ZKUtil.getNodeName(path);
      try {
        Integer id = new Integer(keyId);
        secretManager.removeKey(id);
      } catch (NumberFormatException nfe) {
        LOG.error("Invalid znode name for key ID '"+keyId+"'", nfe);
      }
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    if (keysParentZNode.equals(ZKUtil.getParent(path))) {
      try {
        byte[] data = ZKUtil.getDataAndWatch(watcher, path);
        AuthenticationKey key = (AuthenticationKey)Writables.getWritable(data,
            new AuthenticationKey());
        secretManager.updateKey(key);
      } catch (KeeperException ke) {
        LOG.error("Error reading data from zookeeper", ke);
      } catch (IOException ioe) {
        LOG.error("Error reading permissions writables", ioe);
      }
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(keysParentZNode)) {
      // keys changed
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.watchAndGetNewChildren(watcher, keysParentZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        LOG.error("Error reading data from zookeeper", ke);
      }
    }
  }

  public String getRootKeyZNode() {
    return baseKeyZNode;
  }

  private void refreshNodes(List<ZKUtil.NodeAndData> nodes) {
    for (ZKUtil.NodeAndData n : nodes) {
      String path = n.getNode();
      String keyId = ZKUtil.getNodeName(path);
      try {
        byte[] data = n.getData();
        if (data == null || data.length == 0) {
          LOG.debug("Ignoring empty node "+path);
          continue;
        }
        AuthenticationKey key = (AuthenticationKey)Writables.getWritable(
            data, new AuthenticationKey());
        secretManager.addKey(key);
      } catch (IOException ioe) {
        LOG.error("Failed reading new secret key for id '" + keyId +
            "' from zk", ioe);
      }
    }
  }

  private String getKeyNode(AuthenticationKey key) {
    return ZKUtil.joinZNode(keysParentZNode, Integer.toString(key.getKeyId()));
  }

  public void removeKeyFromZK(AuthenticationKey key) throws IOException {
    String keyZNode = getKeyNode(key);
    try {
      ZKUtil.deleteNode(watcher, keyZNode);
    } catch (KeeperException ke) {
      LOG.error("Failed removing znode "+keyZNode+" for key "+key.getKeyId(),
          ke);
    }
  }

  public void addKeyToZK(AuthenticationKey key) throws IOException {
    String keyZNode = getKeyNode(key);
    byte[] keyData = Writables.getBytes(key);
    try {
      // TODO: is there any point in retrying beyond what ZK client does?
      ZKUtil.createSetData(watcher, keyZNode, keyData);
    } catch (KeeperException ke) {
      LOG.fatal("Unable to synchronize master key "+key.getKeyId()+
          " to znode "+keyZNode, ke);
      throw new RuntimeException("Unable to synchronize secret key "+
          key.getKeyId()+" in zookeeper", ke);
    }
  }

  public void updateKeyInZK(AuthenticationKey key) throws IOException {
    String keyZNode = getKeyNode(key);
    byte[] keyData = Writables.getBytes(key);
    try {
      try {
        ZKUtil.updateExistingNodeData(watcher, keyZNode, keyData, -1);
      } catch (KeeperException.NoNodeException ne) {
        // node was somehow removed, try adding it back
        ZKUtil.createSetData(watcher, keyZNode, keyData);
      }
    } catch (KeeperException ke) {
      LOG.fatal("Unable to update master key "+key.getKeyId()+
          " in znode "+keyZNode);
      throw new RuntimeException("Unable to synchronize secret key "+
          key.getKeyId()+" in zookeeper", ke);
    }
  }
}
