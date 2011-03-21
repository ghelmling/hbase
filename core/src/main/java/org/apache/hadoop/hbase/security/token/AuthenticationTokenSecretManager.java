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

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.zookeeper.KeeperException;

public class AuthenticationTokenSecretManager
    extends SecretManager<AuthenticationTokenIdentifier> {

  static final String NAME_PREFIX = "SecretManager-";

  private static Log LOG = LogFactory.getLog(
      AuthenticationTokenSecretManager.class);

  private long lastKeyUpdate;
  private long keyUpdateInterval;
  private long tokenMaxLifetime;
  private ZKSecretWatcher zkWatcher;
  private LeaderElector leaderElector;

  private ConcurrentMap<Integer,AuthenticationKey> allKeys = Maps.newConcurrentMap();
  private AuthenticationKey currentKey;

  private AtomicInteger idSeq = new AtomicInteger();
  private AtomicLong tokenSeq = new AtomicLong();
  private String name;

  /**
   * Create a new secret manager instance for generating keys.
   * @param conf 
   * @param zk
   * @param keyUpdateInterval
   * @param tokenMaxLifetime
   */
  public AuthenticationTokenSecretManager(Configuration conf,
      ZooKeeperWatcher zk, String serverName,
      long keyUpdateInterval, long tokenMaxLifetime) {
    this.zkWatcher = new ZKSecretWatcher(conf, zk, this);
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenMaxLifetime = tokenMaxLifetime;
    this.leaderElector = new LeaderElector(zk, serverName);
    this.name = NAME_PREFIX+serverName;
  }

  public void start() {
    try {
      // populate any existing keys
      this.zkWatcher.start();
      // try to become leader
      this.leaderElector.start();
    } catch (KeeperException ke) {
      LOG.error("Zookeeper initialization failed", ke);
    }
  }

  public void stop() {
    this.leaderElector.stop("SecretManager stopping");
  }

  public boolean isMaster() {
    return leaderElector.isMaster();
  }

  public String getName() {
    return name;
  }

  @Override
  protected byte[] createPassword(AuthenticationTokenIdentifier identifier) {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    AuthenticationKey secretKey = currentKey;
    identifier.setKeyId(secretKey.getKeyId());
    identifier.setIssueDate(now);
    identifier.setExpirationDate(now + tokenMaxLifetime);
    identifier.setSequenceNumber(tokenSeq.getAndIncrement());
    return createPassword(WritableUtils.toByteArray(identifier),
        secretKey.getKey());
  }

  @Override
  public byte[] retrievePassword(AuthenticationTokenIdentifier identifier)
      throws InvalidToken {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    if (identifier.getExpirationDate() < now) {
      throw new InvalidToken("Token has expired");
    }
    AuthenticationKey masterKey = allKeys.get(identifier.getKeyId());
    if (masterKey == null) {
      throw new InvalidToken("Unknown master key for token (id="+
          identifier.getKeyId()+")");
    }
    // regenerate the password
    return createPassword(WritableUtils.toByteArray(identifier),
        masterKey.getKey());
  }

  @Override
  public AuthenticationTokenIdentifier createIdentifier() {
    return new AuthenticationTokenIdentifier();
  }

  public Token<AuthenticationTokenIdentifier> generateToken(String username) {
    AuthenticationTokenIdentifier ident =
        new AuthenticationTokenIdentifier(username);
    return new Token<AuthenticationTokenIdentifier>(ident, this);
  }

  public void addKey(AuthenticationKey key) throws IOException {
    // ignore zk changes when master
    if (leaderElector.isMaster()) {
      LOG.debug("Running as master, ignoring new key "+key.getKeyId());
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding new key "+key.getKeyId());
    }

    allKeys.putIfAbsent(key.getKeyId(), key);
    if (currentKey == null || key.getKeyId() > currentKey.getKeyId()) {
      currentKey = key;
    }
  }

  public void updateKey(AuthenticationKey key) throws IOException {
    // ignore zk changes when master
    if (leaderElector.isMaster()) {
      LOG.debug("Running as master, ignoring updated key "+key.getKeyId());
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating key "+key.getKeyId());
    }
    allKeys.put(key.getKeyId(), key);
  }

  void removeKey(Integer keyId) {
    // ignore zk changes when master
    if (leaderElector.isMaster()) {
      LOG.debug("Running as master, ignoring removed key "+keyId);
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing key "+keyId);
    }

    allKeys.remove(keyId);
  }

  AuthenticationKey getCurrentKey() {
    return currentKey;
  }

  AuthenticationKey getKey(int keyId) {
    return allKeys.get(keyId);
  }

  void removeExpiredKeys() throws IOException {
    if (!leaderElector.isMaster()) {
      LOG.info("Skipping removeExpiredKeys() because not running as master.");
      return;
    }

    long now = EnvironmentEdgeManager.currentTimeMillis();
    Iterator<AuthenticationKey> iter = allKeys.values().iterator();
    while (iter.hasNext()) {
      AuthenticationKey key = iter.next();
      if (key.getExpiration() < now) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removing expired key "+key.getKeyId());
        }
        iter.remove();
        zkWatcher.removeKeyFromZK(key);
      }
    }
  }

  void rollCurrentKey() throws IOException {
    if (!leaderElector.isMaster()) {
      LOG.info("Skipping rollCurrentKey() because not running as master.");
      return;
    }

    long now = EnvironmentEdgeManager.currentTimeMillis();
    AuthenticationKey prev = currentKey;
    AuthenticationKey newKey = new AuthenticationKey(idSeq.getAndIncrement(),
        Long.MAX_VALUE, // don't allow to expire until it's replace by a new key
        generateSecret());
    allKeys.putIfAbsent(newKey.getKeyId(), newKey);
    currentKey = newKey;
    zkWatcher.addKeyToZK(newKey);
    lastKeyUpdate = now;

    if (prev != null) {
      // make sure previous key is still stored
      prev.setExpiration(now + tokenMaxLifetime);
      allKeys.putIfAbsent(prev.getKeyId(), prev);
      zkWatcher.updateKeyInZK(prev);
    }
  }

  public static SecretKey createSecretKey(byte[] raw) {
    return SecretManager.createSecretKey(raw);
  }

  private class LeaderElector extends Thread implements Stoppable {
    private boolean stopped = false;
    /** Flag indicating whether we're in charge of rolling/expiring keys */
    private boolean isMaster = false;
    private ZKLeaderManager zkLeader;

    public LeaderElector(ZooKeeperWatcher watcher, String serverName) {
      setDaemon(true);
      setName("ZKSecretWatcher-leaderElector");
      zkLeader = new ZKLeaderManager(watcher,
          ZKUtil.joinZNode(zkWatcher.getRootKeyZNode(), "keymaster"),
          Bytes.toBytes(serverName), this);
    }

    public boolean isMaster() {
      return isMaster;
    }

    @Override
    public boolean isStopped() {
      return stopped;
    }

    @Override
    public void stop(String reason) {
      stopped = true;
      // prevent further key generation when stopping
      if (isMaster) {
        zkLeader.stepDownAsLeader();
      }
      isMaster = false;
      LOG.info("Stopping leader election, because: "+reason);
      interrupt();
    }

    public void run() {
      zkLeader.start();
      zkLeader.waitToBecomeLeader();
      isMaster = true;

      while (!stopped) {
        try {
          long now = EnvironmentEdgeManager.currentTimeMillis();

          // clear any expired
          removeExpiredKeys();

          if (lastKeyUpdate + keyUpdateInterval < now) {
            // roll a new master key
            rollCurrentKey();
          }
        } catch (IOException ioe) {
          LOG.error("Error updating keys", ioe);
        }

        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          LOG.debug("Interrupted waiting for next update", ie);
        }
      }
    }
  }
}
