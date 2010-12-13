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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implements the coprocessor environment and runtime support.
 */
public class RegionCoprocessorHost extends CoprocessorHost<RegionCoprocessorEnvironment> {
  private static final Log LOG = LogFactory.getLog(RegionCoprocessorHost.class);

  /**
   * Encapsulation of the environment of each coprocessor
   */
  class RegionEnvironment extends CoprocessorHost.Environment
      implements RegionCoprocessorEnvironment {

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public RegionEnvironment(final Coprocessor impl, Coprocessor.Priority priority) {
      super(impl, priority);
    }

    /** @return the region */
    @Override
    public HRegion getRegion() {
      return region;
    }

    /** @return reference to the region server services */
    @Override
    public RegionServerServices getRegionServerServices() {
      return rsServices;
    }

    public void shutdown() {
      super.shutdown();
    }
  }

  static final Pattern attrSpecMatch = Pattern.compile("(.+):(.+):(.+)");

  /** The region server services */
  RegionServerServices rsServices;
  /** The region */
  HRegion region;

  /**
   * Constructor
   * @param region the region
   * @param rsServices interface to available region server functionality
   * @param conf the configuration
   */
  public RegionCoprocessorHost(final HRegion region,
      final RegionServerServices rsServices, final Configuration conf) {
    this.rsServices = rsServices;
    this.region = region;
    this.pathPrefix = this.region.getRegionNameAsString().replace(',', '_');

    // load system default cp's from configuration.
    loadSystemCoprocessors(conf);

    // load Coprocessor From HDFS
    loadTableCoprocessors();
  }

  void loadTableCoprocessors () {
    // scan the table attributes for coprocessor load specifications
    // initialize the coprocessors
    for (Map.Entry<ImmutableBytesWritable,ImmutableBytesWritable> e:
        region.getTableDesc().getValues().entrySet()) {
      String key = Bytes.toString(e.getKey().get());
      if (key.startsWith("COPROCESSOR")) {
        // found one
        try {
          String spec = Bytes.toString(e.getValue().get());
          Matcher matcher = attrSpecMatch.matcher(spec);
          if (matcher.matches()) {
            Path path = new Path(matcher.group(1));
            String className = matcher.group(2);
            Coprocessor.Priority priority =
              Coprocessor.Priority.valueOf(matcher.group(3));
            load(path, className, priority);
            LOG.info("Load coprocessor " + className + " from HTD of " +
                Bytes.toString(region.getTableDesc().getName()) +
                " successfully.");
          } else {
            LOG.warn("attribute '" + key + "' has invalid coprocessor spec");
          }
        } catch (IOException ex) {
            LOG.warn(StringUtils.stringifyException(ex));
        }
      }
    }
  }

  @Override
  public RegionCoprocessorEnvironment createEnvironment(
      Class<?> implClass, Coprocessor instance, Coprocessor.Priority priority) {
    // Check if it's an Endpoint.
    // Due to current dynamic protocol design, Endpoint
    // uses a different way to be registered and executed.
    // It uses a visitor pattern to invoke registered Endpoint
    // method.
    for (Class c : implClass.getInterfaces()) {
      if (CoprocessorProtocol.class.isAssignableFrom(c)) {
        region.registerProtocol(c, (CoprocessorProtocol)instance);
        break;
      }
    }

    return new RegionEnvironment(instance, priority);
  }

  /**
   * Invoked before a region open
   */
  public void preOpen() {
    loadTableCoprocessors();
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).preOpen(env);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * Invoked after a region open
   */
  public void postOpen() {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).postOpen(env);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * Invoked before a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void preClose(boolean abortRequested) {
    try {
      coprocessorLock.writeLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).preClose(env, abortRequested);
        }
        shutdown(env);
      }
    } finally {
      coprocessorLock.writeLock().unlock();
    }
  }

  /**
   * Invoked after a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void postClose(boolean abortRequested) {
    try {
      coprocessorLock.writeLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).postClose(env, abortRequested);
        }
        shutdown(env);
      }
    } finally {
      coprocessorLock.writeLock().unlock();
    }
  }

  /**
   * Invoked before a region is compacted.
   * @param willSplit true if the compaction is about to trigger a split
   */
  public void preCompact(boolean willSplit) {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).preCompact(env, willSplit);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * Invoked after a region is compacted.
   * @param willSplit true if the compaction is about to trigger a split
   */
  public void postCompact(boolean willSplit) {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).postCompact(env, willSplit);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * Invoked before a memstore flush
   */
  public void preFlush() {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).preFlush(env);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * Invoked after a memstore flush
   */
  public void postFlush() {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).postFlush(env);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * Invoked just before a split
   */
  public void preSplit() {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).preSplit(env);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * Invoked just after a split
   * @param l the new left-hand daughter region
   * @param r the new right-hand daughter region
   */
  public void postSplit(HRegion l, HRegion r) {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).postSplit(env, l, r);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  // RegionObserver support

  /**
   * @param row the row key
   * @param family the family
   * @exception IOException Exception
   */
  public void preGetClosestRowBefore(final byte[] row, final byte[] family)
  throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).preGetClosestRowBefore(env, row, family);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param row the row key
   * @param family the family
   * @param result the result set from the region
   * @return the result set to return to the client
   * @exception IOException Exception
   */
  public Result postGetClosestRowBefore(final byte[] row, final byte[] family,
      Result result) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          result = ((RegionObserver)env.getInstance())
            .postGetClosestRowBefore(env, row, family, result);
        }
      }
      return result;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param get the Get request
   * @return the possibly transformed Get object by coprocessor
   * @exception IOException Exception
   */
  public Get preGet(Get get) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          get = ((RegionObserver)env.getInstance()).preGet(env, get);
        }
      }
      return get;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param get the Get request
   * @param results the result set
   * @return the possibly transformed result set to use
   * @exception IOException Exception
   */
  public List<KeyValue> postGet(final Get get, List<KeyValue> results)
  throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          results = ((RegionObserver)env.getInstance()).postGet(env, get, results);
        }
      }
      return results;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param get the Get request
   * @exception IOException Exception
   */
  public Get preExists(Get get) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          get = ((RegionObserver)env.getInstance()).preExists(env, get);
        }
      }
      return get;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param get the Get request
   * @param exists the result returned by the region server
   * @return the result to return to the client
   * @exception IOException Exception
   */
  public boolean postExists(final Get get, boolean exists)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          exists &= ((RegionObserver)env.getInstance()).postExists(env, get, exists);
        }
      }
      return exists;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param familyMap map of family to edits for the given family.
   * @return the possibly transformed map to actually use
   * @exception IOException Exception
   */
  public Map<byte[], List<KeyValue>> prePut(Map<byte[], List<KeyValue>> familyMap)
  throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          familyMap = ((RegionObserver)env.getInstance()).prePut(env, familyMap);
        }
      }
      return familyMap;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param familyMap map of family to edits for the given family.
   * @exception IOException Exception
   */
  public void postPut(Map<byte[], List<KeyValue>> familyMap)
  throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).postPut(env, familyMap);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param familyMap map of family to edits for the given family.
   * @return the possibly transformed map to actually use
   * @exception IOException Exception
   */
  public Map<byte[], List<KeyValue>> preDelete(Map<byte[], List<KeyValue>> familyMap)
  throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          familyMap = ((RegionObserver)env.getInstance()).preDelete(env, familyMap);
        }
      }
      return familyMap;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param familyMap map of family to edits for the given family.
   * @exception IOException Exception
   */
  public void postDelete(Map<byte[], List<KeyValue>> familyMap)
  throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).postDelete(env, familyMap);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param put data to put if check succeeds
   * @throws IOException e
   */
  public Put preCheckAndPut(final byte [] row, final byte [] family,
      final byte [] qualifier, final byte [] value, Put put)
    throws IOException
  {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          put = ((RegionObserver)env.getInstance()).preCheckAndPut(env, row, family,
            qualifier, value, put);
        }
      }
      return put;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param put data to put if check succeeds
   * @throws IOException e
   */
  public boolean postCheckAndPut(final byte [] row, final byte [] family,
      final byte [] qualifier, final byte [] value, final Put put,
      boolean result)
    throws IOException
  {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          result = ((RegionObserver)env.getInstance()).postCheckAndPut(env, row,
            family, qualifier, value, put, result);
        }
      }
      return result;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param delete delete to commit if check succeeds
   * @throws IOException e
   */
  public Delete preCheckAndDelete(final byte [] row, final byte [] family,
      final byte [] qualifier, final byte [] value, Delete delete)
    throws IOException
  {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          delete = ((RegionObserver)env.getInstance()).preCheckAndDelete(env, row,
              family, qualifier, value, delete);
        }
      }
      return delete;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param delete delete to commit if check succeeds
   * @throws IOException e
   */
  public boolean postCheckAndDelete(final byte [] row, final byte [] family,
      final byte [] qualifier, final byte [] value, final Delete delete,
      boolean result)
    throws IOException
  {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          result = ((RegionObserver)env.getInstance()).postCheckAndDelete(env, row,
            family, qualifier, value, delete, result);
        }
      }
      return result;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL whether to write the increment to the WAL
   * @return new amount to increment
   * @throws IOException if an error occurred on the coprocessor
   */
  public long preIncrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, long amount, final boolean writeToWAL)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          amount = ((RegionObserver)env.getInstance()).preIncrementColumnValue(env,
              row, family, qualifier, amount, writeToWAL);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return amount;
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL whether to write the increment to the WAL
   * @param result the result returned by incrementColumnValue
   * @return the result to return to the client
   * @throws IOException if an error occurred on the coprocessor
   */
  public long postIncrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL,
      long result) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          result = ((RegionObserver)env.getInstance()).postIncrementColumnValue(env,
              row, family, qualifier, amount, writeToWAL, result);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return result;
  }

  /**
   * @param increment increment object
   * @return new amount to increment
   * @throws IOException if an error occurred on the coprocessor
   */
  public Increment preIncrement(Increment increment)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          increment = ((RegionObserver)env.getInstance()).preIncrement(env, increment);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return increment;
  }

  /**
   * @param increment increment object
   * @param result the result returned by incrementColumnValue
   * @return the result to return to the client
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result postIncrement(final Increment increment, Result result)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          result = ((RegionObserver)env.getInstance()).postIncrement(env, increment,
              result);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return result;
  }

  /**
   * @param scan the Scan specification
   * @exception IOException Exception
   */
  public Scan preScannerOpen(Scan scan) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          scan = ((RegionObserver)env.getInstance()).preScannerOpen(env, scan);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return scan;
  }

  /**
   * @param scan the Scan specification
   * @param scannerId the scanner id allocated by the region server
   * @exception IOException Exception
   */
  public void postScannerOpen(final Scan scan, long scannerId)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).postScannerOpen(env, scan, scannerId);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param scannerId the scanner id
   * @return the possibly transformed result set to actually return
   * @exception IOException Exception
   */
  public void preScannerNext(final long scannerId) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).preScannerNext(env, scannerId);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param scannerId the scanner id
   * @param results the result set returned by the region server
   * @return the possibly transformed result set to actually return
   * @exception IOException Exception
   */
  public List<KeyValue> postScannerNext(final long scannerId,
      List<KeyValue> results) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          results = ((RegionObserver)env.getInstance()).postScannerNext(env, scannerId,
            results);
        }
      }
      return results;
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param scannerId the scanner id
   * @exception IOException Exception
   */
  public void preScannerClose(final long scannerId)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).preScannerClose(env, scannerId);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  /**
   * @param scannerId the scanner id
   * @exception IOException Exception
   */
  public void postScannerClose(final long scannerId)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (RegionCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof RegionObserver) {
          ((RegionObserver)env.getInstance()).postScannerClose(env, scannerId);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }
}
