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

package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.*;

import java.io.IOException;

/**
 * Defines coprocessor hooks for interacting with operations on the
 * {@link org.apache.hadoop.hbase.master.HMaster} process.
 */
public interface MasterObserver extends Coprocessor {
  HTableDescriptor preCreateTable(MasterCoprocessorEnvironment env,
      HTableDescriptor desc, byte[][] splitKeys) throws IOException;
  void postCreateTable(MasterCoprocessorEnvironment env,
      HRegionInfo[] regions, boolean sync) throws IOException;

  void preDeleteTable(MasterCoprocessorEnvironment env, byte[] tableName)
      throws IOException;
  void postDeleteTable(MasterCoprocessorEnvironment env, byte[] tableName)
      throws IOException;

  HTableDescriptor preModifyTable(MasterCoprocessorEnvironment env, final byte[] tableName, HTableDescriptor htd)
      throws IOException;
  void postModifyTable(MasterCoprocessorEnvironment env, final byte[] tableName, HTableDescriptor htd)
      throws IOException;

  HColumnDescriptor preAddColumn(MasterCoprocessorEnvironment env, byte [] tableName, HColumnDescriptor column)
      throws IOException;
  void postAddColumn(MasterCoprocessorEnvironment env, byte [] tableName, HColumnDescriptor column)
      throws IOException;

  HColumnDescriptor preModifyColumn(MasterCoprocessorEnvironment env, byte [] tableName, HColumnDescriptor descriptor)
      throws IOException;
  void postModifyColumn(MasterCoprocessorEnvironment env, byte [] tableName, HColumnDescriptor descriptor)
      throws IOException;

  void preDeleteColumn(MasterCoprocessorEnvironment env, final byte [] tableName, final byte [] c)
      throws IOException;
  void postDeleteColumn(MasterCoprocessorEnvironment env, final byte [] tableName, final byte [] c)
      throws IOException;

  void preEnableTable(MasterCoprocessorEnvironment env, final byte [] tableName) throws IOException;
  void postEnableTable(MasterCoprocessorEnvironment env, final byte [] tableName) throws IOException;

  void preDisableTable(MasterCoprocessorEnvironment env, final byte [] tableName) throws IOException;
  void postDisableTable(MasterCoprocessorEnvironment env, final byte [] tableName) throws IOException;

  void preMove(MasterCoprocessorEnvironment env, final HRegionInfo region, final HServerInfo srcServer, final HServerInfo destServer)
      throws UnknownRegionException;
  void postMove(MasterCoprocessorEnvironment env, final HRegionInfo region, final HServerInfo srcServer, final HServerInfo destServer)
      throws UnknownRegionException;

  void preBalance(MasterCoprocessorEnvironment env) throws IOException;
  void postBalance(MasterCoprocessorEnvironment env) throws IOException;

  boolean preBalanceSwitch(MasterCoprocessorEnvironment env, final boolean b) throws IOException;
  void postBalanceSwitch(MasterCoprocessorEnvironment env, final boolean b) throws IOException;

  void preShutdown(MasterCoprocessorEnvironment env) throws IOException;

  void preStopMaster(MasterCoprocessorEnvironment env) throws IOException;
}
