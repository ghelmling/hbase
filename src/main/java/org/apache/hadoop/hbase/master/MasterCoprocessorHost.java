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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.*;

import java.io.IOException;

public class MasterCoprocessorHost
    extends CoprocessorHost<MasterCoprocessorEnvironment> {

  private static class MasterEnvironment extends CoprocessorHost.Environment
      implements MasterCoprocessorEnvironment {
    private MasterServices masterServices;

    public MasterEnvironment(Class<?> implClass, Coprocessor impl,
        Coprocessor.Priority priority, MasterServices services) {
      super(impl, priority);
      this.masterServices = services;
    }

    public MasterServices getMasterServices() {
      return masterServices;
    }
  }

  private MasterServices masterServices;

  MasterCoprocessorHost(final MasterServices services, final Configuration conf) {
    this.masterServices = services;

    loadSystemCoprocessors(conf, MASTER_COPROCESSOR_CONF_KEY);
  }

  @Override
  public MasterCoprocessorEnvironment createEnvironment(Class<?> implClass,
      Coprocessor instance, Coprocessor.Priority priority) {
    return new MasterEnvironment(implClass, instance, priority, masterServices);
  }

  /* Implementation of hooks for invoking MasterObservers */
  HTableDescriptor preCreateTable(HTableDescriptor desc, byte[][] splitKeys)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          desc = ((MasterObserver)env.getInstance()).preCreateTable(env, desc, splitKeys);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }

    return desc;
  }

  void postCreateTable(HRegionInfo[] regions, boolean sync) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postCreateTable(env, regions, sync);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preDeleteTable(byte[] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preDeleteTable(env, tableName);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postDeleteTable(byte[] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postDeleteTable(env, tableName);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  HTableDescriptor preModifyTable(final byte[] tableName, HTableDescriptor htd)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          htd = ((MasterObserver)env.getInstance()).preModifyTable(env, tableName, htd);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return htd;
  }

  void postModifyTable(final byte[] tableName, HTableDescriptor htd)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postModifyTable(env, tableName, htd);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  HColumnDescriptor preAddColumn(byte [] tableName, HColumnDescriptor column)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          column = ((MasterObserver)env.getInstance()).preAddColumn(env, tableName, column);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return column;
  }

  void postAddColumn(byte [] tableName, HColumnDescriptor column)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postAddColumn(env, tableName, column);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  HColumnDescriptor preModifyColumn(byte [] tableName, HColumnDescriptor descriptor)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          descriptor = ((MasterObserver)env.getInstance()).preModifyColumn(env, tableName, descriptor);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return descriptor;
  }

  void postModifyColumn(byte [] tableName, HColumnDescriptor descriptor)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postModifyColumn(env, tableName, descriptor);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preDeleteColumn(final byte [] tableName, final byte [] c)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preDeleteColumn(env, tableName, c);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postDeleteColumn(final byte [] tableName, final byte [] c)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postDeleteColumn(env, tableName, c);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preEnableTable(final byte [] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preEnableTable(env, tableName);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postEnableTable(final byte [] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postEnableTable(env, tableName);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preDisableTable(final byte [] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preDisableTable(env, tableName);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postDisableTable(final byte [] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postDisableTable(env, tableName);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preMove(final HRegionInfo region, final HServerInfo srcServer, final HServerInfo destServer)
      throws UnknownRegionException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preMove(
              env, region, srcServer, destServer);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postMove(final HRegionInfo region, final HServerInfo srcServer, final HServerInfo destServer)
      throws UnknownRegionException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postMove(
              env, region, srcServer, destServer);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preBalance() throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preBalance(env);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postBalance() throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postBalance(env);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  boolean preBalanceSwitch(final boolean b) throws IOException {
    boolean balance = b;
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          balance = ((MasterObserver)env.getInstance()).preBalanceSwitch(env, balance);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return balance;
  }

  void postBalanceSwitch(final boolean b) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postBalanceSwitch(env, b);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preShutdown() throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preShutdown(env);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preStopMaster() throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterCoprocessorEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preStopMaster(env);
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

}
