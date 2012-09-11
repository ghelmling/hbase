/*
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

import com.google.protobuf.RpcCallback;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class BlockingRpcCallback<R> implements RpcCallback<R> {
  private R result;
  private boolean resultSet = false;

  public void run(R parameter) {
    synchronized (this) {
      result = parameter;
      resultSet = true;
      this.notify();
    }
  }

  /**
   * Returns the parameter passed to {@link #run(Object)} or {@code null} if a null value was
   * passed.  When used asynchronously, this method will block until the {@link #run(Object)}
   * method has been called.
   * @return
   */
  public synchronized R get() throws IOException {
    while (!resultSet) {
      try {
        this.wait();
      } catch (InterruptedException ie) {
        // awful, but this will be used where we only expect IOException
        Thread.currentThread().interrupt();
        throw new IOException(ie);
      }
    }
    return result;
  }
}
