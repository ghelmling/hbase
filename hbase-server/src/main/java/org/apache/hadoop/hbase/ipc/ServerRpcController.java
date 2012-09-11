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

package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Used for server-side protobuf RPC service invocations.  This handlers allows
 * invocation exceptions to easily be passed through to the RPC server.
 */
public class ServerRpcController implements RpcController {
  /**
   * The exception thrown within
   * {@link Service#callMethod(Descriptors.MethodDescriptor, RpcController, Message, RpcCallback)},
   * if any.
   */
  // TODO: it would be good widen this to just Throwable, but IOException is what we allow now
  private IOException serviceException;
  private String errorMessage;

  @Override
  public void reset() {
    serviceException = null;
    errorMessage = null;
  }

  @Override
  public boolean failed() {
    return (failedOnException() || errorMessage != null);
  }

  @Override
  public String errorText() {
    return errorMessage;
  }

  @Override
  public void startCancel() {
    // not implemented
  }

  @Override
  public void setFailed(String message) {
    errorMessage = message;
  }

  @Override
  public boolean isCanceled() {
    return false;
  }

  @Override
  public void notifyOnCancel(RpcCallback<Object> objectRpcCallback) {
    // not implemented
  }

  public void setFailedOn(IOException ioe) {
    serviceException = ioe;
    setFailed(StringUtils.stringifyException(ioe));
  }

  public IOException getFailedOn() {
    return serviceException;
  }

  public boolean failedOnException() {
    return serviceException != null;
  }
}
