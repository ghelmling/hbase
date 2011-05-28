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

package org.apache.hadoop.hbase.security.rbac;

import com.google.common.collect.Multimap;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.Map;
import java.util.List;

/**
 * Coprocessor Endpoint protocol defined for access control of security.
 */
public interface AccessControllerProtocol extends CoprocessorProtocol {
  public boolean grant(byte[] user, TablePermission permission)
      throws IOException;

  public boolean revoke(byte[] user, TablePermission permission)
      throws IOException;

//  public Map<String, List<TablePermission>> getTablePermissions(byte[] table)
//      throws IOException;

  public List<UserPermission> getUserPermissions(byte[] tableName)
      throws IOException;

  public Token<AuthenticationTokenIdentifier> getAuthenticationToken()
      throws IOException;

  public String whoami();
}
