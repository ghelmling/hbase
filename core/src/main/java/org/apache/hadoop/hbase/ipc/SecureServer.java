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

package org.apache.hadoop.hbase.ipc;

import com.google.common.base.Function;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.AuthMethod;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslDigestCallbackHandler;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslGssCallbackHandler;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslStatus;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSecretManager;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION;

/** An abstract IPC service.  IPC calls take a single {@link org.apache.hadoop.io.Writable} as a
 * parameter, and return a {@link org.apache.hadoop.io.Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 *
 *
 * <p>Copied local so can fix HBASE-900.
 *
 * @see org.apache.hadoop.hbase.ipc.HBaseClient
 */
public abstract class SecureServer extends HBaseServer {
  private final boolean authorize;
  private boolean isSecurityEnabled;

  // 1 : Introduce ping and server does not throw away RPCs
  // 3 : Introduce the protocol into the RPC connection header
  // 4 : Introduced SASL security layer
  public static final byte CURRENT_VERSION = 4;

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.ipc.SecureServer");
  private static final Log AUDITLOG =
    LogFactory.getLog("SecurityLogger.org.apache.hadoop.ipc.HBaseServer");
  private static final String AUTH_FAILED_FOR = "Auth failed for ";
  private static final String AUTH_SUCCESSFULL_FOR = "Auth successfull for ";

  protected SecretManager<TokenIdentifier> secretManager;
  protected ServiceAuthorizationManager authManager;

  /** Reads calls from a connection and queues them for handling. */
  public class SecureConnection extends HBaseServer.Connection  {
    private boolean rpcHeaderRead = false; // if initial rpc header is read
    private boolean headerRead = false;  //if the connection header that
                                         //follows version is read.
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    protected final LinkedList<Call> responseQueue;
    private volatile int rpcCount = 0; // number of outstanding rpcs
    private long lastContact;
    private int dataLength;
    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    private String hostAddress;
    private String hostName;
    private int remotePort;

    boolean useSasl;
    SaslServer saslServer;
    private AuthMethod authMethod;
    private boolean saslContextEstablished;
    private boolean skipInitialSaslHandshake;
    private ByteBuffer rpcHeaderBuffer;
    private ByteBuffer unwrappedData;
    private ByteBuffer unwrappedDataLengthBuffer;

    public UserGroupInformation attemptingUser = null; // user name before auth

    // Fake 'call' for failed authorization response
    private final int AUTHORIZATION_FAILED_CALLID = -1;
    private final Call authFailedCall =
      new Call(AUTHORIZATION_FAILED_CALLID, null, this);
    private ByteBufferOutputStream authFailedResponse =
        new ByteBufferOutputStream(0);
    // Fake 'call' for SASL context setup
    private static final int SASL_CALLID = -33;
    private final Call saslCall = new Call(SASL_CALLID, null, this);
    private final ByteArrayOutputStream saslResponse = new ByteArrayOutputStream();

    private boolean useWrap = false;

    public SecureConnection(SocketChannel channel, long lastContact) {
      super(channel, lastContact);
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.unwrappedData = null;
      this.unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      InetAddress addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
        this.hostName = addr.getCanonicalHostName();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new LinkedList<Call>();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }

    @Override
    public String toString() {
      return getHostAddress() + ":" + remotePort;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public String getHostName() {
      return hostName;
    }

    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount == 0;
    }

    /* Decrement the outstanding RPC count */
    protected void decRpcCount() {
      rpcCount--;
    }

    /* Increment the outstanding RPC count */
    private void incRpcCount() {
      rpcCount++;
    }

    protected boolean timedOut(long currentTime) {
      return isIdle() && currentTime - lastContact > maxIdleTime;
    }

    private UserGroupInformation getAuthorizedUgi(String authorizedId)
        throws IOException {
      if (authMethod == AuthMethod.DIGEST) {
        TokenIdentifier tokenId = HBaseSaslRpcServer.getIdentifier(authorizedId,
            secretManager);
        UserGroupInformation ugi = tokenId.getUser();
        if (ugi == null) {
          throw new AccessControlException(
              "Can't retrieve username from tokenIdentifier.");
        }
        ugi.addTokenIdentifier(tokenId);
        return ugi;
      } else {
        return UserGroupInformation.createRemoteUser(authorizedId);
      }
    }

    private void saslReadAndProcess(byte[] saslToken) throws IOException,
        InterruptedException {
      if (!saslContextEstablished) {
        byte[] replyToken = null;
        try {
          if (saslServer == null) {
            switch (authMethod) {
            case DIGEST:
              if (secretManager == null) {
                throw new AccessControlException(
                    "Server is not configured to do DIGEST authentication.");
              }
              saslServer = Sasl.createSaslServer(AuthMethod.DIGEST
                  .getMechanismName(), null, HBaseSaslRpcServer.SASL_DEFAULT_REALM,
                  HBaseSaslRpcServer.SASL_PROPS, new SaslDigestCallbackHandler(
                      secretManager, this));
              break;
            default:
              UserGroupInformation current = UserGroupInformation
                  .getCurrentUser();
              String fullName = current.getUserName();
              if (LOG.isDebugEnabled())
                LOG.debug("Kerberos principal name is " + fullName);
              final String names[] = HBaseSaslRpcServer.splitKerberosName(fullName);
              if (names.length != 3) {
                throw new AccessControlException(
                    "Kerberos principal name does NOT have the expected "
                        + "hostname part: " + fullName);
              }
              current.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws SaslException {
                  saslServer = Sasl.createSaslServer(AuthMethod.KERBEROS
                      .getMechanismName(), names[0], names[1],
                      HBaseSaslRpcServer.SASL_PROPS, new SaslGssCallbackHandler());
                  return null;
                }
              });
            }
            if (saslServer == null)
              throw new AccessControlException(
                  "Unable to find SASL server implementation for "
                      + authMethod.getMechanismName());
            if (LOG.isDebugEnabled())
              LOG.debug("Created SASL server with mechanism = "
                  + authMethod.getMechanismName());
          }
          if (LOG.isDebugEnabled())
            LOG.debug("Have read input token of size " + saslToken.length
                + " for processing by saslServer.evaluateResponse()");
          replyToken = saslServer.evaluateResponse(saslToken);
        } catch (IOException e) {
          IOException sendToClient = e;
          Throwable cause = e;
          while (cause != null) {
            if (cause instanceof InvalidToken) {
              sendToClient = (InvalidToken) cause;
              break;
            }
            cause = cause.getCause();
          }
          doSaslReply(SaslStatus.ERROR, null, sendToClient.getClass().getName(),
              sendToClient.getLocalizedMessage());
          rpcMetrics.authenticationFailures.inc();
          String clientIP = this.toString();
          // attempting user could be null
          AUDITLOG.warn(AUTH_FAILED_FOR + clientIP + ":" + attemptingUser);
          throw e;
        }
        if (replyToken != null) {
          if (LOG.isDebugEnabled())
            LOG.debug("Will send token of size " + replyToken.length
                + " from saslServer.");
          doSaslReply(SaslStatus.SUCCESS, new BytesWritable(replyToken), null,
              null);
        }
        if (saslServer.isComplete()) {
          LOG.info("SASL server context established. Negotiated QoP is "
              + saslServer.getNegotiatedProperty(Sasl.QOP));
          String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
          useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
          ticket = getAuthorizedUgi(saslServer.getAuthorizationID());
          LOG.info("SASL server successfully authenticated client: " + ticket);
          rpcMetrics.authenticationSuccesses.inc();
          AUDITLOG.info(AUTH_SUCCESSFULL_FOR + ticket);
          saslContextEstablished = true;
        }
      } else {
        if (LOG.isDebugEnabled())
          LOG.debug("Have read input token of size " + saslToken.length
              + " for processing by saslServer.unwrap()");

        if (!useWrap) {
          processOneRpc(saslToken);
        } else {
          byte[] plaintextData = saslServer.unwrap(saslToken, 0,
              saslToken.length);
          processUnwrappedData(plaintextData);
        }
      }
    }

    private void doSaslReply(SaslStatus status, Writable rv,
        String errorClass, String error) throws IOException {
      saslResponse.reset();
      DataOutputStream out = new DataOutputStream(saslResponse);
      out.writeInt(status.state); // write status
      if (status == SaslStatus.SUCCESS) {
        rv.write(out);
      } else {
        WritableUtils.writeString(out, errorClass);
        WritableUtils.writeString(out, error);
      }
      saslCall.setResponse(ByteBuffer.wrap(saslResponse.toByteArray()));
      responder.doRespond(saslCall);
    }

    private void disposeSasl() {
      if (saslServer != null) {
        try {
          saslServer.dispose();
        } catch (SaslException ignored) {
        }
      }
    }

    public int readAndProcess() throws IOException, InterruptedException {
      while (true) {
        /* Read at most one RPC. If the header is not read completely yet
         * then iterate until we read first RPC or until there is no data left.
         */
        int count = -1;
        if (dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, dataLengthBuffer);
          if (count < 0 || dataLengthBuffer.remaining() > 0)
            return count;
        }

        if (!rpcHeaderRead) {
          //Every connection is expected to send the header.
          if (rpcHeaderBuffer == null) {
            rpcHeaderBuffer = ByteBuffer.allocate(2);
          }
          count = channelRead(channel, rpcHeaderBuffer);
          if (count < 0 || rpcHeaderBuffer.remaining() > 0) {
            return count;
          }
          int version = rpcHeaderBuffer.get(0);
          byte[] method = new byte[] {rpcHeaderBuffer.get(1)};
          authMethod = AuthMethod.read(new DataInputStream(
              new ByteArrayInputStream(method)));
          dataLengthBuffer.flip();
          if (!HEADER.equals(dataLengthBuffer) || version != CURRENT_VERSION) {
            //Warning is ok since this is not supposed to happen.
            LOG.warn("Incorrect header or version mismatch from " +
                     hostAddress + ":" + remotePort +
                     " got version " + version +
                     " expected version " + CURRENT_VERSION);
            return -1;
          }
          dataLengthBuffer.clear();
          if (authMethod == null) {
            throw new IOException("Unable to read authentication method");
          }
          if (isSecurityEnabled && authMethod == AuthMethod.SIMPLE) {
            AccessControlException ae = new AccessControlException(
                "Authentication is required");
            doResponse(authFailedResponse, authFailedCall, null,
                ae.getClass().getName(), ae.getMessage(), "SecureConnection",
                Status.FATAL);
            throw ae;
          }
          if (!isSecurityEnabled && authMethod != AuthMethod.SIMPLE) {
            doSaslReply(SaslStatus.SUCCESS, new IntWritable(
                HBaseSaslRpcServer.SWITCH_TO_SIMPLE_AUTH), null, null);
            authMethod = AuthMethod.SIMPLE;
            // client has already sent the initial Sasl message and we
            // should ignore it. Both client and server should fall back
            // to simple auth from now on.
            skipInitialSaslHandshake = true;
          }
          if (authMethod != AuthMethod.SIMPLE) {
            useSasl = true;
          }

          rpcHeaderBuffer = null;
          rpcHeaderRead = true;
          continue;
        }

        if (data == null) {
          dataLengthBuffer.flip();
          dataLength = dataLengthBuffer.getInt();

          if (dataLength == HBaseClient.PING_CALL_ID) {
            if(!useWrap) { //covers the !useSasl too
              dataLengthBuffer.clear();
              return 0;  //ping message
            }
          }
          if (dataLength < 0) {
            LOG.warn("Unexpected data length " + dataLength + "!! from " +
                getHostAddress());
          }
          data = ByteBuffer.allocate(dataLength);
          incRpcCount();  // Increment the rpc count
        }

        count = channelRead(channel, data);

        if (data.remaining() == 0) {
          dataLengthBuffer.clear();
          data.flip();
          if (skipInitialSaslHandshake) {
            data = null;
            skipInitialSaslHandshake = false;
            continue;
          }
          boolean isHeaderRead = headerRead;
          if (useSasl) {
            saslReadAndProcess(data.array());
          } else {
            processOneRpc(data.array());
          }
          data = null;
          if (!isHeaderRead) {
            continue;
          }
        }
        return count;
      }
    }

    /// Reads the connection header following version
    private void processHeader(byte[] buf) throws IOException {
      DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(buf));
      header.readFields(in);
      try {
        String protocolClassName = header.getProtocol();
        if (protocolClassName != null) {
          protocol = getProtocolClass(header.getProtocol(), conf);
        }
      } catch (ClassNotFoundException cnfe) {
        throw new IOException("Unknown protocol: " + header.getProtocol());
      }

      UserGroupInformation protocolUser = header.getUgi();
      if (!useSasl) {
        ticket = protocolUser;
        if (ticket != null) {
          ticket.setAuthenticationMethod(AuthMethod.SIMPLE.authenticationMethod);
        }
      } else {
        // user is authenticated
        ticket.setAuthenticationMethod(authMethod.authenticationMethod);
        //Now we check if this is a proxy user case. If the protocol user is
        //different from the 'user', it is a proxy user scenario. However,
        //this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(ticket.getUserName()))) {
          if (authMethod == AuthMethod.DIGEST) {
            // Not allowed to doAs if token authentication is used
            throw new AccessControlException("Authenticated user (" + ticket
                + ") doesn't match what the client claims to be ("
                + protocolUser + ")");
          } else {
            // Effective user can be different from authenticated user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            UserGroupInformation realUser = ticket;
            ticket = UserGroupInformation.createProxyUser(protocolUser
                .getUserName(), realUser);
            // Now the user is a proxy user, set Authentication method Proxy.
            ticket.setAuthenticationMethod(AuthenticationMethod.PROXY);
          }
        }
      }
    }

    private void processUnwrappedData(byte[] inBuf) throws IOException,
        InterruptedException {
      ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(
          inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (true) {
        int count = -1;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
            return;
        }

        if (unwrappedData == null) {
          unwrappedDataLengthBuffer.flip();
          int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();

          if (unwrappedDataLength == HBaseClient.PING_CALL_ID) {
            if (LOG.isDebugEnabled())
              LOG.debug("Received ping message");
            unwrappedDataLengthBuffer.clear();
            continue; // ping message
          }
          unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
        }

        count = channelRead(ch, unwrappedData);
        if (count <= 0 || unwrappedData.remaining() > 0)
          return;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          processOneRpc(unwrappedData.array());
          unwrappedData = null;
        }
      }
    }

    private void processOneRpc(byte[] buf) throws IOException,
        InterruptedException {
      if (headerRead) {
        processData(buf);
      } else {
        processHeader(buf);
        headerRead = true;
        if (!authorizeConnection()) {
          throw new AccessControlException("Connection from " + this
              + " for protocol " + header.getProtocol()
              + " is unauthorized for user " + ticket);
        }
      }
    }


    private boolean authorizeConnection() throws IOException {
      try {
        // If auth method is DIGEST, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (ticket != null && ticket.getRealUser() != null
            && (authMethod != AuthMethod.DIGEST)) {
          ProxyUsers.authorize(ticket, this.getHostAddress(), conf);
        }
        authorize(ticket, header, getHostName());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully authorized " + header);
        }
        rpcMetrics.authorizationSuccesses.inc();
      } catch (AuthorizationException ae) {
        LOG.debug("Connection authorization failed: "+ae.getMessage(), ae);
        rpcMetrics.authorizationFailures.inc();
        doResponse(authFailedResponse, authFailedCall, null,
            ae.getClass().getName(), ae.getMessage(), "SecureConnection",
            Status.FATAL);
        return false;
      }
      return true;
    }

    protected synchronized void close() {
      disposeSasl();
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception ignored) {} // FindBugs DE_MIGHT_IGNORE
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception ignored) {}
      }
      try {socket.close();} catch(Exception ignored) {}
    }
  }

  /**
   * Gets the QOS level for this call.  If it is higher than the highPriorityLevel and there
   * are priorityHandlers available it will be processed in it's own thread set.
   *
   * @param param
   * @return priority, higher is better
   */
  private Function<Writable,Integer> qosFunction = null;
  @Override
  public void setQosFunction(Function<Writable, Integer> newFunc) {
    qosFunction = newFunc;
  }

  protected int getQosLevel(Writable param) {
    if (qosFunction == null) {
      return 0;
    }

    Integer res = qosFunction.apply(param);
    if (res == null) {
      return 0;
    }
    return res;
  }

  /** Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   *
   */
  @SuppressWarnings("unchecked")
  protected SecureServer(String bindAddress, int port,
                  Class<? extends Writable> paramClass, int handlerCount,
                  int priorityHandlerCount, Configuration conf, String serverName,
                  int highPriorityLevel)
    throws IOException {
    super(bindAddress, port, paramClass, handlerCount, priorityHandlerCount, conf, serverName, highPriorityLevel);
    this.authorize =
      conf.getBoolean(HADOOP_SECURITY_AUTHORIZATION, false);
    this.isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
    LOG.debug("security enabled="+isSecurityEnabled);

    if (isSecurityEnabled) {
      HBaseSaslRpcServer.init(conf);
    }
  }

  @Override
  protected Connection getConnection(SocketChannel channel, long time) {
    return new SecureConnection(channel, time);
  }

  @Override
  protected void doResponse(ByteBufferOutputStream response, Call call,
      Writable rv, String errorClass, String error, String handler)
  throws IOException {
    doResponse(response, call, rv, errorClass, error, handler,
        error == null ? Status.SUCCESS : Status.ERROR);
  }

  /**
   * Setup response for the IPC Call.
   *
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @param status {@link org.apache.hadoop.hbase.ipc.Status} of the IPC call
   * @throws java.io.IOException
   */
  protected void doResponse(ByteBufferOutputStream response, Call call,
      Writable rv, String errorClass, String error, String handler, Status status)
  throws IOException {
    synchronized (call.connection.responseQueue) {
      // setupResponse() needs to be sync'ed together with
      // responder.doResponse() since setupResponse may use
      // SASL to encrypt response data and SASL enforces
      // its own message ordering.
      DataOutputStream out = new DataOutputStream(response);
      out.writeInt(call.id);                // write call id
      out.writeInt(status.state);           // write status

      if (status == Status.SUCCESS) {
        rv.write(out);
      } else {
        WritableUtils.writeString(out, errorClass);
        WritableUtils.writeString(out, error);
      }
      if (((SecureConnection)call.connection).useWrap) {
        wrapWithSasl(response, call);
      }
      if (response.size() > warnResponseSize) {
        LOG.warn(handler+", responseTooLarge for: "+call+": Size: "
                 + StringUtils.humanReadableInt(response.size()));
      }
      call.setResponse(response.getByteBuffer());
      responder.doRespond(call);
    }
  }

  private void wrapWithSasl(ByteBufferOutputStream response, Call call)
      throws IOException {
    if (((SecureConnection)call.connection).useSasl) {
      byte[] token = response.getByteBuffer().array();
      // synchronization may be needed since there can be multiple Handler
      // threads using saslServer to wrap responses.
      synchronized (((SecureConnection)call.connection).saslServer) {
        token = ((SecureConnection)call.connection).saslServer.wrap(token, 0, token.length);
      }
      if (LOG.isDebugEnabled())
        LOG.debug("Adding saslServer wrapped token of size " + token.length
            + " as call response.");
      response.getByteBuffer().reset();
      DataOutputStream saslOut = new DataOutputStream(response);
      saslOut.writeInt(token.length);
      saslOut.write(token, 0, token.length);
    }
  }

  Configuration getConf() {
    return conf;
  }

  /** for unit testing only, should be called before server is started */
  void disableSecurity() {
    this.isSecurityEnabled = false;
  }

  /** for unit testing only, should be called before server is started */
  void enableSecurity() {
    this.isSecurityEnabled = true;
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop() {
    super.stop();
  }

  public SecretManager<? extends TokenIdentifier> getSecretManager() {
    return this.secretManager;
  }

  public void setSecretManager(SecretManager<? extends TokenIdentifier> secretManager) {
    this.secretManager = (SecretManager<TokenIdentifier>) secretManager;    
  }

  /**
   * Called for each call.
   * @deprecated Use {@link #call(Class, org.apache.hadoop.io.Writable, long)} instead
   */
  @Deprecated
  public Writable call(Writable param, long receiveTime) throws IOException {
    return call(null, param, receiveTime);
  }

  /**
   * Authorize the incoming client connection.
   *
   * @param user client user
   * @param connection incoming connection
   * @param hostname fully-qualified domain name of incoming connection
   * @throws org.apache.hadoop.security.authorize.AuthorizationException when the client isn't authorized to talk the protocol
   */
  public void authorize(UserGroupInformation user,
                        ConnectionHeader connection,
                        String hostname
                        ) throws AuthorizationException {
    if (authorize) {
      Class<?> protocol = null;
      try {
        protocol = getProtocolClass(connection.getProtocol(), getConf());
      } catch (ClassNotFoundException cfne) {
        throw new AuthorizationException("Unknown protocol: " +
                                         connection.getProtocol());
      }
      authManager.authorize(user, protocol, getConf(), hostname);
    }
  }
}