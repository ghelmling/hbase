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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.HBaseSaslRpcClient;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.AuthMethod;
import org.apache.hadoop.hbase.security.KerberosInfo;
import org.apache.hadoop.hbase.security.TokenInfo;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSelector;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.util.ReflectionUtils;

import javax.net.SocketFactory;
import java.io.*;
import java.net.*;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** A client for an IPC service.  IPC calls take a single {@link org.apache.hadoop.io.Writable} as a
 * parameter, and return a {@link org.apache.hadoop.io.Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 *
 * <p>This is the org.apache.hadoop.ipc.Client renamed as HBaseClient and
 * moved into this package so can access package-private methods.
 *
 * @see org.apache.hadoop.hbase.ipc.HBaseServer
 */
public class SecureClient extends HBaseClient {

  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.SecureClient");

  protected static Map<String,TokenSelector<? extends TokenIdentifier>> tokenHandlers =
      new HashMap<String,TokenSelector<? extends TokenIdentifier>>();
  static {
    tokenHandlers.put(AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE.toString(),
        new AuthenticationTokenSelector());
  }

  protected final Hashtable<ConnectionId, Connection> connections =
    new Hashtable<ConnectionId, Connection>();

  protected int counter;                            // counter for call ids
  protected final AtomicBoolean running = new AtomicBoolean(true); // if client runs

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends Thread {
    private InetSocketAddress server;             // server ip:port
    private String serverPrincipal;  // server's krb5 principal name
    private ConnectionHeader header;              // connection header
    private final ConnectionId remoteId;                // connection id
    private AuthMethod authMethod; // authentication method
    private boolean useSasl;
    private Token<? extends TokenIdentifier> token;
    private HBaseSaslRpcClient saslRpcClient;

    private Socket socket = null;                 // connected socket
    private DataInputStream in;
    private DataOutputStream out;

    // currently active calls
    private final Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
    private final AtomicLong lastActivity = new AtomicLong();// last I/O activity time
    protected final AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
    private IOException closeException; // close reason

    public Connection(ConnectionId remoteId) throws IOException {
      this.remoteId = remoteId;
      this.server = remoteId.getAddress();
      if (server.isUnresolved()) {
        throw new UnknownHostException("unknown host: " +
                                       remoteId.getAddress().getHostName());
      }

      UserGroupInformation ticket = remoteId.getTicket();
      Class<?> protocol = remoteId.getProtocol();
      this.useSasl = UserGroupInformation.isSecurityEnabled();
      if (useSasl && protocol != null) {
        TokenInfo tokenInfo = protocol.getAnnotation(TokenInfo.class);
        if (tokenInfo != null) {
          TokenSelector<? extends TokenIdentifier> tokenSelector =
              tokenHandlers.get(tokenInfo.value());
          if (tokenSelector != null) {
            InetSocketAddress addr = remoteId.getAddress();
            token = tokenSelector.selectToken(new Text(addr.getAddress()
                .getHostAddress() + ":" + addr.getPort()),
                ticket.getTokens());
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("No token selector found for type "+tokenInfo.value());
          }
        }
        KerberosInfo krbInfo = protocol.getAnnotation(KerberosInfo.class);
        if (krbInfo != null) {
          String serverKey = krbInfo.serverPrincipal();
          if (serverKey == null) {
            throw new IOException(
                "Can't obtain server Kerberos config key from KerberosInfo");
          }
          serverPrincipal = SecurityUtil.getServerPrincipal(
              conf.get(serverKey), server.getAddress().getCanonicalHostName().toLowerCase());
          if (LOG.isDebugEnabled()) {
            LOG.debug("RPC Server Kerberos principal name for protocol="
                + protocol.getCanonicalName() + " is " + serverPrincipal);
          }
        }
      }

      if (!useSasl) {
        authMethod = AuthMethod.SIMPLE;
      } else if (token != null) {
        authMethod = AuthMethod.DIGEST;
      } else {
        authMethod = AuthMethod.KERBEROS;
      }

      header = new ConnectionHeader(
          protocol == null ? null : protocol.getName(), ticket, authMethod);

      if (LOG.isDebugEnabled())
        LOG.debug("Use " + authMethod + " authentication for protocol "
            + protocol.getSimpleName());

      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
        remoteId.getAddress().toString() +
        ((ticket==null)?" from an unknown user": (" from " + ticket.getUserName())));
      this.setDaemon(true);
    }

    /** Update lastActivity with the current time. */
    private void touch() {
      lastActivity.set(System.currentTimeMillis());
    }

    /**
     * Add a call to this connection's call queue and notify
     * a listener; synchronized.
     * Returns false if called during shutdown.
     * @param call to add
     * @return true if the call was added.
     */
    protected synchronized boolean addCall(Call call) {
      if (shouldCloseConnection.get())
        return false;
      calls.put(call.id, call);
      notify();
      return true;
    }

    /** This class sends a ping to the remote side when timeout on
     * reading. If no failure is detected, it retries until at least
     * a byte is read.
     */
    private class PingInputStream extends FilterInputStream {
      /* constructor */
      protected PingInputStream(InputStream in) {
        super(in);
      }

      /* Process timeout exception
       * if the connection is not going to be closed, send a ping.
       * otherwise, throw the timeout exception.
       */
      private void handleTimeout(SocketTimeoutException e) throws IOException {
        if (shouldCloseConnection.get() || !running.get() ||
            remoteId.rpcTimeout > 0) {
          throw e;
        }
        sendPing();
      }

      /** Read a byte from the stream.
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * @throws java.io.IOException for any IO problem other than socket timeout
       */
      @Override
      public int read() throws IOException {
        do {
          try {
            return super.read();
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }

      /** Read bytes into a buffer starting from offset <code>off</code>
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       *
       * @return the total number of bytes read; -1 if the connection is closed.
       */
      @Override
      public int read(byte[] buf, int off, int len) throws IOException {
        do {
          try {
            return super.read(buf, off, len);
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }
    }

    private synchronized void disposeSasl() {
      if (saslRpcClient != null) {
        try {
          saslRpcClient.dispose();
        } catch (IOException ignored) {
        }
      }
    }

    private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      UserGroupInformation currentUser =
        UserGroupInformation.getCurrentUser();
      UserGroupInformation realUser = currentUser.getRealUser();
      if (authMethod == AuthMethod.KERBEROS &&
          loginUser != null &&
          //Make sure user logged in using Kerberos either keytab or TGT
          loginUser.hasKerberosCredentials() &&
          // relogin only in case it is the login user (e.g. JT)
          // or superuser (like oozie).
          (loginUser.equals(currentUser) || loginUser.equals(realUser))
          ) {
          return true;
      }
      return false;
    }

    private synchronized boolean setupSaslConnection(final InputStream in2,
        final OutputStream out2)
        throws IOException {
      saslRpcClient = new HBaseSaslRpcClient(authMethod, token, serverPrincipal);
      return saslRpcClient.saslConnect(in2, out2);
    }

    private synchronized void setupConnection() throws IOException {
      short ioFailures = 0;
      short timeoutFailures = 0;
      while (true) {
        try {
          this.socket = socketFactory.createSocket();
          this.socket.setTcpNoDelay(tcpNoDelay);
          this.socket.setKeepAlive(tcpKeepAlive);
          // connection time out is 20s
          NetUtils.connect(this.socket, remoteId.getAddress(), 20000);
          if (remoteId.rpcTimeout > 0) {
            pingInterval = remoteId.rpcTimeout; // overwrite pingInterval
          }
          this.socket.setSoTimeout(pingInterval);
          return;
        } catch (SocketTimeoutException toe) {
          /* The max number of retries is 45,
           * which amounts to 20s*45 = 15 minutes retries.
           */
          handleConnectionFailure(timeoutFailures++, maxRetries, toe);
        } catch (IOException ie) {
          handleConnectionFailure(ioFailures++, maxRetries, ie);
        }
      }
    }

    /**
     * If multiple clients with the same principal try to connect
     * to the same server at the same time, the server assumes a
     * replay attack is in progress. This is a feature of kerberos.
     * In order to work around this, what is done is that the client
     * backs off randomly and tries to initiate the connection
     * again.
     * The other problem is to do with ticket expiry. To handle that,
     * a relogin is attempted.
     */
    private synchronized void handleSaslConnectionFailure(
        final int currRetries,
        final int maxRetries, final Exception ex, final Random rand,
        final UserGroupInformation ugi)
    throws IOException, InterruptedException{
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        public Object run() throws IOException, InterruptedException {
          final short MAX_BACKOFF = 5000;
          closeConnection();
          if (shouldAuthenticateOverKrb()) {
            if (currRetries < maxRetries) {
              LOG.debug("Exception encountered while connecting to " +
                  "the server : " + ex);
              //try re-login
              if (UserGroupInformation.isLoginKeytabBased()) {
                UserGroupInformation.getLoginUser().reloginFromKeytab();
              } else {
                UserGroupInformation.getLoginUser().reloginFromTicketCache();
              }
              disposeSasl();
              //have granularity of milliseconds
              //we are sleeping with the Connection lock held but since this
              //connection instance is being used for connecting to the server
              //in question, it is okay
              Thread.sleep((rand.nextInt(MAX_BACKOFF) + 1));
              return null;
            } else {
              String msg = "Couldn't setup connection for " +
              UserGroupInformation.getLoginUser().getUserName() +
              " to " + serverPrincipal;
              LOG.warn(msg);
              throw (IOException) new IOException(msg).initCause(ex);
            }
          } else {
            LOG.warn("Exception encountered while connecting to " +
                "the server : " + ex);
          }
          if (ex instanceof RemoteException)
            throw (RemoteException)ex;
          throw new IOException(ex);
        }
      });
    }
    /** Connect to the server and set up the I/O streams. It then sends
     * a header to the server and starts
     * the connection thread that waits for responses.
     * @throws java.io.IOException e
     */
	  protected synchronized void setupIOstreams() throws IOException, InterruptedException {
      if (socket != null || shouldCloseConnection.get()) {
        return;
      }

      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to "+server);
        }
        short numRetries = 0;
        final short MAX_RETRIES = 5;
        Random rand = null;
        while (true) {
          setupConnection();
          InputStream inStream = NetUtils.getInputStream(socket);
          OutputStream outStream = NetUtils.getOutputStream(socket);
          writeRpcHeader(outStream);
          if (useSasl) {
            final InputStream in2 = inStream;
            final OutputStream out2 = outStream;
            UserGroupInformation ticket = remoteId.getTicket();
            if (authMethod == AuthMethod.KERBEROS) {
              if (ticket.getRealUser() != null) {
                ticket = ticket.getRealUser();
              }
            }
            boolean continueSasl = false;
            try {
              continueSasl =
                ticket.doAs(new PrivilegedExceptionAction<Boolean>() {
                  @Override
                  public Boolean run() throws IOException {
                    return setupSaslConnection(in2, out2);
                  }
                });
            } catch (Exception ex) {
              if (rand == null) {
                rand = new Random();
              }
              handleSaslConnectionFailure(numRetries++, MAX_RETRIES, ex, rand,
                   ticket);
              continue;
            }
            if (continueSasl) {
              // Sasl connect is successful. Let's set up Sasl i/o streams.
              inStream = saslRpcClient.getInputStream(inStream);
              outStream = saslRpcClient.getOutputStream(outStream);
            } else {
              // fall back to simple auth because server told us so.
              authMethod = AuthMethod.SIMPLE;
              header = new ConnectionHeader(header.getProtocol(),
                  header.getUgi(), authMethod);
              useSasl = false;
            }
          }
          this.in = new DataInputStream(new BufferedInputStream
              (new PingInputStream(inStream)));
          this.out = new DataOutputStream
          (new BufferedOutputStream(outStream));
          writeHeader();

          // update last activity time
          touch();

          // start the receiver thread after the socket connection has been set up
          start();
          return;
        }
      } catch (IOException e) {
        markClosed(e);
        close();

        throw e;
      }
    }

    private void closeConnection() {
      // close the current connection
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          LOG.warn("Not able to close a socket", e);
        }
	  }
      // set socket to null so that the next call to setupIOstreams
      // can start the process of connect all over again.
      socket = null;
    }

    /* Handle connection failures
     *
     * If the current number of retries is equal to the max number of retries,
     * stop retrying and throw the exception; Otherwise backoff N seconds and
     * try connecting again.
     *
     * This Method is only called from inside setupIOstreams(), which is
     * synchronized. Hence the sleep is synchronized; the locks will be retained.
     *
     * @param curRetries current number of retries
     * @param maxRetries max number of retries allowed
     * @param ioe failure reason
     * @throws IOException if max number of retries is reached
     */
    private void handleConnectionFailure(
        int curRetries, int maxRetries, IOException ioe) throws IOException {

      closeConnection();

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {
        throw ioe;
      }

      // otherwise back off and retry
      try {
        Thread.sleep(failureSleep);
      } catch (InterruptedException ignored) {}

      LOG.info("Retrying connect to server: " + server +
        " after sleeping " + failureSleep + "ms. Already tried " + curRetries +
        " time(s).");
    }

    /* Write the RPC header */
    private void writeRpcHeader(OutputStream outStream) throws IOException {
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream));
      // Write out the header, version and authentication method
      out.write(HBaseServer.HEADER.array());
      out.write(HBaseServer.CURRENT_VERSION);
      authMethod.write(out);
      out.flush();
    }

    /* Write the protocol header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    private void writeHeader() throws IOException {
      // Write out the ConnectionHeader
      DataOutputBuffer buf = new DataOutputBuffer();
      header.write(buf);

      // Write out the payload length
      int bufLen = buf.getLength();
      out.writeInt(bufLen);
      out.write(buf.getData(), 0, bufLen);
    }

    /* wait till someone signals us to start reading RPC response or
     * it is idle too long, it is marked as to be closed,
     * or the client is marked as not running.
     *
     * Return true if it is time to read a response; false otherwise.
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    private synchronized boolean waitForWork() {
      if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
        long timeout = maxIdleTime-
              (System.currentTimeMillis()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (InterruptedException ignored) {}
        }
      }

      if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
        return true;
      } else if (shouldCloseConnection.get()) {
        return false;
      } else if (calls.isEmpty()) { // idle connection closed or stopped
        markClosed(null);
        return false;
      } else { // get stopped but there are still pending requests
        markClosed((IOException)new IOException().initCause(
            new InterruptedException()));
        return false;
      }
    }

    public InetSocketAddress getRemoteAddress() {
      return server;
    }

    /* Send a ping to the server if the time elapsed
     * since last I/O activity is equal to or greater than the ping interval
     */
    protected synchronized void sendPing() throws IOException {
      long curTime = System.currentTimeMillis();
      if ( curTime - lastActivity.get() >= pingInterval) {
        lastActivity.set(curTime);
        //noinspection SynchronizeOnNonFinalField
        synchronized (this.out) {
          out.writeInt(PING_CALL_ID);
          out.flush();
        }
      }
    }

    @Override
    public void run() {
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": starting, having connections "
            + connections.size());

      try {
        while (waitForWork()) {//wait here for work - read or close connection
          receiveResponse();
        }
      } catch (Throwable t) {
        LOG.warn("Unexpected exception receiving call responses", t);
        markClosed(new IOException("Unexpected exception receiving call responses", t));
      }

      close();

      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": stopped, remaining connections "
            + connections.size());
    }

    /** Initiates a call by sending the parameter to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     */
    protected void sendParam(Call call) {
      if (shouldCloseConnection.get()) {
        return;
      }

      DataOutputBuffer d=null;
      try {
        //noinspection SynchronizeOnNonFinalField
        synchronized (this.out) { // FindBugs IS2_INCONSISTENT_SYNC
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + " sending #" + call.id);

          //for serializing the
          //data to be written
          d = new DataOutputBuffer();
          d.writeInt(call.id);
          call.param.write(d);
          byte[] data = d.getData();
          int dataLength = d.getLength();
          out.writeInt(dataLength);      //first put the data length
          out.write(data, 0, dataLength);//write the data
          out.flush();
        }
      } catch(IOException e) {
        markClosed(e);
      } finally {
        //the buffer is just an in-memory buffer, but it is still polite to
        // close early
        IOUtils.closeStream(d);
      }
    }

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    private void receiveResponse() {
      if (shouldCloseConnection.get()) {
        return;
      }
      touch();

      try {
        int id = in.readInt();                    // try to read an id

        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " got value #" + id);

        Call call = calls.get(id);

        int state = in.readInt();     // read call status
        if (state == Status.SUCCESS.state) {
          Writable value = ReflectionUtils.newInstance(valueClass, conf);
          value.readFields(in);                 // read value
          call.setValue(value);
          calls.remove(id);
        } else if (state == Status.ERROR.state) {
          call.setException(new RemoteException(WritableUtils.readString(in),
                                                WritableUtils.readString(in)));
          calls.remove(id);
        } else if (state == Status.FATAL.state) {
          // Close the connection
          calls.remove(id);
          markClosed(new RemoteException(WritableUtils.readString(in),
                                         WritableUtils.readString(in)));
        }
      } catch (IOException e) {
        markClosed(e);
      }
    }

    private synchronized void markClosed(IOException e) {
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = e;
        notifyAll();
      }
    }

    /** Close the connection. */
    private synchronized void close() {
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // release the resources
      // first thing to do;take the connection out of the connection list
      synchronized (connections) {
        if (connections.get(remoteId) == this) {
          connections.remove(remoteId);
        }
      }

      // close the streams and therefore the socket
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      disposeSasl();

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn(
              "A connection is closed for no cause and calls are not empty");

          // clean up calls anyway
          closeException = new IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + server + ": " +
              closeException.getMessage(),closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }

    /* Cleanup all calls and mark them as done */
    private void cleanupCalls() {
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
      while (itor.hasNext()) {
        Call c = itor.next().getValue();
        c.setException(closeException); // local exception
        itor.remove();
      }
    }
  }

  /**
   * Construct an IPC client whose values are of the given {@link org.apache.hadoop.io.Writable}
   * class.
   * @param valueClass value class
   * @param conf configuration
   * @param factory socket factory
   */
  public SecureClient(Class<? extends Writable> valueClass, Configuration conf,
      SocketFactory factory) {
    super(valueClass, conf, factory);
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass value class
   * @param conf configuration
   */
  public SecureClient(Class<? extends Writable> valueClass, Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }

  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given host/port are reused. */
  private Connection getConnection(InetSocketAddress addr,
                                   Class<? extends VersionedProtocol> protocol,
                                   UserGroupInformation ticket,
                                   int rpcTimeout,
                                   Call call)
                                   throws IOException, InterruptedException {
    if (!running.get()) {
      // the client is stopped
      throw new IOException("The client is stopped");
    }
    Connection connection;
    /* we could avoid this allocation for each RPC by having a
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    ConnectionId remoteId = new ConnectionId(addr, protocol, ticket, rpcTimeout);
    do {
      synchronized (connections) {
        connection = connections.get(remoteId);
        if (connection == null) {
          connection = new Connection(remoteId);
          connections.put(remoteId, connection);
        }
      }
    } while (!connection.addCall(call));

    //we don't invoke the method below inside "synchronized (connections)"
    //block above. The reason for that is if the server happens to be slow,
    //it will take longer to establish a connection and that will slow the
    //entire system down.
    connection.setupIOstreams();
    return connection;
  }

  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, returning the value.
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception. */
  public Writable call(Writable param, InetSocketAddress addr,
                       Class<? extends VersionedProtocol> protocol,
                       UserGroupInformation ticket, int rpcTimeout)
      throws InterruptedException, IOException {
    Call call = new Call(param);
    Connection connection = getConnection(addr, protocol, ticket, rpcTimeout, call);
    connection.sendParam(call);                 // send the parameter
    boolean interrupted = false;
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (call) {
      while (!call.done) {
        try {
          call.wait();                           // wait for the result
        } catch (InterruptedException ignored) {
          // save the fact that we were interrupted
          interrupted = true;
        }
      }

      if (interrupted) {
        // set the interrupt flag now that we are done waiting
        Thread.currentThread().interrupt();
      }

      if (call.error != null) {
        if (call.error instanceof RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        }
        // local exception
        throw wrapException(addr, call.error);
      }
      return call.value;
    }
  }

}