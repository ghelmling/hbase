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
