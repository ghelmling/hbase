package org.apache.hadoop.hbase.coprocessor;

import com.google.protobuf.Service;

/**
 * Coprocessor endpoints providing protobuf services should implement this
 * interface and return the service instance via {@link #getService()}.
 */
public interface CoprocessorService {
  public Service getService();
}
