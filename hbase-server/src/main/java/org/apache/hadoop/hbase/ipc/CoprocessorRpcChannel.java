package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

/**
 */
public class CoprocessorRpcChannel implements RpcChannel, BlockingRpcChannel {
  private static Log LOG = LogFactory.getLog(CoprocessorRpcChannel.class);

  private Configuration conf;
  private final HConnection connection;
  private final byte[] table;
  private final byte[] row;

  public CoprocessorRpcChannel(Configuration conf, HConnection conn,
                               byte[] table, byte[] row) {
    this.conf = conf;
    this.connection = conn;
    this.table = table;
    this.row = row;
  }

  @Override
  public void callMethod(Descriptors.MethodDescriptor method,
                         RpcController controller,
                         Message request, Message responsePrototype,
                         RpcCallback<Message> callback) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Call: "+method.getName()+", "+request.toString());
    }

    if (row != null) {
      final Exec exec = new Exec(conf, row, protocol, method, args);
      ServerCallable<ExecResult> callable =
          new ServerCallable<ExecResult>(connection, table, row) {
            public ExecResult call() throws Exception {
              byte[] regionName = location.getRegionInfo().getRegionName();
              return ProtobufUtil.execCoprocessor(server, exec, regionName);
            }
          };
      ExecResult result = callable.withRetries();
      this.regionName = result.getRegionName();
      LOG.debug("Result is region="+ Bytes.toStringBinary(regionName) +
          ", value="+result.getValue());
      return result.getValue();
    }

    return null;
  }

  @Override
  public Message callBlockingMethod(Descriptors.MethodDescriptor method,
                                    RpcController rpcController,
                                    Message request, Message responsePrototype)
      throws ServiceException {
    return null;
  }
}
