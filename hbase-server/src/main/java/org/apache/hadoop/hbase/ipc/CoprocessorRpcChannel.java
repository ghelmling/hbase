package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;

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

    if (row == null) {
      throw new IllegalArgumentException("Missing row property for remote region location");
    }

    final ClientProtos.CoprocessorServiceCall call =
        ClientProtos.CoprocessorServiceCall.newBuilder()
            .setRow(ByteString.copyFrom(row))
            .setServiceName(method.getService().getFullName())
            .setMethodName(method.getName())
            .setRequest(request.toByteString()).build();
    ServerCallable<ClientProtos.CoprocessorServiceResponse> callable =
        new ServerCallable<ClientProtos.CoprocessorServiceResponse>(connection, table, row) {
          public CoprocessorServiceResponse call() throws Exception {
            byte[] regionName = location.getRegionInfo().getRegionName();
            return ProtobufUtil.execService(server, call, regionName);
          }
        };
    Message response = null;
    try {
      CoprocessorServiceResponse result = callable.withRetries();
      response = responsePrototype.newBuilderForType().mergeFrom(result.getValue()).build();
      LOG.debug("Result is region="+ Bytes.toStringBinary(
          result.getRegion().getValue().toByteArray()) + ", value="+response);
    } catch (IOException ioe) {
      if (controller != null) {
        controller.setFailed(ioe.getMessage());
      }
    }
    if (callback != null) {
      callback.run(response);
    }
  }

  @Override
  public Message callBlockingMethod(Descriptors.MethodDescriptor method,
                                    RpcController controller,
                                    Message request, Message responsePrototype)
      throws ServiceException {
    final Message.Builder responseBuilder = responsePrototype.newBuilderForType();
    callMethod(method, controller, request, responsePrototype,
        new RpcCallback<Message>() {
          @Override
          public void run(Message message) {
            responseBuilder.mergeFrom(message);
          }
        });

    return responseBuilder.build();
  }
}
