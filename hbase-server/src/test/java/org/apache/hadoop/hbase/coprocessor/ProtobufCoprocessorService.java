package org.apache.hadoop.hbase.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;

import java.io.IOException;

/**
*/
public class ProtobufCoprocessorService
    extends TestRpcServiceProtos.TestProtobufRpcProto
    implements CoprocessorService, Coprocessor {
  public ProtobufCoprocessorService() {
  }

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void ping(RpcController controller, TestProtos.EmptyRequestProto request,
                   RpcCallback<TestProtos.EmptyResponseProto> done) {
    done.run(TestProtos.EmptyResponseProto.getDefaultInstance());
  }

  @Override
  public void echo(RpcController controller, TestProtos.EchoRequestProto request,
                   RpcCallback<TestProtos.EchoResponseProto> done) {
    String message = request.getMessage();
    done.run(TestProtos.EchoResponseProto.newBuilder().setMessage(message).build());
  }

  @Override
  public void error(RpcController controller, TestProtos.EmptyRequestProto request,
                    RpcCallback<TestProtos.EmptyResponseProto> done) {
    ResponseConverter.setControllerException(controller, new IOException("Test exception"));
    done.run(null);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
