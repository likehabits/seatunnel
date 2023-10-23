 package org.apache.seatunnel;

 import com.google.protobuf.Empty;
 import com.seatunnel.grpc.MetaDataSetDataStream;
 import com.seatunnel.grpc.MetaStreamServiceGrpc;
 import io.grpc.Server;
 import io.grpc.ServerBuilder;
 import io.grpc.stub.StreamObserver;

 public class GrpcTest extends MetaStreamServiceGrpc.MetaStreamServiceImplBase {

    public static void main(String[] args) throws Exception {
        Server server = ServerBuilder.forPort(50051).addService(new GrpcTest()).build();
        System.out.println("启动了");
        server.start();
        server.awaitTermination();

    }

    @Override
    public StreamObserver<MetaDataSetDataStream> dataSetDataStream(
            StreamObserver<Empty> responseObserver) {
        return new StreamObserver<MetaDataSetDataStream>() {
            String dataSetId = null;

            @Override
            public void onNext(MetaDataSetDataStream metaDataSetDataStream) {
                dataSetId = metaDataSetDataStream.getDataSetId();
                System.out.println("接受消息:" + metaDataSetDataStream.toString());
            }

            @Override
            public void onError(Throwable throwable) {}

            @Override
            public void onCompleted() {
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
                System.out.println(dataSetId + ":结束");
            }
        };
    }
 }
