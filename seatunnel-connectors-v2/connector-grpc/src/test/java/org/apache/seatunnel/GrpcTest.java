package org.apache.seatunnel;

import com.seatunnel.grpc.Empty;
import com.seatunnel.grpc.MetaDataSetDataStream;
import com.seatunnel.grpc.MetaStreamServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class GrpcTest extends MetaStreamServiceGrpc.MetaStreamServiceImplBase {

    public static void main(String[] args) throws Exception {
        Server server = ServerBuilder.forPort(50051).addService(new GrpcTest()).build();
        server.start();
        server.awaitTermination();
    }

    @Override
    public StreamObserver<MetaDataSetDataStream> dataSetDataStream(
            StreamObserver<Empty> responseObserver) {
        return new StreamObserver<MetaDataSetDataStream>() {
            @Override
            public void onNext(MetaDataSetDataStream metaDataSetDataStream) {
                System.out.println(metaDataSetDataStream.toString());
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("onError");
            }

            @Override
            public void onCompleted() {
                System.out.println("onCompleted");
            }
        };
    }
}
