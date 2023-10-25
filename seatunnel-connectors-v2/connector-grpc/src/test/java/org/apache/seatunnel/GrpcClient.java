// package org.apache.seatunnel;
//
// import com.google.protobuf.ByteString;
// import com.google.protobuf.Empty;
// import com.seatunnel.grpc.MetaDataSetDataStream;
// import com.seatunnel.grpc.MetaStreamServiceGrpc;
// import com.seatunnel.grpc.Project;
// import com.seatunnel.grpc.VarType;
// import io.grpc.ManagedChannel;
// import io.grpc.ManagedChannelBuilder;
// import io.grpc.stub.StreamObserver;
// import lombok.extern.slf4j.Slf4j;
//
// import java.io.UnsupportedEncodingException;
// // import java.util.concurrent.TimeUnit;
//
// @Slf4j
// public class GrpcClient {
//    public static void main(String[] args) {
//        try {
//            ManagedChannel client =
//                    ManagedChannelBuilder.forAddress("127.0.0.1", 50051)
//                            .maxInboundMessageSize(Integer.MAX_VALUE)
//                            .maxInboundMetadataSize(Integer.MAX_VALUE)
//                            .usePlaintext()
//                            .build();
//            StreamObserver<MetaDataSetDataStream> metaDataSetDataStreamStreamObserver =
//                    MetaStreamServiceGrpc.newStub(client)
//                            .dataSetDataStream(
//                                    new StreamObserver<Empty>() {
//                                        @Override
//                                        public void onNext(Empty empty) {}
//
//                                        @Override
//                                        public void onError(Throwable throwable) {}
//
//                                        @Override
//                                        public void onCompleted() {
//                                            log.error("active close");
//                                        }
//                                    });
//
//            System.out.println("启动");
//            List<Map<String, Object>> list = new ArrayList<>();
//            list.add(
//                    new HashMap() {
//                        {
//                            put("id", 1);
//                            put("name", "hauhua");
//                            put("age", System.currentTimeMillis()+10000);
//                        }
//                    });
//            list.add(
//                    new HashMap() {
//                        {
//                            put("id", 2);
//                            put("name", "hauhua1");
//                            put("age", System.currentTimeMillis()+20000);
//                        }
//                    });
//            list.add(
//                    new HashMap() {
//                        {
//                            put("id", 3);
//                            put("name", "hauhua1");
//                            put("age", System.currentTimeMillis()+30000);
//                        }
//                    });
//
//            for (Map<String, Object> map : list) {
//                Map<String, Project> dataProjectMap = new HashMap<>();
//                Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
//                while (iterator.hasNext()) {
//                    Map.Entry<String, Object> next = iterator.next();
//                    dataProjectMap.put(next.getKey(), getProjectBuilder(next.getValue()));
//                }
//                MetaDataSetDataStream build =
//                        MetaDataSetDataStream.newBuilder()
//                                .setDataSetId("12123232")
//                                .setTraceId("3434df")
//                                .putAllData(dataProjectMap)
//                                .build();
//                System.out.println(build.toString());
//                metaDataSetDataStreamStreamObserver.onNext(build);
//                System.out.println("发送");
//            }
//            metaDataSetDataStreamStreamObserver.onCompleted();
//            client.shutdown().awaitTermination(5, TimeUnit.SECONDS);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static Project getProjectBuilder(Object o) throws UnsupportedEncodingException {
//        if (o instanceof Long) {
//            return Project.newBuilder().setVarType(VarType.INT64).setValueInt64((long) o).build();
//        } else if (o instanceof String) {
//            return Project.newBuilder()
//                    .setVarType(VarType.STRING)
//                    .setValueString(ByteString.copyFrom(o.toString(), "utf-8"))
//                    .build();
//        } else if (o instanceof Integer) {
//            return Project.newBuilder()
//                    .setVarType(VarType.INT32)
//                    .setValueInt32((Integer) o)
//                    .build();
//        } else if (o instanceof Float) {
//            return Project.newBuilder().setVarType(VarType.FLOAT).setValueFloat((Float)
// o).build();
//        } else if (o instanceof Double) {
//            return Project.newBuilder()
//                    .setVarType(VarType.DOUBLE)
//                    .setValueDouble((Double) o)
//                    .build();
//        } else if (o instanceof Boolean) {
//            return Project.newBuilder().setVarType(VarType.BOOL).setValueBool((Boolean)
// o).build();
//        } else if (o instanceof byte[]) {
//            return Project.newBuilder()
//                    .setVarType(VarType.BYTE)
//                    .setValueBytes(ByteString.copyFrom((byte[]) o))
//                    .build();
//        } else if (o instanceof Date) {
//            return Project.newBuilder()
//                    .setVarType(VarType.TIMESTAMP)
//                    .setValueInt64(((Date) o).getTime())
//                    .build();
//        }
//        return Project.newBuilder()
//                .setVarType(VarType.STRING)
//                .setValueString(ByteString.copyFrom("", "utf-8"))
//                .build();
//    }
// }
