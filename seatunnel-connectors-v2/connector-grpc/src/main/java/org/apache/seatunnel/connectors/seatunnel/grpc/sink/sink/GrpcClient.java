/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.grpc.sink.sink;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.grpc.sink.config.GrpcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.grpc.sink.exception.GrpcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.grpc.sink.exception.GrpcConnectorException;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.seatunnel.grpc.MetaDataSetDataStream;
import com.seatunnel.grpc.MetaStreamServiceGrpc;
import com.seatunnel.grpc.Project;
import com.seatunnel.grpc.VarType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public class GrpcClient {

    private final String hostName;
    private final int port;
    private final String traceId;
    private final String dataSetId;
    private int retries;
    private final int maxNumRetries;
    private transient ManagedChannel client;
    private transient StreamObserver<MetaDataSetDataStream> metaDataSetDataStreamStreamObserver;
    private final SerializationSchema serializationSchema;
    private volatile boolean isRunning = Boolean.TRUE;
    private static final int CONNECTION_RETRY_DELAY = 500;

    public GrpcClient(GrpcSinkConfig config, SerializationSchema serializationSchema) {
        this.hostName = config.getHost();
        this.port = config.getPort();
        this.traceId = config.getTraceId();
        this.dataSetId = config.getDataSetId();
        this.serializationSchema = serializationSchema;
        retries = config.getMaxNumRetries();
        maxNumRetries = config.getMaxNumRetries();
    }

    private void createConnection() {
        log.error(
                "hostName:{} - port:{} - traceId:{} - dataSetId:{}",
                hostName,
                port,
                traceId,
                dataSetId);
        client =
                ManagedChannelBuilder.forAddress(hostName, port)
                        .maxInboundMessageSize(Integer.MAX_VALUE)
                        .maxInboundMetadataSize(Integer.MAX_VALUE)
                        .usePlaintext()
                        .build();
        metaDataSetDataStreamStreamObserver =
                MetaStreamServiceGrpc.newStub(client)
                        .dataSetDataStream(
                                new StreamObserver<Empty>() {
                                    @Override
                                    public void onNext(Empty empty) {}

                                    @Override
                                    public void onError(Throwable throwable) {}

                                    @Override
                                    public void onCompleted() {}
                                });
    }

    public void open() throws IOException {
        try {
            synchronized (GrpcClient.class) {
                createConnection();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new GrpcConnectorException(
                    GrpcConnectorErrorCode.SOCKET_SERVER_CONNECT_FAILED,
                    String.format("Cannot connect to grpc server at %s:%d", hostName, port),
                    e);
        }
    }

    public void write(SeaTunnelRow row) throws IOException {
        JsonSerializationSchema s = ((JsonSerializationSchema) serializationSchema);
        try {
            if (s.getNode() == null) {
                s.setNode(s.getMapper().createObjectNode());
            }
            s.getRuntimeConverter().convert(s.getMapper(), s.getNode(), row);
        } catch (Throwable e) {
            throw new SeaTunnelJsonFormatException(
                    CommonErrorCode.JSON_OPERATION_FAILED,
                    String.format("Failed to deserialize JSON '%s'.", row),
                    e);
        }

        Map<String, Project> dataProjectMap = new HashMap<>();
        Map<String, Object> jsonDataMap = JsonUtils.toMap(s.getNode());
        Iterator<Map.Entry<String, Object>> iterator = jsonDataMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            dataProjectMap.put(next.getKey(), getProjectBuilder(next.getValue()));
        }
        MetaDataSetDataStream dataBuild =
                MetaDataSetDataStream.newBuilder()
                        .setDataSetId(dataSetId)
                        .setTraceId(traceId)
                        .putAllData(dataProjectMap)
                        .build();
        try {
            log.error("grpc - noNext:{}", dataBuild.toString());
            metaDataSetDataStreamStreamObserver.onNext(dataBuild);
        } catch (Exception e) {
            // if no re-tries are enable, fail immediately
            if (maxNumRetries == 0) {
                throw new GrpcConnectorException(
                        GrpcConnectorErrorCode.SEND_MESSAGE_TO_SOCKET_SERVER_FAILED,
                        String.format(
                                "Failed to send message '%s' to grpc server at %s:%d. Connection re-tries are not enabled.",
                                row, hostName, port),
                        e);
            }

            log.error(
                    "Failed to send message '{}' to grpc server at {}:{}. Trying to reconnect...",
                    row,
                    hostName,
                    port,
                    e);

            synchronized (GrpcClient.class) {
                Exception lastException = null;
                retries = 0;
                while (isRunning && (maxNumRetries < 0 || retries < maxNumRetries)) {
                    // first, clean up the old resources
                    try {
                        if (metaDataSetDataStreamStreamObserver != null) {
                            metaDataSetDataStreamStreamObserver.onCompleted();
                        }
                    } catch (Exception ee) {
                        log.error("Could not close output stream from failed write attempt", ee);
                    }
                    try {
                        if (client != null) {
                            client.shutdown();
                        }
                    } catch (Exception ee) {
                        log.error("Could not close grpc from failed write attempt", ee);
                    }

                    // try again
                    retries++;

                    try {
                        // initialize a new connection
                        createConnection();
                        metaDataSetDataStreamStreamObserver.onNext(dataBuild);
                        return;
                    } catch (Exception ee) {
                        lastException = ee;
                        log.error(
                                "Re-connect to grpc server and send message failed. Retry time(s): {}",
                                retries,
                                ee);
                    }
                    try {
                        this.wait(CONNECTION_RETRY_DELAY);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new GrpcConnectorException(
                                GrpcConnectorErrorCode.SOCKET_WRITE_FAILED,
                                "unable to write; interrupted while doing another attempt",
                                e);
                    }
                }

                if (isRunning) {
                    throw new GrpcConnectorException(
                            GrpcConnectorErrorCode.SEND_MESSAGE_TO_SOCKET_SERVER_FAILED,
                            String.format(
                                    "Failed to send message '%s' to grpc server at %s:%d. Failed after %d retries.",
                                    row, hostName, port, retries),
                            lastException);
                }
            }
        }
    }

    public void close() throws IOException {
        isRunning = false;
        synchronized (this) {
            this.notifyAll();
            try {
                if (metaDataSetDataStreamStreamObserver != null) {
                    metaDataSetDataStreamStreamObserver.onCompleted();
                }
            } finally {
                if (client != null) {
                    client.shutdown();
                }
            }
        }
    }

    private Project getProjectBuilder(Object o) throws UnsupportedEncodingException {
        if (o instanceof Long) {
            return Project.newBuilder().setVarType(VarType.INT64).setValueInt64((long) o).build();
        } else if (o instanceof String) {
            return Project.newBuilder()
                    .setVarType(VarType.STRING)
                    .setValueString(ByteString.copyFrom(String.valueOf(o), "utf-8"))
                    .build();
        } else if (o instanceof Integer) {
            return Project.newBuilder().setVarType(VarType.INT32).setValueInt32((int) o).build();
        } else if (o instanceof Float) {
            return Project.newBuilder().setVarType(VarType.FLOAT).setValueFloat((float) o).build();
        } else if (o instanceof Double) {
            return Project.newBuilder()
                    .setVarType(VarType.DOUBLE)
                    .setValueDouble((double) o)
                    .build();
        } else if (o instanceof Boolean) {
            return Project.newBuilder().setVarType(VarType.BOOL).setValueBool((boolean) o).build();
        } else if (o instanceof byte[]) {
            return Project.newBuilder()
                    .setVarType(VarType.BYTE)
                    .setValueBytes(ByteString.copyFrom((byte[]) o))
                    .build();
        } else if (o instanceof Date) {
            return Project.newBuilder()
                    .setVarType(VarType.TIMESTAMP)
                    .setValueInt64(((Date) o).getTime())
                    .build();
        }
        return Project.newBuilder().build();
    }
}
