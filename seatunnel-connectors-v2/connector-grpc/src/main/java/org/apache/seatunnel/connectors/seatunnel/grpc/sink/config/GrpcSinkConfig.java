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

package org.apache.seatunnel.connectors.seatunnel.grpc.sink.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.grpc.sink.config.GrpcSinkConfigOptions.DATA_SET_ID;
import static org.apache.seatunnel.connectors.seatunnel.grpc.sink.config.GrpcSinkConfigOptions.HOST;
import static org.apache.seatunnel.connectors.seatunnel.grpc.sink.config.GrpcSinkConfigOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.grpc.sink.config.GrpcSinkConfigOptions.PORT;
import static org.apache.seatunnel.connectors.seatunnel.grpc.sink.config.GrpcSinkConfigOptions.TRACE_ID;

@Data
public class GrpcSinkConfig implements Serializable {
    private String host;
    private int port;
    private String dataSetId;
    private String traceId;
    private int maxNumRetries;

    public GrpcSinkConfig(Config config) {
        this.host = config.getString(HOST.key());
        this.port = config.getInt(PORT.key());
        this.dataSetId = config.getString(DATA_SET_ID.key());
        this.traceId = config.getString(TRACE_ID.key());
        if (config.hasPath(MAX_RETRIES.key())) {
            this.maxNumRetries = config.getInt(MAX_RETRIES.key());
        }
    }
}
