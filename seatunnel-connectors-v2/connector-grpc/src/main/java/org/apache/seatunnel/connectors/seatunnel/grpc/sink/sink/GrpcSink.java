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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.grpc.sink.config.GrpcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.grpc.sink.exception.GrpcConnectorException;

import com.google.auto.service.AutoService;

import java.io.IOException;

import static org.apache.seatunnel.connectors.seatunnel.grpc.sink.config.GrpcSinkConfigOptions.DATA_SET_ID;
import static org.apache.seatunnel.connectors.seatunnel.grpc.sink.config.GrpcSinkConfigOptions.HOST;
import static org.apache.seatunnel.connectors.seatunnel.grpc.sink.config.GrpcSinkConfigOptions.PORT;
import static org.apache.seatunnel.connectors.seatunnel.grpc.sink.config.GrpcSinkConfigOptions.TRACE_ID;

@AutoService(SeaTunnelSink.class)
public class GrpcSink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    private Config pluginConfig;
    private GrpcSinkConfig sinkConfig;
    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public String getPluginName() {
        return "Grpc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, PORT.key(), HOST.key(), DATA_SET_ID.key(), TRACE_ID.key());
        if (!result.isSuccess()) {
            throw new GrpcConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        sinkConfig = new GrpcSinkConfig(pluginConfig);
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new GrpcSinkWriter(sinkConfig, seaTunnelRowType);
    }
}
