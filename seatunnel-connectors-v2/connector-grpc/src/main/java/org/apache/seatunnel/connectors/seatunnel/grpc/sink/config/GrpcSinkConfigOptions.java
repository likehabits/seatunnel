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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class GrpcSinkConfigOptions {
    private static final int DEFAULT_MAX_RETRIES = 3;

    public static final Option<String> HOST =
            Options.key("host").stringType().noDefaultValue().withDescription("grpc host");

    public static final Option<Integer> PORT =
            Options.key("port").intType().noDefaultValue().withDescription("grpc port");

    public static final Option<Integer> DATA_SET_ID =
            Options.key("dataSetId").intType().noDefaultValue().withDescription("grpc dataSetId");

    public static final Option<Integer> TRACE_ID =
            Options.key("traceId").intType().noDefaultValue().withDescription("grpc traceId");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(DEFAULT_MAX_RETRIES)
                    .withDescription("default value is " + DEFAULT_MAX_RETRIES + ", max retries");
}
