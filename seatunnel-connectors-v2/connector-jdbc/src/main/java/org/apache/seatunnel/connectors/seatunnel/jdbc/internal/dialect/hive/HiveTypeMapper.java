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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class HiveTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    // ============================data types=====================

    private static final String HIVE_UNKNOWN = "UNKNOWN";
    private static final String HIVE_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String HIVE_TINYINT = "TINYINT";
    private static final String HIVE_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String HIVE_SMALLINT = "SMALLINT";
    private static final String HIVE_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String HIVE_MEDIUMINT = "MEDIUMINT";
    private static final String HIVE_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String HIVE_INT = "INT";
    private static final String HIVE_INT_UNSIGNED = "INT UNSIGNED";
    private static final String HIVE_INTEGER = "INTEGER";
    private static final String HIVE_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String HIVE_BIGINT = "BIGINT";
    private static final String HIVE_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String HIVE_DECIMAL = "DECIMAL";
    private static final String HIVE_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String HIVE_FLOAT = "FLOAT";
    private static final String HIVE_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String HIVE_DOUBLE = "DOUBLE";
    private static final String HIVE_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    private static final String HIVE_STRING = "STRING";
    private static final String HIVE_CHAR = "CHAR";
    private static final String HIVE_VARCHAR = "VARCHAR";
    private static final String HIVE_TINYTEXT = "TINYTEXT";
    private static final String HIVE_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String HIVE_TEXT = "TEXT";
    private static final String HIVE_LONGTEXT = "LONGTEXT";
    private static final String HIVE_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String HIVE_DATE = "DATE";
    private static final String HIVE_DATETIME = "DATETIME";
    private static final String HIVE_TIME = "TIME";
    private static final String HIVE_TIMESTAMP = "TIMESTAMP";
    private static final String HIVE_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    private static final String HIVE_TINYBLOB = "TINYBLOB";
    private static final String HIVE_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String HIVE_BLOB = "BLOB";
    private static final String HIVE_LONGBLOB = "LONGBLOB";
    private static final String HIVE_BINARY = "BINARY";
    private static final String HIVE_VARBINARY = "VARBINARY";
    private static final String HIVE_GEOMETRY = "GEOMETRY";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (mysqlType) {
            case HIVE_BIT:
                if (precision == 1) {
                    return BasicType.BOOLEAN_TYPE;
                } else {
                    return PrimitiveByteArrayType.INSTANCE;
                }
            case HIVE_TINYINT:
            case HIVE_TINYINT_UNSIGNED:
            case HIVE_SMALLINT:
            case HIVE_SMALLINT_UNSIGNED:
            case HIVE_MEDIUMINT:
            case HIVE_MEDIUMINT_UNSIGNED:
            case HIVE_INT:
            case HIVE_INTEGER:
            case HIVE_YEAR:
                return BasicType.INT_TYPE;
            case HIVE_INT_UNSIGNED:
            case HIVE_INTEGER_UNSIGNED:
            case HIVE_BIGINT:
                return BasicType.LONG_TYPE;
            case HIVE_BIGINT_UNSIGNED:
                return new DecimalType(20, 0);
            case HIVE_DECIMAL:
                if (precision > 38) {
                    LOG.warn("{} will probably cause value overflow.", HIVE_DECIMAL);
                    return new DecimalType(38, 18);
                }
                return new DecimalType(precision, scale);
            case HIVE_DECIMAL_UNSIGNED:
                return new DecimalType(precision + 1, scale);
            case HIVE_FLOAT:
                return BasicType.FLOAT_TYPE;
            case HIVE_FLOAT_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", HIVE_FLOAT_UNSIGNED);
                return BasicType.FLOAT_TYPE;
            case HIVE_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case HIVE_DOUBLE_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", HIVE_DOUBLE_UNSIGNED);
                return BasicType.DOUBLE_TYPE;
            case HIVE_STRING:
            case HIVE_CHAR:
            case HIVE_TINYTEXT:
            case HIVE_MEDIUMTEXT:
            case HIVE_TEXT:
            case HIVE_VARCHAR:
            case HIVE_JSON:
                return BasicType.STRING_TYPE;
            case HIVE_LONGTEXT:
                LOG.warn(
                        "Type '{}' has a maximum precision of 536870911 in MySQL. "
                                + "Due to limitations in the seatunnel type system, "
                                + "the precision will be set to 2147483647.",
                        HIVE_LONGTEXT);
                return BasicType.STRING_TYPE;
            case HIVE_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case HIVE_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case HIVE_DATETIME:
            case HIVE_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case HIVE_TINYBLOB:
            case HIVE_MEDIUMBLOB:
            case HIVE_BLOB:
            case HIVE_LONGBLOB:
            case HIVE_VARBINARY:
            case HIVE_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

                // Doesn't support yet
            case HIVE_GEOMETRY:
            case HIVE_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format(
                                "Doesn't support Hive type '%s' on column '%s'  yet.",
                                mysqlType, jdbcColumnName));
        }
    }
}
