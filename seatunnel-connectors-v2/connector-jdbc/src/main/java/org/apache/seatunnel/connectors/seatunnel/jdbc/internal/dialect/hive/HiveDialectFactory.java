package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;

import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(JdbcDialectFactory.class)
public class HiveDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(@NonNull String url) {
        return url.startsWith("jdbc:hive2:");
    }

    @Override
    public JdbcDialect create() {
        return new HiveDialect();
    }

    @Override
    public JdbcDialect create(String compatibleMode, String fieldId) {
        return new HiveDialect(fieldId);
    }
}
