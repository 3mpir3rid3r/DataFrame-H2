package com.trading.bot.enums;

public enum ColumnTypeEnum {
    DATE_TIME("timestamp DEFAULT CURRENT_TIMESTAMP"),
    LONG("bigint DEFAULT 0"),
    INTEGER("int DEFAULT 0"),
    STRING("varchar(255) DEFAULT ''"),
    BOOLEAN("boolean DEFAULT false"),
    BIG_DECIMAL("decimal(30, 10) DEFAULT 0.0");
    private final String columnType;

    ColumnTypeEnum(String columnType) {
        this.columnType = columnType;
    }

    public String getColumnType() {
        return columnType;
    }
}
