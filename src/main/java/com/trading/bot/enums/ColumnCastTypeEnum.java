package com.trading.bot.enums;

import java.math.BigDecimal;
import java.util.Date;

public enum ColumnCastTypeEnum {
    DATE_TIME(Date.class),
    LONG(Long.class),
    INTEGER(Integer.class),
    STRING(String.class),
    BOOLEAN(Boolean.class),
    BIG_DECIMAL(BigDecimal.class),
    OBJECT(Object.class);
    private final Class<?> castType;

    ColumnCastTypeEnum(Class<?> castType) {
        this.castType = castType;
    }

    public Class<?> getCastType() {
        return castType;
    }
}
