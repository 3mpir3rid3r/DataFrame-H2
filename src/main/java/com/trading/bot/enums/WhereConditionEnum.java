package com.trading.bot.enums;

public enum WhereConditionEnum {
    EQUALS("="),
    NOT_EQUALS("<>"),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUALS(">="),
    LESS_THAN("<"),
    LESS_THAN_OR_EQUALS("<="),
    IN("IN"),
    NOT_IN("NOT IN"),
    BETWEEN("BETWEEN");
    private final String whereCondition;

    WhereConditionEnum(String whereCondition) {
        this.whereCondition = whereCondition;
    }

    public String getWhereCondition() {
        return whereCondition;
    }
}
