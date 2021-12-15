package com.trading.bot.enums;

public enum ReplaceEnum {
    DATA_TABLE("tblNameForQuery");
    private final String replace;

    ReplaceEnum(String replace) {
        this.replace = replace;
    }

    public String getReplace() {
        return replace;
    }
}
