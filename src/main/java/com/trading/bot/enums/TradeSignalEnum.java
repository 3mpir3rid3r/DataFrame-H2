package com.trading.bot.enums;

public enum TradeSignalEnum {
    BUY("1"),
    SELL("2"),
    NONE("0");
    private final String tradeSignal;

    TradeSignalEnum(String tradeSignal) {
        this.tradeSignal = tradeSignal;
    }

    public String getTradeSignal() {
        return tradeSignal;
    }
}
