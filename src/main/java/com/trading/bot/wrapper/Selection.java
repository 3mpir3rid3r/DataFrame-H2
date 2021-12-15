package com.trading.bot.wrapper;

import com.trading.bot.enums.ColumnsNamesEnum;
import com.trading.bot.enums.ReplaceEnum;
import com.trading.bot.enums.WhereConditionEnum;
import com.trading.bot.util.DFUtil;
import lombok.Getter;

import javax.transaction.NotSupportedException;
import java.io.InvalidObjectException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Selection {
    private final boolean canJoin;
    @Getter
    private final boolean isOk;
    @Getter
    private String conditionString;

    private Selection(String conditionString, boolean canJoin) {
        this.conditionString = conditionString;
        this.canJoin = canJoin;
        isOk = true;
    }

    public static Selection createForHiddenColumn(String column, WhereConditionEnum whereConditionEnum, Object... value) {
        return create(DFUtil.toHiddenColumnString(column), whereConditionEnum, value);
    }

    public static Selection create(String column, WhereConditionEnum whereConditionEnum, Object... value) {
        String sql;
        if (WhereConditionEnum.BETWEEN.equals(whereConditionEnum)) {
            sql = DFUtil.toColumnStringWithQuotes(column).concat(" ").concat(whereConditionEnum.getWhereCondition()).concat(" ").concat("'").concat(value[0].toString()).concat("'").concat(" AND ").concat("'").concat(value[1].toString()).concat("'");
        } else if (WhereConditionEnum.IN.equals(whereConditionEnum) || WhereConditionEnum.NOT_IN.equals(whereConditionEnum)) {
            String collect = Arrays.stream(value).map(o -> "'".concat(o.toString()).concat("'")).collect(Collectors.joining(","));
            sql = DFUtil.toColumnStringWithQuotes(column).concat(" ").concat(whereConditionEnum.getWhereCondition()).concat(" (").concat(collect).concat(")");
        } else {
            sql = DFUtil.toColumnStringWithQuotes(column).concat(" ").concat(whereConditionEnum.getWhereCondition()).concat(" ").concat("'").concat(value[0].toString()).concat("'");
        }
        return new Selection(sql, true);
    }

    private static <T> Selection getInCondition(List<T> value, WhereConditionEnum whereConditionEnum) throws Exception {
        if (value.size() <= 0) {
            throw new InvalidObjectException("Invalid row count");
        }
        String collect = value.stream().map(o -> "'".concat(o.toString()).concat("'")).collect(Collectors.joining(","));
        return new Selection(" ".concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" ").concat(whereConditionEnum.getWhereCondition()).concat(" (").concat(collect).concat(")"), true);

    }

    public static <T> Selection in(List<T> value) throws Exception {
        return getInCondition(value, WhereConditionEnum.IN);
    }

    public static Selection in(Object... value) throws Exception {
        return getInCondition(Arrays.asList(value), WhereConditionEnum.IN);
    }

    public static <T> Selection notIn(List<T> value) throws Exception {
        return getInCondition(value, WhereConditionEnum.NOT_IN);
    }

    public static Selection notIn(Object... value) throws Exception {
        return getInCondition(Arrays.asList(value), WhereConditionEnum.NOT_IN);
    }

    public static Selection last(int rowCount) throws Exception {
        if (rowCount <= 0) {
            throw new InvalidObjectException("Invalid row count");
        }
        return new Selection(" ".concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" > (SELECT MAX(").concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(") - 1 - ").concat(String.valueOf(rowCount)).concat(" FROM ").concat(ReplaceEnum.DATA_TABLE.getReplace()).concat(") AND ").concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" <> (SELECT MAX(").concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(") FROM ").concat(ReplaceEnum.DATA_TABLE.getReplace()).concat(")"), true);
    }

    public static Selection limitLast(int rowCount) throws Exception {
        if (rowCount <= 0) {
            throw new InvalidObjectException("Invalid row count");
        }
        return new Selection(" ".concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" <> (SELECT MAX(").concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(") FROM ").concat(ReplaceEnum.DATA_TABLE.getReplace()).concat(") ORDER BY ").concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" DESC LIMIT ").concat(String.valueOf(rowCount)), false);
    }

    public static Selection first(int rowCount) throws Exception {
        if (rowCount <= 0) {
            throw new InvalidObjectException("Invalid row count");
        }
        return new Selection(" ".concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" < (SELECT MIN(").concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(") + ").concat(String.valueOf(rowCount)).concat(" FROM ").concat(ReplaceEnum.DATA_TABLE.getReplace()).concat(")"), true);
    }

    public static Selection limitFirst(int rowCount) throws Exception {
        if (rowCount <= 0) {
            throw new InvalidObjectException("Invalid row count");
        }
        return new Selection(" ".concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" > 0 ORDER BY ").concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" ASC LIMIT ").concat(String.valueOf(rowCount)), false);
    }

    public static Selection between(int start, int end) throws Exception {
        if (start <= 0) {
            throw new InvalidObjectException("Invalid start value.");
        }
        if (end <= 0) {
            throw new InvalidObjectException("Invalid end end.");
        }
        return Selection.create(ColumnsNamesEnum.CANDLE_INDEX.toString(), WhereConditionEnum.BETWEEN, String.valueOf(start), String.valueOf(end));
    }

    public static Selection limitBetween(int start, int count) throws Exception {
        if (start <= 0) {
            throw new InvalidObjectException("Invalid start value.");
        }
        if (count <= 0) {
            throw new InvalidObjectException("Invalid row count.");
        }
        return new Selection(" ".concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" > 0 ORDER BY ").concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" ASC LIMIT ").concat(String.valueOf(start)).concat(",").concat(String.valueOf(count)), false);
    }

    public static Selection lastFinalised() throws Exception {
        return new Selection(" ".concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" = (SELECT MAX(").concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(") - 1 FROM ").concat(ReplaceEnum.DATA_TABLE.getReplace()).concat(")"), true);
    }

    public static Selection lastRunning() throws Exception {
        return new Selection(" ".concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(" = (SELECT MAX(").concat(DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX)).concat(") FROM ").concat(ReplaceEnum.DATA_TABLE.getReplace()).concat(")"), true);
    }

    public Selection and(Selection selection) throws Exception {
        if (!canJoin) {
            throw new NotSupportedException("Can't join this selection object.left side object not supported.");
        }
        conditionString = conditionString.concat(" AND ").concat(selection.getConditionString());
        return this;
    }

    public Selection or(Selection selection) throws Exception {
        if (!canJoin) {
            throw new NotSupportedException("Can't join this selection object.left side object not supported.");
        }
        conditionString = conditionString.concat(" OR ").concat(selection.getConditionString());
        return this;
    }
}
