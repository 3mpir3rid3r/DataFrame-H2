package com.trading.bot.util;

import com.trading.bot.enums.AggregateFunctionEnum;
import com.trading.bot.enums.ColumnsNamesEnum;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

public class DFUtil {
    public static final String[] defaultTableColumns = new String[]{
            DFUtil.toColumnString(ColumnsNamesEnum.CANDLE_INDEX),
            DFUtil.toColumnString(ColumnsNamesEnum.DATE_AND_TIME),
            DFUtil.toColumnString(ColumnsNamesEnum.OPEN),
            DFUtil.toColumnString(ColumnsNamesEnum.HIGH),
            DFUtil.toColumnString(ColumnsNamesEnum.LOW),
            DFUtil.toColumnString(ColumnsNamesEnum.CLOSE),
            DFUtil.toColumnString(ColumnsNamesEnum.VOLUME),
            DFUtil.toColumnString(ColumnsNamesEnum.TREAD_SIGNAL)
    };
    public static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Random rnd = new Random();

    public static String getRandomString(int length, boolean forTableName) {
        String chars = forTableName ? "ABCDEFGHIJKLMNOPQRSTUVWXYZ" : "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; stringBuilder.toString().replace("-", "").length() < length; i++) {
            int index = (int) (rnd.nextFloat() * chars.length());
            stringBuilder.append(chars.charAt(index));
            if (!forTableName && (i + 1) != length && (i + 1) % 5 == 0) {
                stringBuilder.append("-");
            }
        }
        return stringBuilder.toString();
    }

    public static String toAggFuncString(String column, AggregateFunctionEnum aggregateFunctionEnum) {
        return aggregateFunctionEnum.toString().concat("(").concat(DFUtil.toColumnStringWithQuotes(column)).concat(")").concat(" AS ").concat(DFUtil.toColumnStringWithQuotes(column));
    }

    public static String toColumnStringWithQuotes(String columnName) {
        return "`".concat(columnName.toUpperCase().replaceAll("[^A-Z0-9]", "")).concat("`");
    }

    public static String toHiddenColumnStringWithQuotes(String columnName) {
        return "`".concat(ColumnsNamesEnum.DEFAULT_HIDDEN_COLUMN_PREFIX.toString()).concat(columnName.toUpperCase().replaceAll("[^A-Z0-9]", "")).concat("`");
    }

    public static String toColumnString(String columnName) {
        return columnName.toUpperCase().replaceAll("[^A-Z0-9]", "");
    }

    public static String toHiddenColumnString(String columnName) {
        return ColumnsNamesEnum.DEFAULT_HIDDEN_COLUMN_PREFIX.toString().concat(columnName.toUpperCase().replaceAll("[^A-Z0-9]", ""));
    }

    public static String toColumnStringWithQuotes(ColumnsNamesEnum defaultTableColumnsEnum) {
        return toColumnStringWithQuotes(defaultTableColumnsEnum.toString());
    }

    private static String toColumnString(ColumnsNamesEnum defaultTableColumnsEnum) {
        return defaultTableColumnsEnum.toString().toUpperCase().replaceAll("[^A-Z0-9]", "");
    }

    public static void print(String[][] table) {
        final int maxWidth = 30;
        List<String[]> tableList = new ArrayList<>(Arrays.asList(table));
        List<String[]> finalTableList = new ArrayList<>();
        for (String[] row : tableList) {
            boolean needExtraRow;
            int splitRow = 0;
            do {
                needExtraRow = false;
                String[] newRow = new String[row.length];
                for (int i = 0; i < row.length; i++) {
                    if (row[i].length() < maxWidth) {
                        newRow[i] = splitRow == 0 ? row[i] : "";
                    } else if ((row[i].length() > (splitRow * maxWidth))) {
                        int end = Math.min(row[i].length(), ((splitRow * maxWidth) + maxWidth));
                        newRow[i] = row[i].substring((splitRow * maxWidth), end);
                        needExtraRow = true;
                    } else {
                        newRow[i] = "";
                    }
                }
                finalTableList.add(newRow);
                if (needExtraRow) {
                    splitRow++;
                }
            } while (needExtraRow);
        }
        String[][] finalTable = new String[finalTableList.size()][finalTableList.get(0).length];
        for (int i = 0; i < finalTable.length; i++) {
            finalTable[i] = finalTableList.get(i);
        }
        Map<Integer, Integer> columnLengths = new HashMap<>();
        Arrays.stream(finalTable).forEach(a -> Stream.iterate(0, (i -> i < a.length), (i -> ++i)).forEach(i -> {
            columnLengths.putIfAbsent(i, 0);
            if (columnLengths.get(i) < a[i].length()) {
                columnLengths.put(i, a[i].length());
            }
        }));
        StringBuilder formatString = new StringBuilder("");
        final String flag = "-";
        columnLengths.forEach((key, value) -> formatString.append("| %" + flag).append(value).append("s "));
        formatString.append("|\n");
        String line = columnLengths.entrySet().stream().reduce("", (ln, b) -> {
            String templn = "+-";
            templn = templn + Stream.iterate(0, (i -> i < b.getValue()), (i -> ++i)).reduce("", (ln1, b1) -> ln1 + "-",
                    (a1, b1) -> a1 + b1);
            templn = templn + "-";
            return ln + templn;
        }, (a, b) -> a + b);
        line = line + "+\n";
        System.out.print(line);
        Arrays.stream(finalTable).limit(1).forEach(a -> System.out.printf(formatString.toString(), a));
        System.out.print(line);

        Stream.iterate(1, (i -> i < finalTable.length), (i -> ++i)).forEach(a -> System.out.printf(formatString.toString(), finalTable[a]));
        System.out.print(line);
    }

    public static String convertToString(Object o) {
        if (o instanceof Date) {
            return simpleDateFormat.format((Date) o);
        } else if (o instanceof Long) {
            return ((Long) o).toString();
        } else if (o instanceof Integer) {
            return ((Integer) o).toString();
        } else if (o instanceof Boolean) {
            return ((Boolean) o).toString();
        } else if (o instanceof BigDecimal) {
            return ((BigDecimal) o).toPlainString();
        } else {
            return o.toString();
        }
    }

}
