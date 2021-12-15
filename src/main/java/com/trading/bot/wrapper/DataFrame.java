package com.trading.bot.wrapper;

import com.trading.bot.entity.TableMst;
import com.trading.bot.enums.*;
import com.trading.bot.repository.TableMstRepository;
import com.trading.bot.util.DFUtil;
import com.trading.bot.util.Util;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.CandlestickRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.DefaultHighLowDataset;
import org.jfree.data.xy.OHLCDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.ta4j.core.Bar;
import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBarSeriesBuilder;
import org.ta4j.core.indicators.helpers.ClosePriceIndicator;

import javax.transaction.NotSupportedException;
import javax.transaction.Transactional;
import javax.transaction.TransactionalException;
import java.awt.*;
import java.io.InvalidObjectException;
import java.math.BigDecimal;
import java.rmi.AlreadyBoundException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

@Component
@NoArgsConstructor
public class DataFrame {
    private static JdbcTemplate jdbcTemplate;
    private static TableMstRepository tableMstRepository;
    private TableMst tableMst;
    private boolean hasPendingQuery;
    private boolean canCallAggregate;
    private String columnName;
    private String pendingQuery;
    private int SARCalPeriod;
    private double SARCalAtrMultiplier;
    @Getter
    private String coin;
    @Getter
    private boolean isOk;

    @Autowired
    private DataFrame(JdbcTemplate jdbcTemplate, TableMstRepository tableMstRepository) {
        DataFrame.jdbcTemplate = jdbcTemplate;
        DataFrame.tableMstRepository = tableMstRepository;
    }

    private DataFrame(String coin, String ipAddress, boolean isSpotDataFrame, int SARCalPeriod, double SARCalAtrMultiplier) {
        String dataTableName = DFUtil.getRandomString(50, true);
        try {
            tableMst = new TableMst();
            this.coin = coin;
            this.SARCalPeriod = SARCalPeriod;
            this.SARCalAtrMultiplier = SARCalAtrMultiplier;
            tableMst.setCoinName(coin);
            tableMst.setIpAddress(ipAddress);
            tableMst.setDataTableName(dataTableName);
            tableMst.setBotType(isSpotDataFrame ? "SPOT" : "FUTURES");
            tableMst = tableMstRepository.save(tableMst);
            try {
                isOk = createDataTableWithDefaultColumn(dataTableName, isSpotDataFrame);
                isOk = updateDataFrameWithHiddenColumns(isOk);
            } catch (Exception e) {
                isOk = false;
                throw new RuntimeException("Data table initiate error.");
            }
        } catch (Exception e) {
            isOk = false;
            throw new RuntimeException("Main table initiate error.");
        }
    }

    private boolean updateDataFrameWithHiddenColumns(boolean isOk) throws Exception {
        addHiddenColumns(ColumnTypeEnum.BIG_DECIMAL, "tr", "upperBand", "lowerBand", "sAndR", "sAndRGap")
                .addHiddenColumns(ColumnTypeEnum.BOOLEAN, "isUpTrend", "isReversal", "isSAndR")
                .addHiddenColumns(ColumnTypeEnum.INTEGER, "sAndRTouchCount");
        return isOk;
    }


    public static DataFrame createSpotDataFrame(String coin, String ipAddress, int SARCalPeriod, double SARCalAtrMultiplier) {
        return new DataFrame(coin, ipAddress, true, SARCalPeriod, SARCalAtrMultiplier);
    }

    public static DataFrame createFuturesDataFrame(String coin, String ipAddress, int SARCalPeriod, double SARCalAtrMultiplier) {
        return new DataFrame(coin, ipAddress, false, SARCalPeriod, SARCalAtrMultiplier);
    }

    private boolean createDataTableWithDefaultColumn(String dataTableName, boolean isSpotDataFrame) throws Exception {
        if (isSpotDataFrame) {
            return 0 == jdbcTemplate.update("CREATE TABLE " + dataTableName + " ( " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX) + " BIGINT auto_increment PRIMARY KEY, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.DATE_AND_TIME) + " TIMESTAMP NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.OPEN) + " decimal(30, 10) NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CLOSE) + " decimal(30, 10) NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.HIGH) + " decimal(30, 10) NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.LOW) + " decimal(30, 10) NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.VOLUME) + " decimal(30, 10) NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.TREAD_SIGNAL) + " int NOT NULL )");
        } else {
            return 0 == jdbcTemplate.update("CREATE TABLE " + dataTableName + " ( " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CANDLE_INDEX) + " BIGINT auto_increment PRIMARY KEY, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.DATE_AND_TIME) + " TIMESTAMP NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.OPEN) + " decimal(30, 10) NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CLOSE) + " decimal(30, 10) NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.HIGH) + " decimal(30, 10) NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.LOW) + " decimal(30, 10) NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.VOLUME) + " decimal(30, 10) NOT NULL," + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.STOP_LOSS) + " decimal(30, 10) DEFAULT 0.0 NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.ENTRY) + " decimal(30, 10) DEFAULT 0.0 NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.TAKE_PROFIT) + " decimal(30, 10) DEFAULT 0.0 NOT NULL, " + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.TREAD_SIGNAL) + " int NOT NULL )");
        }
    }

    public DataFrame where(Selection selection) throws Exception {
        if (hasPendingQuery) {
            if (selection != null) {
                if (!selection.isOk()) {
                    throw new InvalidObjectException("Selection not initiated.");
                }
                if (pendingQuery.contains(" WHERE ")) {
                    throw new AlreadyBoundException("Where clause already exist.");
                }
                if (pendingQuery.contains(" GROUP BY ")) {
                    throw new AlreadyBoundException("Group by clause already exist.");
                }
                pendingQuery = pendingQuery.concat(" WHERE ".concat(selection.getConditionString()));
            }
            return this;
        } else {
            throw new DataFormatException("No any pending query.you need to create new query before call where().");
        }
    }

    public DataFrame groupBy(String... columns) throws Exception {
        if (hasPendingQuery) {
            if (columns != null && columns.length > 0) {
                if (pendingQuery.contains(" GROUP BY ")) {
                    throw new AlreadyBoundException("Group by clause already exist.");
                }
                String collect = Arrays.stream(columns).map(s -> DFUtil.toColumnStringWithQuotes(checkAndConvertColumn(s))).collect(Collectors.joining(","));
                pendingQuery = pendingQuery.concat(" GROUP BY ".concat(collect));
            }
            return this;
        } else {
            throw new DataFormatException("No any pending query.you need to create new query before call where().");
        }
    }

    private void replaceQueryConstantBeforeExecute() {
        pendingQuery = pendingQuery.replace(ReplaceEnum.DATA_TABLE.getReplace(), tableMst.getDataTableName());
    }

    public boolean insertRowToTable(Date t, String o, String h, String l, String c, String v) throws Exception {
        if (0 < jdbcTemplate.update("INSERT INTO " + tableMst.getDataTableName() + " (" + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.DATE_AND_TIME) + "," + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.OPEN) + "," + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.CLOSE) + "," + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.HIGH) + "," + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.LOW) + "," + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.VOLUME) + "," + DFUtil.toColumnStringWithQuotes(ColumnsNamesEnum.TREAD_SIGNAL) + ") VALUES ('" + DFUtil.simpleDateFormat.format(t) + "','" + o + "','" + c + "','" + h + "','" + l + "','" + v + "', '" + TradeSignalEnum.NONE.getTradeSignal() + "')")) {
            calculateSupportAndResistance();
            return true;
        } else {
            return false;
        }
    }

    private DataFrame cleanPendingQuery() {
        hasPendingQuery = false;
        pendingQuery = "";
        columnName = "";
        return this;
    }

    private boolean isColumnExist(String columnName) {
        try {
            if (isHasAggregate(columnName)) {
                columnName = columnName.substring(columnName.indexOf("("), columnName.indexOf(")")).replace("(", "").replace("`", "");
            }
            String s = jdbcTemplate.queryForObject("".concat("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '").concat(tableMst.getDataTableName()).concat("' AND COLUMN_NAME = '").concat(columnName).concat("'"), String.class);
            return Objects.nonNull(s) && !s.trim().isEmpty();
        } catch (Exception ex) {
            return false;
        }
    }

    private String getAllColumnNames(boolean withHiddenColumns) throws Exception {
        try {
            String s = "".concat("SELECT GROUP_CONCAT(CONCAT('`',COLUMN_NAME,'`')) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '").concat(tableMst.getDataTableName()).concat("'");
            if (!withHiddenColumns) {
                s = s.concat(" AND COLUMN_NAME NOT LIKE '%").concat(ColumnsNamesEnum.DEFAULT_HIDDEN_COLUMN_PREFIX.toString()).concat("%'");
            }
            return jdbcTemplate.queryForObject(s, String.class);
        } catch (Exception ex) {
            throw new InvalidObjectException("Columns list fetch error.");
        }
    }

    public DataFrame addColumns(ColumnTypeEnum columnTypeEnum, String... names) throws Exception {
        return addColumns(columnTypeEnum, Arrays.asList(names));
    }

    private DataFrame addHiddenColumns(ColumnTypeEnum columnTypeEnum, String... names) throws Exception {
        List<String> collect = Arrays.stream(names).map(DFUtil::toHiddenColumnString).collect(Collectors.toList());
        return addColumns(columnTypeEnum, collect);
    }

    private DataFrame addColumns(ColumnTypeEnum columnTypeEnum, Collection<String> names) throws Exception {
        if (columnTypeEnum == null) {
            throw new NullPointerException("Column type null.");
        }
        if (names.isEmpty()) {
            throw new NullPointerException("Column list null.");
        }
        for (String name : names) {
            int update = jdbcTemplate.update("ALTER TABLE " + tableMst.getDataTableName() + " ADD COLUMN IF NOT EXISTS " + DFUtil.toColumnStringWithQuotes(name) + " " + columnTypeEnum.getColumnType());
            if (update != 0) {
                throw new RuntimeException(String.format("%s column can't add to dataframe.", name));
            }
        }
        return this;
    }

    private DataFrame alterForAggregateFunction(AggregateFunctionEnum aggregateFunctionEnum, String beforeColumnName) throws Exception {
        if (canCallAggregate) {
            pendingQuery = pendingQuery.replace("SELECT ", "SELECT " + aggregateFunctionEnum.toString() + "( " + beforeColumnName + " ");
            pendingQuery = pendingQuery.replace(pendingQuery.substring(pendingQuery.indexOf(" FROM ")), ") AS ".concat(DFUtil.toColumnStringWithQuotes(columnName)).concat(pendingQuery.substring(pendingQuery.indexOf(" FROM "))));
            return this;
        } else {
            throw new NotSupportedException("You must need to call single column select method instead of multiple column select method.");
        }
    }

    public DataFrame select(String... columns) throws Exception {
        return select(Arrays.asList(columns), false);
    }

    private DataFrame selectHiddenColumn(String... columns) throws Exception {
        List<String> collect = Arrays.asList(columns).contains("*") ? Arrays.asList(columns) : Arrays.stream(columns).map(s -> {
            if (!isColumnExist(s) && isColumnExist(DFUtil.toHiddenColumnString(s))) {
                return DFUtil.toHiddenColumnString(s);
            }
            return s;
        }).collect(Collectors.toList());
        return select(collect, true);
    }

    private DataFrame select(List<String> columns, boolean withHiddenColumns) throws Exception {
        if (!hasPendingQuery) {
            if (columns == null || columns.size() == 0) {
                throw new NullPointerException("Columns can't be empty.");
            }
            String sql;
            if (columns.contains("*")) {
                sql = "SELECT " + getAllColumnNames(withHiddenColumns) + " FROM " + tableMst.getDataTableName();
            } else {
                sql = "SELECT ";
                for (String column : columns) {
                    if (!column.trim().isEmpty()) {
                        if (isColumnExist(column)) {
                            sql = sql.concat(isHasAggregate(column) ? column : DFUtil.toColumnStringWithQuotes(column)).concat(",");
                        } else {
                            throw new NullPointerException("Can't find column in table.");
                        }
                    } else {
                        throw new NullPointerException("Column name empty.");
                    }
                }
                sql = sql.substring(0, sql.length() - 1).concat(" FROM " + tableMst.getDataTableName());
            }
            canCallAggregate = columns.size() == 1 && !columns.contains("*") && !isHasAggregate(sql);
            columnName = isColumnExist(columns.get(0)) && !isColumnExist(DFUtil.toHiddenColumnString(columns.get(0))) ? columns.get(0) : DFUtil.toHiddenColumnString(columns.get(0));
            pendingQuery = sql;
            hasPendingQuery = true;
            return this;
        } else {
            throw new DataFormatException("Pending query is exist.you need to call cleanPendingQuery() or execute pending query before create new query.");
        }
    }

    private boolean isHasAggregate(String s) {
        boolean has = false;
        for (AggregateFunctionEnum aggregateFunctionEnum : AggregateFunctionEnum.values()) {
            if (s.contains(aggregateFunctionEnum.toString()) && s.contains("(") && s.contains(")")) {
                has = true;
                break;
            }
        }
        return has;
    }

    public DataFrame update(DataMap<String, Object> updateColumnList) throws Exception {
        if (!hasPendingQuery) {
            if (updateColumnList == null || updateColumnList.size() == 0) {
                throw new NullPointerException("Update column list can't be empty.");
            }

            DataMap<String, Object> hiddenList = new DataMap<>();
            for (var entry : updateColumnList.entrySet()) {
                if (!isColumnExist(entry.getKey()) && isColumnExist(DFUtil.toHiddenColumnString(entry.getKey()))) {
                    hiddenList.put(DFUtil.toHiddenColumnString(entry.getKey()), entry.getValue());
                } else {
                    hiddenList.put(entry.getKey(), entry.getValue());
                }
            }

            updateColumnList = hiddenList;
            String sql = "UPDATE " + tableMst.getDataTableName() + " SET ";
            for (var entry : updateColumnList.entrySet()) {
                if (!entry.getKey().trim().isEmpty()) {
                    if (isColumnExist(entry.getKey())) {
                        sql = sql.concat(DFUtil.toColumnStringWithQuotes(entry.getKey())).concat("=").concat("'").concat(entry.getValue().toString()).concat("',");
                    } else {
                        throw new NullPointerException("Can't find column in table.");
                    }
                } else {
                    throw new NullPointerException("Column name empty.");
                }
            }
            sql = sql.substring(0, sql.length() - 1);
            pendingQuery = sql;
            hasPendingQuery = true;
            canCallAggregate = false;
            return this;
        } else {
            throw new DataFormatException("Pending query is exist.you need to call cleanPendingQuery() or execute pending query before create new query.");
        }
    }

    public boolean execute() {
        if (!StringUtils.isBlank(pendingQuery)) {
            replaceQueryConstantBeforeExecute();
            int update = jdbcTemplate.update(pendingQuery);
            cleanPendingQuery();
            return update > 0;
        }
        throw new NullPointerException("Query not found.");
    }

    public DataMap<String, Object> executeForSingleRowData() {
        List<DataMap<String, Object>> dataMaps = executeForData();
        return dataMaps.isEmpty() ? new DataMap<>() : dataMaps.get(0);
    }

    public List<DataMap<String, Object>> executeForData() {
        if (!StringUtils.isBlank(pendingQuery)) {

            List<DataMap<String, Object>> list = new ArrayList<>();

            replaceQueryConstantBeforeExecute();
            List<Map<String, Object>> maps = jdbcTemplate.queryForList(pendingQuery);
            cleanPendingQuery();

            maps.forEach(stringObjectMap -> {
                DataMap<String, Object> dataMap = new DataMap<>(String.CASE_INSENSITIVE_ORDER);
                dataMap.putAll(stringObjectMap);
                list.add(dataMap);
            });
            return list;
        }
        throw new NullPointerException("Query not found.");
    }

    public <T> T executeForSingleData(Class<T> returnType) {
        if (!StringUtils.isBlank(pendingQuery)) {
            if (!canCallAggregate) {
                throw new NullPointerException("Select single column before call aggregate function.");
            }
            validateClassType(returnType);
            replaceQueryConstantBeforeExecute();
            List<Map<String, Object>> maps = jdbcTemplate.queryForList(pendingQuery);
            if (maps.isEmpty()) {
                cleanPendingQuery();
                throw new EmptyResultDataAccessException("Empty data set.", 1);
            }
            T data = returnType.cast(maps.get(0).get(columnName.toUpperCase()));
            cleanPendingQuery();
            return data;
        }
        throw new NullPointerException("Query not found.");
    }

    public <T> List<T> executeForData(Class<T> returnType) {
        if (!StringUtils.isBlank(pendingQuery)) {
            validateClassType(returnType);
            replaceQueryConstantBeforeExecute();
            List<Map<String, Object>> maps = jdbcTemplate.queryForList(pendingQuery);
            if (maps.isEmpty()) {
                cleanPendingQuery();
                throw new EmptyResultDataAccessException("Empty data set.", 1);
            }
            List<T> list = new ArrayList<>();
            maps.forEach(stringObjectMap -> {
                list.add(returnType.cast(stringObjectMap.get(columnName.toUpperCase())));
            });
            cleanPendingQuery();
            return list;
        }
        throw new NullPointerException("Query not found.");
    }

    private void validateClassType(Class<?> returnType) {
        if (returnType == null || StringUtils.isBlank(returnType.getName())) {
            throw new NullPointerException("Return type can't be null.You must need to call single column select method instead of multiple column select method.");
        }
        if (StringUtils.isBlank(columnName) && canCallAggregate) {
            throw new NullPointerException("Select column can't be null.You must need to call single column select method instead of multiple column select method.");
        }
        for (ColumnCastTypeEnum columnCastTypeEnum : ColumnCastTypeEnum.values()) {
            if (columnCastTypeEnum.getCastType().equals(returnType)) {
                return;
            }
        }
        throw new ClassCastException("Invalid class type.Allowed only ColumnCastTypeEnum class types.");
    }

    public String executeForSingleData() {
        return DFUtil.convertToString(executeForSingleData(ColumnCastTypeEnum.OBJECT.getCastType()));
    }

    private String checkAndConvertColumn(String column) {
        if (!isColumnExist(column) && isColumnExist(DFUtil.toHiddenColumnString(column))) {
            column = DFUtil.toHiddenColumnString(column);
        }
        return column;
    }

    public String min(String column) throws Exception {
        return DFUtil.toAggFuncString(checkAndConvertColumn(column), AggregateFunctionEnum.MIN);
    }


    public DataFrame min() throws Exception {
        if (!canCallAggregate) {
            throw new NullPointerException("Select single column before call aggregate function.");
        }
        return alterForAggregateFunction(AggregateFunctionEnum.MIN, "");
    }

    public String max(String column) throws Exception {
        return DFUtil.toAggFuncString(checkAndConvertColumn(column), AggregateFunctionEnum.MAX);
    }

    public DataFrame max() throws Exception {
        if (!canCallAggregate) {
            throw new NullPointerException("Select single column before call aggregate function.");
        }
        return alterForAggregateFunction(AggregateFunctionEnum.MAX, "");
    }

    public String avg(String column) throws Exception {
        return DFUtil.toAggFuncString(checkAndConvertColumn(column), AggregateFunctionEnum.AVG);
    }

    public DataFrame avg() throws Exception {
        if (!canCallAggregate) {
            throw new NullPointerException("Select single column before call aggregate function.");
        }
        return alterForAggregateFunction(AggregateFunctionEnum.AVG, "");
    }

    public String count(String column) throws Exception {
        return DFUtil.toAggFuncString(checkAndConvertColumn(column), AggregateFunctionEnum.COUNT);
    }

    public DataFrame count() throws Exception {
        if (!canCallAggregate) {
            throw new NullPointerException("Select single column before call aggregate function.");
        }
        return alterForAggregateFunction(AggregateFunctionEnum.COUNT, "");
    }

    public DataFrame count(boolean isDistinct) throws Exception {
        if (!canCallAggregate) {
            throw new NullPointerException("Select single column before call aggregate function.");
        }
        return alterForAggregateFunction(AggregateFunctionEnum.COUNT, isDistinct ? "DISTINCT" : "");
    }

    public String sum(String column) throws Exception {
        return DFUtil.toAggFuncString(checkAndConvertColumn(column), AggregateFunctionEnum.SUM);
    }

    public DataFrame sum() throws Exception {
        if (!canCallAggregate) {
            throw new NullPointerException("Select single column before call aggregate function.");
        }
        return alterForAggregateFunction(AggregateFunctionEnum.SUM, "");
    }

    public Long getRowCount() throws Exception {
        return select(ColumnsNamesEnum.CANDLE_INDEX.toString()).max().executeForSingleData(Long.class);
    }

    public String dataToJson() throws Exception {
        return Util.OBJECT_MAPPER.writeValueAsString(select("*").executeForData());
    }

    public String tradeSignalToJson(TradeSignalEnum tradeSignalEnum) throws Exception {
        List<DataMap<String, Object>> maps = select(ColumnsNamesEnum.CANDLE_INDEX.toString()).where(Selection.create(ColumnsNamesEnum.TREAD_SIGNAL.toString(), WhereConditionEnum.EQUALS, tradeSignalEnum.getTradeSignal())).executeForData();
        List<String> list = new ArrayList<>();
        maps.forEach(stringObjectMap -> {
            list.add(stringObjectMap.getString(ColumnsNamesEnum.CANDLE_INDEX.toString()));
        });
        return Util.OBJECT_MAPPER.writeValueAsString(list);
    }

    public void printAll() throws Exception {
        printAll(false);
    }

    public void printAll(boolean printHiddenColumns) throws Exception {
        if (StringUtils.isBlank(pendingQuery)) {
            select(Collections.singletonList("*"), printHiddenColumns);
        }

        List<DataMap<String, Object>> dataMaps = executeForData();
        if (!dataMaps.isEmpty() && !dataMaps.get(0).isEmpty()) {
            String[][] finalArray = new String[dataMaps.size() + 1][dataMaps.get(0).size()];
            List<String> columnNames = new ArrayList<>();
            for (int i = 0; i < DFUtil.defaultTableColumns.length; i++) {
                if (dataMaps.get(0).containsKey(DFUtil.defaultTableColumns[i])) {
                    columnNames.add(DFUtil.defaultTableColumns[i]);
                }
            }
            columnNames.addAll(dataMaps.get(0).keySet());
            finalArray[0] = new ArrayList<>(new LinkedHashSet<>(columnNames)).toArray(String[]::new);
            for (int i = 0; i < dataMaps.size(); i++) {
                Map<String, Object> row = dataMaps.get(i);
                for (int j = 0; j < finalArray[0].length; j++) {
                    finalArray[i + 1][j] = DFUtil.convertToString(row.get(finalArray[0][j]));
                }
            }
            DFUtil.print(finalArray);
        } else {
            throw new EmptyResultDataAccessException("Data not found.", 1);
        }
    }

    public BarSeries getBarSeries() throws Exception {
        BarSeries series = new BaseBarSeriesBuilder().withName(coin).build();
        List<DataMap<String, Object>> dataMaps = select("*").executeForData();
        for (DataMap<String, Object> row : dataMaps) {
            series.addBar(
                    ZonedDateTime.ofInstant(row.getDate(ColumnsNamesEnum.DATE_AND_TIME.toString()).toInstant(), ZoneId.systemDefault()),
                    row.getString(ColumnsNamesEnum.OPEN.toString()),
                    row.getString(ColumnsNamesEnum.HIGH.toString()),
                    row.getString(ColumnsNamesEnum.LOW.toString()),
                    row.getString(ColumnsNamesEnum.CLOSE.toString()),
                    row.getString(ColumnsNamesEnum.VOLUME.toString())
            );
        }
        return series;
    }

    @Transactional
    public boolean dispose() {
        int update = 0;
        try {
            update = jdbcTemplate.update("drop table " + tableMst.getDataTableName());
        } catch (DataAccessException e) {
            throw new TransactionalException("Data table drop error.", e);
        }
        if (update > 0) {
            try {
                tableMstRepository.delete(tableMst);
                return true;
            } catch (Exception e) {
                throw new TransactionalException("Data table partially dropped,main table drop error.", e);
            }
        }
        return false;
    }

    public List<DataMap<String, Double>> getSupportAndResistance() throws Exception {
        List<DataMap<String, Double>> list = new ArrayList<>();
        List<DataMap<String, Object>> dataMaps = selectHiddenColumn("sAndR", "sAndRGap", "sAndRTouchCount").where(Selection.createForHiddenColumn("isSAndR", WhereConditionEnum.EQUALS, true)).groupBy("sAndR").executeForData();
        for (DataMap<String, Object> stringObjectDataMap : dataMaps) {
            DataMap<String, Double> dataMap = new DataMap<>();
            dataMap.put("levelValue", stringObjectDataMap.getHiddenDouble("sAndR"));
            dataMap.put("plusMinus", stringObjectDataMap.getHiddenDouble("sAndRGap"));
            dataMap.put("touchCount", stringObjectDataMap.getHiddenDouble("sAndRTouchCount"));
            list.add(dataMap);
        }
        return list;
    }

    public void viewChart() {
        try {
            BarSeries series = getBarSeries();
            OHLCDataset ohlcDataset = createOHLCDataset(series);
            TimeSeriesCollection xyDataset = createAdditionalDataset(series);
            JFreeChart chart = ChartFactory.createCandlestickChart("Bitstamp BTC price", "Time", "USD", ohlcDataset, true);
            CandlestickRenderer renderer = new CandlestickRenderer();
            renderer.setAutoWidthMethod(CandlestickRenderer.WIDTHMETHOD_SMALLEST);
            XYPlot plot = chart.getXYPlot();
            plot.setRenderer(renderer);
            int index = 1;
            plot.setDataset(index, xyDataset);
            plot.mapDatasetToRangeAxis(index, 0);
            XYLineAndShapeRenderer renderer2 = new XYLineAndShapeRenderer(true, false);
            renderer2.setSeriesPaint(index, Color.blue);
            plot.setRenderer(index, renderer2);
            plot.setRangeGridlinePaint(Color.lightGray);
            plot.setBackgroundPaint(Color.white);
            NumberAxis numberAxis = (NumberAxis) plot.getRangeAxis();
            numberAxis.setAutoRangeIncludesZero(false);
            plot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);
            List<DataMap<String, Double>> supportAndResistance = getSupportAndResistance();
            for (DataMap<String, Double> dataMap : supportAndResistance) {
                ValueMarker mark2 = new ValueMarker(dataMap.getDouble("levelValue"), Color.RED, new BasicStroke(1), Color.RED, null, 1f);
                plot.addRangeMarker(mark2);
            }
            displayChart(chart);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void displayChart(JFreeChart chart) {
        // Chart panel
        ChartPanel panel = new ChartPanel(chart);
        panel.setFillZoomRectangle(true);
        panel.setMouseWheelEnabled(true);
        panel.setPreferredSize(new Dimension(1024, 400));
        // Application frame
        ApplicationFrame frame = new ApplicationFrame("Ta4j example - Buy and sell signals to chart");
        frame.setContentPane(panel);
        frame.pack();
        RefineryUtilities.centerFrameOnScreen(frame);
        frame.setVisible(true);
    }

    private static OHLCDataset createOHLCDataset(BarSeries series) {
        final int nbBars = series.getBarCount();

        Date[] dates = new Date[nbBars];
        double[] opens = new double[nbBars];
        double[] highs = new double[nbBars];
        double[] lows = new double[nbBars];
        double[] closes = new double[nbBars];
        double[] volumes = new double[nbBars];

        for (int i = 0; i < nbBars; i++) {
            Bar bar = series.getBar(i);
            dates[i] = new Date(bar.getEndTime().toEpochSecond() * 1000);
            opens[i] = bar.getOpenPrice().doubleValue();
            highs[i] = bar.getHighPrice().doubleValue();
            lows[i] = bar.getLowPrice().doubleValue();
            closes[i] = bar.getClosePrice().doubleValue();
            volumes[i] = bar.getVolume().doubleValue();
        }

        return new DefaultHighLowDataset("btc", dates, highs, lows, opens, closes, volumes);
    }

    private static TimeSeriesCollection createAdditionalDataset(BarSeries series) {
        ClosePriceIndicator indicator = new ClosePriceIndicator(series);
        TimeSeriesCollection dataset = new TimeSeriesCollection();
        org.jfree.data.time.TimeSeries chartTimeSeries = new org.jfree.data.time.TimeSeries("Btc price");
        for (int i = 0; i < series.getBarCount(); i++) {
            Bar bar = series.getBar(i);
            chartTimeSeries.add(new Second(new Date(bar.getEndTime().toEpochSecond() * 1000)),
                    indicator.getValue(i).doubleValue());
        }
        dataset.addSeries(chartTimeSeries);
        return dataset;
    }

    private void calculateSupportAndResistance() throws Exception {
        final List<DataMap<String, Object>> rows = selectHiddenColumn("*").where(Selection.limitLast(2)).executeForData();
        if (null != rows && rows.size() == 2) {
            try {
                DataMap<String, Object> currentRow = rows.get(0);
                DataMap<String, Object> previousRow = rows.get(1);

                final double avg = (currentRow.getDouble(ColumnsNamesEnum.HIGH.toString()) + currentRow.getDouble(ColumnsNamesEnum.LOW.toString())) / 2;
                final double tr = tr(currentRow, previousRow);
                currentRow.putHidden("tr", tr);
                update(currentRow).where(Selection.create(ColumnsNamesEnum.CANDLE_INDEX.toString(), WhereConditionEnum.EQUALS, currentRow.get(ColumnsNamesEnum.CANDLE_INDEX.toString()).toString())).execute();

                if (getRowCount() > SARCalPeriod) {
                    final double atr = Util.findAverage(atr(SARCalPeriod), tr);
                    final double upperBand = avg + (SARCalAtrMultiplier * atr);
                    final double lowerBand = avg - (SARCalAtrMultiplier * atr);

                    currentRow.putHidden("upperBand", upperBand);
                    currentRow.putHidden("lowerBand", lowerBand);
                    currentRow.putHidden("isUpTrend", setIsUpTrend(currentRow, previousRow));
                    currentRow.putHidden("isReversal", previousRow.getHiddenBoolean("isUpTrend") != currentRow.getHiddenBoolean("isUpTrend"));
                    update(currentRow).where(Selection.create(ColumnsNamesEnum.CANDLE_INDEX.toString(), WhereConditionEnum.EQUALS, currentRow.get(ColumnsNamesEnum.CANDLE_INDEX.toString()).toString())).execute();

                    List<DataMap<String, Object>> reversals = selectHiddenColumn(ColumnsNamesEnum.CANDLE_INDEX.toString(), "isUpTrend").where(Selection.createForHiddenColumn("isReversal", WhereConditionEnum.EQUALS, true).and(Selection.limitLast(2))).executeForData();
                    if (Objects.nonNull(reversals) && reversals.size() == 2) {
                        currentRow = reversals.get(0);
                        previousRow = reversals.get(1);
                        if (currentRow.getHiddenBoolean("isUpTrend")) {
                            final DataMap<String, Object> minLowList = select(min(ColumnsNamesEnum.OPEN.toString()), min(ColumnsNamesEnum.CLOSE.toString())).where(Selection.between(previousRow.getLong(ColumnsNamesEnum.CANDLE_INDEX.toString()).intValue(), currentRow.getLong(ColumnsNamesEnum.CANDLE_INDEX.toString()).intValue())).executeForSingleRowData();
                            if (null != minLowList) {
                                double minLow = Util.findMin(minLowList.getDouble(ColumnsNamesEnum.OPEN.toString()), minLowList.getDouble(ColumnsNamesEnum.CLOSE.toString()));
                                final DataMap<String, Object> minDataMaps = selectHiddenColumn(ColumnsNamesEnum.CANDLE_INDEX.toString(), ColumnsNamesEnum.CLOSE.toString(), ColumnsNamesEnum.OPEN.toString(), "lowerBand").where(Selection.between(previousRow.getLong(ColumnsNamesEnum.CANDLE_INDEX.toString()).intValue(), currentRow.getLong(ColumnsNamesEnum.CANDLE_INDEX.toString()).intValue()).and(Selection.create(ColumnsNamesEnum.OPEN.toString(), WhereConditionEnum.EQUALS, minLow).or(Selection.create(ColumnsNamesEnum.CLOSE.toString(), WhereConditionEnum.EQUALS, minLow)))).executeForSingleRowData();
                                final double candleMinBeforeLow = Util.findMin(minDataMaps.getDouble(ColumnsNamesEnum.CLOSE.toString()), minDataMaps.getDouble(ColumnsNamesEnum.OPEN.toString()));
                                final double minAvg = Util.findAverage(candleMinBeforeLow, minDataMaps.getHiddenDouble("lowerBand"));
                                final double minGap = Util.findDiff(minAvg, minDataMaps.getHiddenDouble("lowerBand"));
                                minDataMaps.putHidden("sAndR", minAvg);
                                minDataMaps.putHidden("sAndRGap", minGap);
                                minDataMaps.putHidden("isSAndR", true);
                                update(minDataMaps).where(Selection.create(ColumnsNamesEnum.CANDLE_INDEX.toString(), WhereConditionEnum.EQUALS, minDataMaps.getInteger(ColumnsNamesEnum.CANDLE_INDEX.toString()))).execute();
                            }
                        } else {
                            final DataMap<String, Object> maxHighList = select(max(ColumnsNamesEnum.OPEN.toString()), max(ColumnsNamesEnum.CLOSE.toString())).where(Selection.between(previousRow.getLong(ColumnsNamesEnum.CANDLE_INDEX.toString()).intValue(), currentRow.getLong(ColumnsNamesEnum.CANDLE_INDEX.toString()).intValue())).executeForSingleRowData();
                            if (null != maxHighList) {
                                double maxHigh = Util.findMax(maxHighList.getDouble(ColumnsNamesEnum.OPEN.toString()), maxHighList.getDouble(ColumnsNamesEnum.CLOSE.toString()));
                                final DataMap<String, Object> maxDataMaps = selectHiddenColumn(ColumnsNamesEnum.CANDLE_INDEX.toString(), ColumnsNamesEnum.CLOSE.toString(), ColumnsNamesEnum.OPEN.toString(), "upperBand").where(Selection.between(previousRow.getLong(ColumnsNamesEnum.CANDLE_INDEX.toString()).intValue(), currentRow.getLong(ColumnsNamesEnum.CANDLE_INDEX.toString()).intValue()).and(Selection.create(ColumnsNamesEnum.OPEN.toString(), WhereConditionEnum.EQUALS, maxHigh).or(Selection.create(ColumnsNamesEnum.CLOSE.toString(), WhereConditionEnum.EQUALS, maxHigh)))).executeForSingleRowData();
                                final double candleMaxAfterHigh = Util.findMax(maxDataMaps.getDouble(ColumnsNamesEnum.CLOSE.toString()), maxDataMaps.getDouble(ColumnsNamesEnum.OPEN.toString()));
                                final double maxAvg = Util.findAverage(candleMaxAfterHigh, maxDataMaps.getHiddenDouble("upperBand"));
                                final double maxGap = Util.findDiff(maxAvg, maxDataMaps.getHiddenDouble("upperBand"));
                                maxDataMaps.putHidden("sAndR", maxAvg);
                                maxDataMaps.putHidden("sAndRGap", maxGap);
                                maxDataMaps.putHidden("isSAndR", true);
                                update(maxDataMaps).where(Selection.create(ColumnsNamesEnum.CANDLE_INDEX.toString(), WhereConditionEnum.EQUALS, maxDataMaps.getInteger(ColumnsNamesEnum.CANDLE_INDEX.toString()))).execute();
                            }
                        }
                    }
                    while (true) {
                        boolean canBreak = true;
                        final List<DataMap<String, Object>> sAndRLevels = selectHiddenColumn("sAndR", "sAndRGap").where(Selection.createForHiddenColumn("isSAndR", WhereConditionEnum.EQUALS, true)).groupBy("sAndR").executeForData();
                        for (DataMap<String, Object> dataMap : sAndRLevels) {
                            Double sAndR = dataMap.getHiddenDouble("sAndR");
                            Double sAndRGap = dataMap.getHiddenDouble("sAndRGap");
                            double upLevel = sAndR + sAndRGap;
                            double downLevel = sAndR - sAndRGap;
                            Long count = selectHiddenColumn("sAndR").count(true).where(Selection.createForHiddenColumn("sAndR", WhereConditionEnum.BETWEEN, downLevel, upLevel).and(Selection.createForHiddenColumn("isSAndR", WhereConditionEnum.EQUALS, true))).executeForSingleData(Long.class);
                            if (count > 1) {
                                List<Long> ids = select(ColumnsNamesEnum.CANDLE_INDEX.toString()).where(Selection.createForHiddenColumn("sAndR", WhereConditionEnum.BETWEEN, downLevel, upLevel).and(Selection.createForHiddenColumn("isSAndR", WhereConditionEnum.EQUALS, true))).executeForData(Long.class);
                                DataMap<String, Object> avgs = select(avg("sAndR"), avg("sAndRGap")).where(Selection.in(ids)).executeForSingleRowData();
                                update(avgs).where(Selection.in(ids)).execute();
                                canBreak = false;
                            }
                        }
                        if (canBreak) {
                            break;
                        }
                    }

                    final List<DataMap<String, Object>> sAndRLevels = selectHiddenColumn(ColumnsNamesEnum.CANDLE_INDEX.toString(), "sAndR", "sAndRGap").where(Selection.createForHiddenColumn("isSAndR", WhereConditionEnum.EQUALS, true)).executeForData();
                    for (DataMap<String, Object> dataMap : sAndRLevels) {
                        Long countMax = select(ColumnsNamesEnum.CANDLE_INDEX.toString()).count().where(Selection.create(ColumnsNamesEnum.HIGH.toString(), WhereConditionEnum.BETWEEN, dataMap.getHiddenDouble("sAndR") - dataMap.getHiddenDouble("sAndRGap"), dataMap.getHiddenDouble("sAndR") + dataMap.getHiddenDouble("sAndRGap"))).executeForSingleData(Long.class);
                        Long countMin = select(ColumnsNamesEnum.CANDLE_INDEX.toString()).count().where(Selection.create(ColumnsNamesEnum.LOW.toString(), WhereConditionEnum.BETWEEN, dataMap.getHiddenDouble("sAndR") - dataMap.getHiddenDouble("sAndRGap"), dataMap.getHiddenDouble("sAndR") + dataMap.getHiddenDouble("sAndRGap"))).executeForSingleData(Long.class);

                        dataMap.putHidden("sAndRTouchCount", (int) Util.findSum(countMax, countMin));
                        update(dataMap).where(Selection.create(ColumnsNamesEnum.CANDLE_INDEX.toString(), WhereConditionEnum.EQUALS, dataMap.getInteger(ColumnsNamesEnum.CANDLE_INDEX.toString()))).execute();
                    }
                }
            } catch (EmptyResultDataAccessException ignored) {
            } catch (Exception e) {
                throw new RuntimeException("Support and resistance calculation error.");
            }
        }
    }

    private double tr(DataMap<String, Object> row, DataMap<String, Object> prevRow) {
        return Util.findMax(Math.abs(row.getDouble(ColumnsNamesEnum.HIGH.toString()) - row.getDouble(ColumnsNamesEnum.LOW.toString())), Math.abs(row.getDouble(ColumnsNamesEnum.HIGH.toString()) - prevRow.getDouble(ColumnsNamesEnum.CLOSE.toString())), Math.abs(row.getDouble(ColumnsNamesEnum.LOW.toString()) - prevRow.getDouble(ColumnsNamesEnum.CLOSE.toString())));
    }

    private double atr(int period) throws Exception {
        return selectHiddenColumn("tr").avg().where(Selection.last(period)).executeForSingleData(BigDecimal.class).doubleValue();
    }

    private boolean setIsUpTrend(DataMap<String, Object> row, DataMap<String, Object> prevRow) {
        if (row.getDouble(ColumnsNamesEnum.CLOSE.toString()) > prevRow.getHiddenDouble("upperBand")) {
            return true;
        } else if (row.getDouble(ColumnsNamesEnum.CLOSE.toString()) < prevRow.getHiddenDouble("lowerband")) {
            return false;
        } else {
            if (prevRow.getHiddenBoolean("isUpTrend") && row.getHiddenDouble("lowerband") < prevRow.getHiddenDouble("lowerband")) {
                row.putHidden("lowerBand", prevRow.getHiddenDouble("lowerband"));
            }
            if (!prevRow.getHiddenBoolean("isUpTrend") && row.getHiddenDouble("upperBand") > prevRow.getHiddenDouble("upperBand")) {
                row.putHidden("upperBand", prevRow.getHiddenDouble("upperBand"));
            }
            return prevRow.getHiddenBoolean("isUpTrend");
        }
    }
}
