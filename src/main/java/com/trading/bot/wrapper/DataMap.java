package com.trading.bot.wrapper;

import com.trading.bot.util.DFUtil;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Date;
import java.util.TreeMap;

public class DataMap<K, V> extends TreeMap<String, V> {

    private static final long serialVersionUID = -609643336632818694L;

    DataMap(Comparator<? super String> comparator) {
        super(comparator);
    }

    public DataMap() {
        super();
    }

    public BigDecimal getBigDecimal(String key) {
        return get(key) == null ? null : (BigDecimal) get(key);
    }

    public BigDecimal getHiddenBigDecimal(String key) {
        return getBigDecimal(DFUtil.toHiddenColumnString(key));
    }

    public Double getDouble(String key) {
        return get(key) == null ? null : Double.parseDouble(get(key).toString());
    }

    public Double getHiddenDouble(String key) {
        return getDouble(DFUtil.toHiddenColumnString(key));
    }

    public Long getLong(String key) {
        return get(key) == null ? null : Long.parseLong(get(key).toString());
    }

    public Long getHiddenLong(String key) {
        return getLong(DFUtil.toHiddenColumnString(key));
    }

    public Integer getInteger(String key) {
        return get(key) == null ? null : Integer.parseInt(get(key).toString());
    }

    public Integer getHiddenInteger(String key) {
        return getInteger(DFUtil.toHiddenColumnString(key));
    }

    public Boolean getBoolean(String key) {
        return get(key) == null ? null : Boolean.parseBoolean(get(key).toString());
    }

    public Boolean getHiddenBoolean(String key) {
        return getBoolean(DFUtil.toHiddenColumnString(key));
    }

    public Date getDate(String key) {
        return get(key) == null ? null : (Date) get(key);
    }

    public Date getHiddenDate(String key) {
        return getDate(DFUtil.toHiddenColumnString(key));
    }

    public String getString(String key) {
        return get(key) == null ? null : get(key).toString();
    }

    public String getHiddenString(String key) {
        return getString(DFUtil.toHiddenColumnString(key));
    }


    @Override
    public V put(String key, V value) {
        return super.put(key, value);
    }

    public V putHidden(String key, V value) {
        return put(DFUtil.toHiddenColumnString(key), value);
    }
}
