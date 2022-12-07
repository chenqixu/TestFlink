package com.cqx.examples.test.sql;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TableFactory
 *
 * @author chenqixu
 */
public class TableFactory {
    private static final TableFactory tableFactory = new TableFactory();
    private List<Row> rowList = new ArrayList<>();
    private List<String> fieldList = new ArrayList<>();
    private Map<String, Table> namedCustomersMap = new HashMap<>();
    private TableEnvironment tableEnv;

    private TableFactory() {
    }

    public static TableFactory getInstance() {
        return tableFactory;
    }

    public TableFactory newTable() {
        rowList = new ArrayList<>();
        fieldList = new ArrayList<>();
        return this;
    }

    public TableFactory addField(String fieldName) {
        fieldList.add(fieldName);
        return this;
    }

    public TableFactory addRow(Row row) {
        rowList.add(row);
        return this;
    }

    public TableFactory setTableEnv(TableEnvironment tableEnv) {
        this.tableEnv = tableEnv;
        return this;
    }

    public TableFactory buildTable(String tableName) {
        if (this.tableEnv == null) throw new NullPointerException("请设置环境变量！");
        Table rawCustomers = tableEnv.fromValues(rowList);
        Table truncatedCustomers = rawCustomers.select(Expressions.withColumns(Expressions.range(1, fieldList.size())));
        Table namedCustomers = null;
        if (fieldList.size() > 1) {
            String[] strs = new String[fieldList.size() - 1];
            for (int i = 1; i < fieldList.size(); i++) {
                strs[i - 1] = fieldList.get(i);
            }
            namedCustomers = truncatedCustomers.as(fieldList.get(0), strs);
        } else if (fieldList.size() == 1) {
            namedCustomers = truncatedCustomers.as(fieldList.get(0));
        }
        if (namedCustomers != null) namedCustomersMap.put(tableName, namedCustomers);
        return this;
    }

    public Map<String, Table> getNamedCustomersMap() {
        return namedCustomersMap;
    }
}
