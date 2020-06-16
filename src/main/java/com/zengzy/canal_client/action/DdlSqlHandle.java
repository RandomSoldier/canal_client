package com.zengzy.canal_client.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.zengzy.canal_client.model.DdlReturn;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.commons.lang.StringUtils;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: canal_client
 * @description: 解析DDL类SQL生成对应语句
 * @author: zengzy
 * @create: 2020-06-16 14:23
 **/

public class DdlSqlHandle {
    String sql = "";
    String schemaName = "";
    String tableName = "";
    CanalEntry.EventType eventType = null;

    public DdlSqlHandle(CanalEntry.EventType eventType, String sql, String schemaName, String tableName) {
        this.eventType = eventType;
        this.sql = sql;
        this.schemaName = schemaName;
        this.tableName = tableName;

    }

    public String main() throws JSQLParserException {
        sql = DdlSqlHandle(eventType, sql, schemaName, tableName);
        return sql;
    }

    protected static String DdlSqlHandle(CanalEntry.EventType eventType, String sql, String schemaName, String tableName) throws JSQLParserException {

        if (sql.contains("USING BTREE")) {
            sql = sql.replaceAll("USING BTREE", "");
        }

        StringBuilder builder = new StringBuilder();

        List<DdlReturn> ddlReturnList = getColumnBySql(sql);
        for (DdlReturn ddlReturn : ddlReturnList) {
            Map<String, String> columnList = ddlReturn.map;
            String statement = ddlReturn.statement;
            String operation = ddlReturn.operation;

            if (statement.equals("CreateTable")) {  // 新增表
                builder.append("CREATE TABLE IF NOT EXISTS `").append(schemaName).append("_history").append("`").append(".").append("`").append(tableName).append("` ");
                builder.append("(`id` bigint(20) NOT NULL AUTO_INCREMENT,");
                builder.append("`type` varchar(10),");
                builder.append("`es` bigint(20),");
                builder.append("`ts` bigint(20), ");

                for (Map.Entry<String, String> entry : columnList.entrySet()) {
                    String columnName = entry.getKey();
                    if (columnName.contains("`")) {
                        columnName = columnName.replaceAll("`", "");
                        columnName = "`h_".concat(columnName).concat("`");
                    } else {
                        columnName = "h_".concat(columnName);
                    }
                    String colDataType = entry.getValue();
                    builder.append(columnName).append(" ").append(colDataType).append(", ");
                }
                builder.append("PRIMARY KEY (`id`));");
            } else if (statement.equals("Alter")) {  // 修改表
                if (operation.equals("ADD") || operation.equals("MODIFY")) { // 增加字段  // 修改字段
                    builder.append("ALTER TABLE `").append(schemaName).append("_history").append("`").append(".").append("`").append(tableName).append("` ").append(operation).append(" COLUMN ");
                    for (Map.Entry<String, String> entry : columnList.entrySet()) {
                        String columnName = entry.getKey();
                        if (columnName.contains("`")) {
                            columnName = columnName.replaceAll("`", "");
                            columnName = "`h_".concat(columnName).concat("`");
                        } else {
                            columnName = "h_".concat(columnName);
                        }
                        String colDataType = entry.getValue();
                        builder.append(columnName).append(" ").append(colDataType).append(" , ").append(operation).append(" COLUMN ");
                    }
                    builder.delete(builder.lastIndexOf(","), builder.length()).append(";\n");
                } else if (operation.equals("CHANGE")) { // 字段改名
                    builder.append("ALTER TABLE `").append(schemaName).append("_history").append("`").append(".").append("`").append(tableName).append("` CHANGE ");
                    for (Map.Entry<String, String> entry : columnList.entrySet()) {
                        String columnOldName = StringUtils.substringBefore(entry.getKey(), "|");
                        if (columnOldName.contains("`")) {
                            columnOldName = columnOldName.replaceAll("`", "");
                            columnOldName = "`h_".concat(columnOldName).concat("`");
                        } else {
                            columnOldName = "h_".concat(columnOldName);
                        }
                        String columnName = StringUtils.substringAfter(entry.getKey(), "|");
                        if (columnName.contains("`")) {
                            columnName = columnName.replaceAll("`", "");
                            columnName = "`h_".concat(columnName).concat("`");
                        } else {
                            columnName = "h_".concat(columnName);
                        }
                        String colDataType = entry.getValue();
                        builder.append(columnOldName).append(" ").append(columnName).append(" ").append(colDataType).append(" , CHANGE ");
                    }
                    builder.delete(builder.lastIndexOf(","), builder.length()).append(";\n");

                } else { // 舍弃剩余操作，例如删除字段,修改索引等操作
                }
            } else {
            }
        }
        return builder.toString();
    }

    public static List<DdlReturn> getColumnBySql(String sql) throws JSQLParserException {

        CCJSqlParserManager parser = new CCJSqlParserManager();
        Statement statement = parser.parse(new StringReader(sql));

        List columnList = null;
        List AlterExpressions = null;
        String empstatement = "";
        String empoperation = "";
        Map<String, String> map = new LinkedHashMap<>();

        DdlReturn ddlReturn = new DdlReturn();
        List<DdlReturn> ddlReturnList = new ArrayList<>();

        if (statement instanceof CreateTable) {
            empstatement = "CreateTable";
            columnList = ((CreateTable) statement).getColumnDefinitions();
            for (int i = 0; i < columnList.size(); i++) {
                ColumnDefinition columnDefinition = (ColumnDefinition) columnList.get(i);
                map.put(columnDefinition.getColumnName(), columnDefinition.getColDataType().toString());
            }
            ddlReturn = new DdlReturn();
            ddlReturn.statement = empstatement;
            ddlReturn.operation = empoperation;
            ddlReturn.map = map;
            ddlReturnList.add(ddlReturn);
        } else if (statement instanceof Alter) {
            empstatement = "Alter";
            AlterExpressions = ((Alter) statement).getAlterExpressions();
            for (int i = 0; i < AlterExpressions.size(); i++) {
                columnList = ((Alter) statement).getAlterExpressions().get(i).getColDataTypeList();
                empoperation = ((Alter) statement).getAlterExpressions().get(i).getOperation().toString();
                String columnOldName = ((Alter) statement).getAlterExpressions().get(i).getColOldName();
                if (columnList != null && !columnList.isEmpty()) {
                    for (int j = 0; j < columnList.size(); j++) {
                        map = new LinkedHashMap<>();
                        AlterExpression.ColumnDataType columnDataType = (AlterExpression.ColumnDataType) columnList.get(j);
                        if (columnOldName == null) {
                            map.put(columnDataType.getColumnName(), columnDataType.getColDataType().toString());
                        } else {
                            map.put(columnOldName + "|" + columnDataType.getColumnName(), columnDataType.getColDataType().toString());
                        }
                        ddlReturn = new DdlReturn();
                        ddlReturn.statement = empstatement;
                        ddlReturn.operation = empoperation;
                        ddlReturn.map = map;
                        ddlReturnList.add(ddlReturn);
                    }
                } else {
                }
            }
        } else {
        }
        return ddlReturnList;
    }
}