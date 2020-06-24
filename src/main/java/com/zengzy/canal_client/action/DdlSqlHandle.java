package com.zengzy.canal_client.action;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.zengzy.canal_client.model.DdlReturn;
import org.apache.commons.lang.StringUtils;

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

    public String main(){
        sql = DdlSqlHandle(eventType, sql, schemaName, tableName);
        return sql;
    }

    protected String DdlSqlHandle(CanalEntry.EventType eventType, String sql, String schemaName, String tableName) {
        sql = sql.replaceAll("//.*|/\\*[\\s\\S]*?\\*/|(\"(\\\\.|[^\"])*\")", "");  // 替换掉SQL中的注释部分，避免影响解析(多行注释)
        sql = sql.replaceAll("--[^\\r\\n]*","");  // 替换掉SQL中的注释部分，避免影响解析（单行注释）
        StringBuilder builder = new StringBuilder();
        List<DdlReturn> ddlReturnList = getColumnBySql(sql); // 获取对象的列及属性,如果为新增库及删除库及表，则存储表名字
        for (DdlReturn ddlReturn : ddlReturnList) {
            Map<String, String> columnList = ddlReturn.map;
            String statement = ddlReturn.statement;
            String operation = ddlReturn.operation;
            if (statement.equals("CREATE DATABASE")) {
                builder.append("CREATE DATABASE IF NOT EXISTS ");
                for (Map.Entry<String, String> entry : columnList.entrySet()) {
                    String name = entry.getKey();
                    name = modifyName(name, "database");
                    String characterSet = StringUtils.substringBefore(entry.getValue(), "|");
                    String collate = StringUtils.substringAfter(entry.getValue(), "|");
                    builder.append(name).append(" DEFAULT CHARACTER SET ").append(characterSet).append(" DEFAULT COLLATE ").append(collate).append(" ;");
                }
            } else if (statement.equals("CREATE TABLE")) {  // 新增表
                builder.append("CREATE TABLE IF NOT EXISTS `").append(schemaName).append("_history").append("`").append(".").append("`").append(tableName).append("` ");
                builder.append("(`id` bigint(20) NOT NULL AUTO_INCREMENT,");
                builder.append("`type` varchar(10),");
                builder.append("`es` bigint(20),");
                builder.append("`ts` bigint(20),");

                for (Map.Entry<String, String> entry : columnList.entrySet()) {
                    String columnName = entry.getKey();
                    columnName = modifyName(columnName,"column");
                    String colDataType = entry.getValue();
                    builder.append(columnName).append(" ").append(colDataType).append(", ");
                }
                builder.append("PRIMARY KEY (`id`));\n");
            } else if (statement.equals("ALTER TABLE")) {  // 修改表
                if (operation.equalsIgnoreCase("ADD COLUMN") || operation.equals("MODIFY COLUMN")) { // 增加字段  // 修改字段
                    builder.append("ALTER TABLE `").append(schemaName).append("_history").append("`").append(".").append("`").append(tableName).append("` ").append(operation).append(" ");
                    for (Map.Entry<String, String> entry : columnList.entrySet()) {
                        String columnName = entry.getKey();
                        columnName = modifyName(columnName,"column");
                        String colDataType = entry.getValue();
                        builder.append(columnName).append(" ").append(colDataType).append(" , ").append(operation).append(" ");
                    }
                    builder.delete(builder.lastIndexOf(","), builder.length()).append(";\n");
                } else if (operation.equalsIgnoreCase("CHANGE COLUMN")) { // 字段改名
                    builder.append("ALTER TABLE `").append(schemaName).append("_history").append("`").append(".").append("`").append(tableName).append("` ").append(operation).append(" ");
                    for (Map.Entry<String, String> entry : columnList.entrySet()) {
                        String columnOldName = StringUtils.substringBefore(entry.getKey(), "|");
                        columnOldName = modifyName(columnOldName,"column");
                        String columnName = StringUtils.substringAfter(entry.getKey(), "|");
                        columnName = modifyName(columnName,"column");
                        String colDataType = entry.getValue();
                        builder.append(columnOldName).append(" ").append(columnName).append(" ").append(colDataType).append(" , ").append(operation).append(" ");
                    }
                    builder.delete(builder.lastIndexOf(","), builder.length()).append(";\n");

                } else { // 舍弃剩余操作，例如删除字段,修改索引等操作
                }
            }else if(statement.equals("RENAME")){
                builder.append("RENAME ");
                for (Map.Entry<String, String> entry : columnList.entrySet()) {
                    String name = entry.getKey();
                    String to = entry.getValue();
                    schemaName = modifyName(schemaName,"database");
                    builder.append(operation).append(" ").append(schemaName).append(".").append(name).append(" TO ").append(schemaName).append(".").append(to).append(" ;\n");
                }
            } else if (statement.equals("DROP")) {  // 删除类，仅处理删除库及删除表
                builder.append("DROP ");
                for (Map.Entry<String, String> entry : columnList.entrySet()) {
                    String dropName = entry.getKey();
                    if (operation.equalsIgnoreCase("database") || operation.equalsIgnoreCase("schema")) {
                        dropName = modifyName(dropName,"database");
                        builder.append(operation).append(" IF EXISTS ").append(dropName).append(" ;");
                    }else{
                        schemaName=modifyName(schemaName,"database");
                        builder.append(operation).append(" IF EXISTS ").append(schemaName).append(".").append(dropName).append(" ;");
                    }
                }
            } else {
            }
        }
        return builder.toString();
    }

    public static  List<DdlReturn> getColumnBySql(String sql) {
        List columnList = null;
        List AlterExpressions = null;
        String empstatement = "";
        String empoperation = "";
        Map<String, String> map = null;

        DdlReturn ddlReturn = null;
        List<DdlReturn> ddlReturnList = new ArrayList<>();
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL,true);

        for (SQLStatement statement : statementList) {
            if (statement instanceof SQLCreateDatabaseStatement) { // 如果是创建库脚本则生成带后缀的库名称
                empstatement = "CREATE DATABASE";
                String name = ((SQLCreateDatabaseStatement) statement).getName().getSimpleName();
                String characterSet = ((SQLCreateDatabaseStatement) statement).getCharacterSet();
                String collate = ((SQLCreateDatabaseStatement) statement).getCollate();
                if (characterSet == null || characterSet.length() <= 0) {
                    characterSet = "utf8mb4";
                }
                if (collate == null || collate.length() <= 0) {
                    collate = "utf8mb4_general_ci";
                }
                map = new LinkedHashMap<>();
                map.put(name, characterSet + "|" + collate);
                ddlReturn = new DdlReturn();
                ddlReturn.setDdlReturn(empstatement,empoperation,map);
                ddlReturnList.add(ddlReturn);
            } else if (statement instanceof MySqlCreateTableStatement) {
                empstatement = "CREATE TABLE";
                columnList = ((MySqlCreateTableStatement) statement).getTableElementList();
                map = new LinkedHashMap<>();
                for (int i = 0; i < columnList.size(); i++) {
                    if (columnList.get(i) instanceof SQLColumnDefinition) {
                        SQLColumnDefinition sqlColumnDefinition = (SQLColumnDefinition) columnList.get(i);
                        map.put(sqlColumnDefinition.getName().toString(), sqlColumnDefinition.getDataType().toString());
                    }
                }
                ddlReturn = new DdlReturn();
                ddlReturn.setDdlReturn(empstatement,empoperation,map);
                ddlReturnList.add(ddlReturn);
            } else if (statement instanceof SQLAlterTableStatement) {
                empstatement = "ALTER TABLE";
                AlterExpressions = ((SQLAlterTableStatement) statement).getItems();
                for (int i = 0; i < AlterExpressions.size(); i++) {
                    if (AlterExpressions.get(i) instanceof SQLAlterTableAddColumn) {
                        empoperation = "ADD COLUMN";
                        columnList = ((SQLAlterTableAddColumn) AlterExpressions.get(i)).getColumns();
                        if (columnList != null && !columnList.isEmpty()) {
                            for (int j = 0; j < columnList.size(); j++) {
                                SQLColumnDefinition sqlColumnDefinition = (SQLColumnDefinition) columnList.get(j);
                                map = new LinkedHashMap<>();
                                map.put(sqlColumnDefinition.getName().toString(), sqlColumnDefinition.getDataType().toString());
                                ddlReturn = new DdlReturn();
                                ddlReturn.setDdlReturn(empstatement,empoperation,map);
                                ddlReturnList.add(ddlReturn);
                            }
                        }
                    } else if (AlterExpressions.get(i) instanceof MySqlAlterTableChangeColumn) {
                        empoperation = "CHANGE COLUMN";
                        String columnOldName = ((MySqlAlterTableChangeColumn) AlterExpressions.get(i)).getColumnName().getSimpleName();
                        SQLColumnDefinition sqlColumnDefinition = ((MySqlAlterTableChangeColumn) AlterExpressions.get(i)).getNewColumnDefinition();
                        map = new LinkedHashMap<>();
                        map.put(columnOldName + "|" + sqlColumnDefinition.getName().toString(), sqlColumnDefinition.getDataType().toString());
                        ddlReturn = new DdlReturn();
                        ddlReturn.setDdlReturn(empstatement,empoperation,map);
                        ddlReturnList.add(ddlReturn);
                    } else if (AlterExpressions.get(i) instanceof MySqlAlterTableModifyColumn) {
                        empoperation = "MODIFY COLUMN";
                        SQLColumnDefinition sqlColumnDefinition = ((MySqlAlterTableModifyColumn) AlterExpressions.get(i)).getNewColumnDefinition();
                        map = new LinkedHashMap<>();
                        map.put(sqlColumnDefinition.getName().toString(), sqlColumnDefinition.getDataType().toString());
                        ddlReturn = new DdlReturn();
                        ddlReturn.setDdlReturn(empstatement,empoperation,map);
                        ddlReturnList.add(ddlReturn);
                    }
                }
            }else if(statement instanceof MySqlRenameTableStatement){
                empstatement = "RENAME";
                empoperation = "TABLE";
                List<MySqlRenameTableStatement.Item> renameTable = ((MySqlRenameTableStatement) statement).getItems();
                for (int i = 0; i < renameTable.size(); i++) {
                    String name = renameTable.get(i).getName().getSimpleName();
                    String to = renameTable.get(i).getTo().getSimpleName();
                    map = new LinkedHashMap<>();
                    map.put( name,to);
                    ddlReturn = new DdlReturn();
                    ddlReturn.setDdlReturn(empstatement,empoperation,map);
                    ddlReturnList.add(ddlReturn);
                }
            } else if (statement instanceof SQLDropTableStatement) {
                empstatement = "DROP";
                empoperation = "TABLE";
                List<SQLExprTableSource> DropTableList = ((SQLDropTableStatement) statement).getTableSources();
                for (int i = 0; i < DropTableList.size(); i++) {
                    String name = DropTableList.get(i).getName().getSimpleName();
                    map = new LinkedHashMap<>();
                    map.put( name,"");
                    ddlReturn = new DdlReturn();
                    ddlReturn.setDdlReturn(empstatement,empoperation,map);
                    ddlReturnList.add(ddlReturn);
                }
            } else if (statement instanceof SQLDropDatabaseStatement) {
                empstatement = "DROP";
                empoperation = "DATABASE";
                String name = ((SQLDropDatabaseStatement) statement).getDatabase().toString();
                map = new LinkedHashMap<>();
                map.put(name, "");
                ddlReturn = new DdlReturn();
                ddlReturn.setDdlReturn(empstatement,empoperation,map);
                ddlReturnList.add(ddlReturn);
            }
        }
        return ddlReturnList;
    }

    public String modifyName(String name, String type) {
        if (type.equals("column")) {
            if (name.contains("`")) {
                name = name.replaceAll("`", "");
                name = "`h_".concat(name).concat("`");
            } else {
                name = "h_".concat(name);
            }
        } else if (type.equals("database")) {
            if (name.contains("`")) {
                name = name.replaceAll("`", "");
                name = "`".concat(name).concat("_history`");
            } else {
                name = name.concat("_history");
            }
        }
        return name;
    }
}