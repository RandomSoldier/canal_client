package com.zengzy.canal_client.action;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.*;

import com.zengzy.canal_client.model.DdlReturn;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;

public class BaseCanalClient {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    protected volatile boolean running = false;
    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };
    protected Thread thread = null;
    protected CanalConnector connector;
    protected JdbcTemplate jdbcTemplate;
    protected static String context_format = null;
    protected static String row_format = null;
    protected static String transaction_format = null;
    protected String destination;

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {}({}) , gtid : ({}) , delay : {} ms"
                + SEP;

        transaction_format = SEP
                + "================> binlog[{}:{}] , executeTime : {}({}) , gtid : ({}) , delay : {}ms"
                + SEP;

    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

    public void setTargetConnector(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void analysisMessage(List<Entry> entrys) throws JSQLParserException {
        String sql = "";

        for (Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;
            Date date = new Date(entry.getHeader().getExecuteTime());
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage = null;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                EventType eventType = rowChage.getEventType();

                logger.info(row_format,
                        new Object[]{entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                                entry.getHeader().getTableName(), eventType,
                                String.valueOf(entry.getHeader().getExecuteTime()), simpleDateFormat.format(date),
                                entry.getHeader().getGtid(), String.valueOf(delayTime)});

                if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info(" sql ----> " + rowChage.getSql() + SEP);
                    sql = DdlSqlHandle(eventType, rowChage.getSql(), entry.getHeader().getSchemaName(), entry.getHeader().getTableName());
                    System.out.println(sql);
                    if (sql.isEmpty()) {
                        continue;
                    } else {
                        jdbcTemplate.execute(sql);
                    }
                    continue;
                }

                long es = entry.getHeader().getExecuteTime();
                long ts = System.currentTimeMillis();
                String tableName = entry.getHeader().getTableName();
                String schemaName = entry.getHeader().getSchemaName();

                for (RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == EventType.DELETE) {
                        sql = getColumnSql(rowData.getBeforeColumnsList(), schemaName, tableName, eventType, es, ts);
                        logger.info(sql);
                        jdbcTemplate.execute(sql);
                    } else if (eventType == EventType.INSERT) {
                        sql = getColumnSql(rowData.getAfterColumnsList(), schemaName, tableName, eventType, es, ts);
                        logger.info(sql);
                        jdbcTemplate.execute(sql);
                    } else {
                        sql = getColumnSql(rowData.getAfterColumnsList(), schemaName, tableName, eventType, es, ts);
                        logger.info(sql);
                        jdbcTemplate.execute(sql);
                    }
                }
            }
        }
        // return sql;
    }

    protected String getColumnSql(List<Column> columns, String schemaName, String tableName, EventType eventType, long es, long ts) {
        StringBuilder builder = new StringBuilder();
        String type = "";
        if (eventType == EventType.DELETE) {
            type = "DELETE";
        } else if (eventType == EventType.INSERT) {
            type = "INSERT";
        } else {
            type = "UPDATE";
        }
        builder.append("insert into `").append(schemaName).append("_history").append("`").append(".").append("`").append(tableName).append("`");
        StringBuilder c = new StringBuilder();
        StringBuilder v = new StringBuilder();

        c.append("(").append("`").append("type").append("`").append(",");
        c.append("`").append("es").append("`").append(",");
        c.append("`").append("ts").append("`").append(",");

        v.append("(").append("'").append(type).append("'").append(",");
        v.append(es).append(",");
        v.append(ts).append(",");

        for (Column column : columns) {
            try {
                c.append("`").append("h_").append(column.getName()).append("`");
                if (StringUtils.containsIgnoreCase(column.getMysqlType(), "BLOB")
                        || StringUtils.containsIgnoreCase(column.getMysqlType(), "BINARY")) {
                    v.append("'").append(new String(column.getValue().getBytes("ISO-8859-1"), "UTF-8")).append("'");
                } else if (StringUtils.containsIgnoreCase(column.getMysqlType(), "char")) {
                    v.append("'").append(column.getValue()).append("'");
                } else if (StringUtils.containsIgnoreCase(column.getMysqlType(), "int")) {
                    v.append(column.getValue());
                }
            } catch (UnsupportedEncodingException e) {
            }
            c.append(",");
            v.append(",");
        }
        c.deleteCharAt(c.length() - 1).append(")");
        v.deleteCharAt(v.length() - 1).append(")");

        builder.append(c).append("values").append(v);
        return builder.toString();
    }

    protected String DdlSqlHandle(EventType eventType, String sql, String schemaName, String tableName) throws JSQLParserException {

        if (sql.contains("USING BTREE")) {
            sql = sql.replaceAll("USING BTREE", "");
        }

        StringBuilder builder = new StringBuilder();
        // Map<String, String> columnList = getColumnBySql(sql);

        List<DdlReturn> ddlReturnList = getColumnBySql(sql);
        for (DdlReturn ddlReturn : ddlReturnList) {
            Map<String, String> columnList = ddlReturn.map;
            String statement = ddlReturn.statement;
            String operation = ddlReturn.operation;

            if (statement.equals("CreateTable")) {  // 新增表
                builder.append("CREATE TABLE `").append(schemaName).append("_history").append("`").append(".").append("`").append(tableName).append("`");
                builder.append("(`id` bigint(20) NOT NULL AUTO_INCREMENT,");
                builder.append("`type` varchar(10),");
                builder.append("`es` bigint(20),");
                builder.append("`ts` bigint(20), ");

                for (Map.Entry<String, String> entry : columnList.entrySet()) {
                    String columnName = entry.getKey();
                    if (columnName.contains("`")) {
                        columnName = columnName.replaceAll("`", "");
                        columnName = "`h_".concat(columnName).concat("`");
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
                } else if (operation == "CHANGE") { // 字段改名
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