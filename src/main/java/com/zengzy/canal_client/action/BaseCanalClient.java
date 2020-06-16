package com.zengzy.canal_client.action;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
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
import org.apache.commons.lang.StringEscapeUtils;
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
                    if (eventType == EventType.QUERY) {
                        StringBuilder out = new StringBuilder();
                        MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);
                        MySqlStatementParser parser = new MySqlStatementParser(sql);
                        List<SQLStatement> statementList = parser.parseStatementList();
                        // for (SQLStatement statement : statementList) {
                        // statement.accept(visitor);
                        // visitor.println();
                        // }

                    } else {
                        //sql = new DdlSqlHandle(eventType, rowChage.getSql(), entry.getHeader().getSchemaName(), entry.getHeader().getTableName()).main();
                        if (sql.isEmpty()) {
                            continue;
                        } else {
                            String[] sqlArr = sql.split("\n");
                            for (int i = 0; i < sqlArr.length; i++) {
                                sql = sqlArr[i];
                                logger.info(" sql ----> " + sql + SEP);
                                //jdbcTemplate.execute(sql);
                            }
                        }
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
                        //jdbcTemplate.execute(sql);
                    } else if (eventType == EventType.INSERT) {
                        sql = getColumnSql(rowData.getAfterColumnsList(), schemaName, tableName, eventType, es, ts);
                        logger.info(sql);
                        //jdbcTemplate.execute(sql);
                    } else {
                        sql = getColumnSql(rowData.getAfterColumnsList(), schemaName, tableName, eventType, es, ts);
                        logger.info(sql);
                        //jdbcTemplate.execute(sql);
                    }
                }
            }
        }
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
        builder.append("insert into `").append(schemaName).append("_history").append("`.`").append(tableName).append("`");
        StringBuilder c = new StringBuilder();
        StringBuilder v = new StringBuilder();

        c.append("(`type`, ");
        c.append("`es`, ");
        c.append("`ts`, ");

        v.append("('").append(type).append("',");
        v.append(es).append(",");
        v.append(ts).append(",");

        for (Column column : columns) {
            try {
                c.append("h_").append(column.getName());
                String mysqlType = column.getMysqlType();
                if (column.getIsNull()) {
                    v.append("NULL");
                } else {
                    if (StringUtils.containsIgnoreCase(mysqlType, "BLOB") || StringUtils.containsIgnoreCase(mysqlType, "BINARY")) {
                        v.append("'").append(new String(column.getValue().getBytes("ISO-8859-1"), "UTF-8")).append("'");
                    } else if (StringUtils.containsIgnoreCase(mysqlType, "char") || StringUtils.containsIgnoreCase(mysqlType, "text") || StringUtils.containsIgnoreCase(mysqlType, "date")) {
                        v.append("'").append(StringEscapeUtils.escapeJavaScript(column.getValue())).append("'");
                    } else if (StringUtils.containsIgnoreCase(mysqlType, "int") || StringUtils.containsIgnoreCase(mysqlType, "decimal")) {
                        v.append(column.getValue());
                    } else {
                        v.append(column.getValue());
                    }
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
}