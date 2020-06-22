import com.alibaba.otter.canal.protocol.CanalEntry;
import com.zengzy.canal_client.action.DdlSqlHandle;

import java.util.ArrayList;

public class DdlSqlHandleTest {
    public static void main(String[] args) {
        String sql = "CREATE DEFINER=`root`@`%` PROCEDURE `rewards_report`(\n" +
                "    IN min_monthly_purchases TINYINT UNSIGNED\n" +
                "    , IN min_dollar_amount_purchased DECIMAL(10,2)\n" +
                "    , OUT count_rewardees INT\n" +
                ")\n" +
            //    "    READS SQL DATA\n" +
                "    COMMENT 'Provides a customizable report on best customers'\n" +
                "BEGIN\n" +
                "\n" +
                "    DECLARE last_month_start DATE;\n" +
                "    DECLARE last_month_end DATE;\n" +
                "\n" +
                "    /* Some sanity checks... */\n" +
                "    IF min_monthly_purchases = 0 THEN\n" +
                "        SELECT 'Minimum monthly purchases parameter must be > 0';\n" +
                "        LEAVE proc;\n" +
                "    END IF;\n" +
                "    IF min_dollar_amount_purchased = 0.00 THEN\n" +
                "        SELECT 'Minimum monthly dollar amount purchased parameter must be > $0.00';\n" +
                "        LEAVE proc;\n" +
                "    END IF;\n" +
                "\n" +
                "    /* Determine start and end time periods */\n" +
                "    SET last_month_start = DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH);\n" +
                "    SET last_month_start = STR_TO_DATE(CONCAT(YEAR(last_month_start),'-',MONTH(last_month_start),'-01'),'%Y-%m-%d');\n" +
                "    SET last_month_end = LAST_DAY(last_month_start);\n" +
                "\n" +
                "    /*\n" +
                "        Create a temporary storage area for\n" +
                "        Customer IDs.\n" +
                "    */\n" +
                "    CREATE TEMPORARY TABLE tmpCustomer (customer_id SMALLINT UNSIGNED NOT NULL PRIMARY KEY);\n" +
                "\n" +
                "    /*\n" +
                "        Find all customers meeting the\n" +
                "        monthly purchase requirements\n" +
                "    */\n" +
                "    INSERT INTO tmpCustomer (customer_id)\n" +
                "    SELECT p.customer_id\n" +
                "    FROM payment AS p\n" +
                "    WHERE DATE(p.payment_date) BETWEEN last_month_start AND last_month_end\n" +
                "    GROUP BY customer_id\n" +
                "    HAVING SUM(p.amount) > min_dollar_amount_purchased\n" +
                "    AND COUNT(customer_id) > min_monthly_purchases;\n" +
                "\n" +
                "    /* Populate OUT parameter with count of found customers */\n" +
                "    SELECT COUNT(*) FROM tmpCustomer INTO count_rewardees;\n" +
                "\n" +
                "    /*\n" +
                "        Output ALL customer information of matching rewardees.\n" +
                "        Customize output as needed.\n" +
                "    */\n" +
                "    SELECT c.*\n" +
                "    FROM tmpCustomer AS t\n" +
                "    INNER JOIN customer AS c ON t.customer_id = c.customer_id;\n" +
                "\n" +
                "    /* Clean up */\n" +
                "    DROP TABLE tmpCustomer;\n" +
                "END";

//        String sql = "CREATE TABLE public_history (\n" +
//                "id int(10) NOT NULL AUTO_INCREMENT ,\n" +
//                "v_id int(11) DEFAULT NULL ,\n" +
//                "name char(255) NOT NULL ,\n" +
//                "type enum('日策略','周策略')  NOT NULL ,\n" +
//                "update_uid int(10) NOT NULL,\n" +
//                "update_time datetime DEFAULT NULL,\n" +
//                "create_uid int(11) NOT NULL,\n" +
//                "create_time datetime NOT NULL,\n" +
//                "PRIMARY KEY (id),\n" +
//                "UNIQUE KEY name (name)\n" +
//                ") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 ;";
        ArrayList<String> arrayList = new ArrayList<>();
        arrayList.add(sql);
        //arrayList.add(sql1);
        //arrayList.add(sql2);
        //arrayList.add(sql3);
        //arrayList.add(sql4);
        //arrayList.add(sql5);
        //arrayList.add(sql6);
        //arrayList.add(sql7);
        //arrayList.add(sql8);
        //arrayList.add(sql9);

        String schemaName = "";
        String tableName = "";
        CanalEntry.EventType eventType = CanalEntry.EventType.QUERY;
        for (int i = 0; i < arrayList.size(); i++) {
            //new DdlSqlHandle(eventType,arrayList.get(i),schemaName,tableName).main();
            System.out.println(new DdlSqlHandle(eventType, arrayList.get(i), schemaName, tableName).main());
            //System.out.println("1");
        }

//        CCJSqlParserManager pm = new CCJSqlParserManager();
//        net.sf.jsqlparser.statement.Statement statement = pm.parse(new StringReader(sql));
//        System.out.println(statement);
//        if (statement instanceof Select) {
//            Select selectStatement = (Select) statement;
//            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
//            List tableList = tablesNamesFinder.getTableList(selectStatement);
//            for (Iterator iter = tableList.iterator(); iter.hasNext();) {
//                System.out.println(tableName);
//            }
//        }

//        final String dbType = JdbcConstants.MYSQL; // 可以是ORACLE、POSTGRESQL、SQLSERVER、ODPS等
//        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
//        SQLStatement statement = stmtList.get(0);
//        Map map = null;
//        if (statement instanceof MySqlCreateTableStatement) {
//            String empstatement = "CREATE TABLE";
//            List columnList = ((MySqlCreateTableStatement) statement).getTableElementList();
//            map = new LinkedHashMap<>();
//            for (int i = 0; i < columnList.size(); i++) {
//                if (columnList.get(i) instanceof SQLColumnDefinition) {
//                    SQLColumnDefinition sqlColumnDefinition = (SQLColumnDefinition) columnList.get(i);
//                    map.put(sqlColumnDefinition.getName().toString(), sqlColumnDefinition.getDataType().toString());
//                }
//            }
//        }
//        System.out.println(map);
    }
}
