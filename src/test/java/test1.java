import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;


import java.util.List;

/**
 * @program: canal_client
 * @description: 处理新建库
 * @author: zengzy
 * @create: 2020-06-16 17:50
 **/

public class test1 {
    public static void main(String[] args) {
//        String sql = "CREATE SCHEMA `test`\n" +
//                "DEFAULT CHARACTER SET utf8\n" +
//                "DEFAULT COLLATE utf8_general_ci;";
        String sql = "CREATE DEFINER=`root`@`%` PROCEDURE `rewards_report`(\n" +
                "    IN min_monthly_purchases TINYINT UNSIGNED\n" +
                "    , IN min_dollar_amount_purchased DECIMAL(10,2)\n" +
                "    , OUT count_rewardees INT\n" +
                ")\n" +
                "\n" +
                "\n" +
                "proc: BEGIN\n" +
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
//        String sql = "SELECT id FROM user WHERE status = 1;\n" +
//                "SELECT id FROM order WHERE create_time > '2018-01-01'";


        StringBuilder out = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        for (SQLStatement statement : statementList) {
            statement.accept(visitor);
            visitor.println();
            System.out.println(out.toString());
        }

        //return out.toString();
    }

}
