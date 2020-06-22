import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;


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
        String sql = "CREATE TABLE address (\n" +
                "  address_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
                "  address VARCHAR(50) NOT NULL,\n" +
                "  address2 VARCHAR(50) DEFAULT NULL,\n" +
                "  district VARCHAR(20) NOT NULL,\n" +
                "  city_id SMALLINT UNSIGNED NOT NULL,\n" +
                "  postal_code VARCHAR(10) DEFAULT NULL,\n" +
                "  phone VARCHAR(20) NOT NULL,\n" +
                "  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
                "  PRIMARY KEY  (address_id),\n" +
                "  KEY idx_fk_city_id (city_id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;CREATE TABLE category (\n" +
                "  category_id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
                "  name VARCHAR(25) NOT NULL,\n" +
                "  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
                "  PRIMARY KEY  (category_id)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
//        String sql = "SELECT id FROM user WHERE status = 1;\n" +
//                "SELECT id FROM order WHERE create_time > '2018-01-01'";


        StringBuilder out = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        for (SQLStatement statement : statementList) {
            if(statement instanceof MySqlCreateTableStatement){

                System.out.println("a");
            }
            statement.accept(visitor);
            visitor.println();
            System.out.println(out.toString());
        }

        //return out.toString();
    }

}
