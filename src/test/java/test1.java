import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcConstants;

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
        String sql = "DROP SCHEMA `test`";
//        String sql = "SELECT id FROM user WHERE status = 1;\n" +
//                "SELECT id FROM order WHERE create_time > '2018-01-01'";


        StringBuilder out = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        for (SQLStatement statement : statementList) {
            statement.accept(visitor);
            visitor.println();
        }
        //return out.toString();
    }

}
