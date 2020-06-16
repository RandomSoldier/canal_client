import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
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
        String dbType = JdbcConstants.MYSQL;

        // 新建 MySQL Parser
        SQLStatementParser parser = new MySqlStatementParser(sql);

        // 使用Parser解析生成AST，这里SQLStatement就是AST
        SQLStatement statement = parser.parseStatement();

        // 使用visitor来访问AST
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        statement.accept(visitor);

        // 从visitor中拿出你所关注的信息
        //System.out.println(visitor);
        if(statement instanceof SQLDropDatabaseStatement){
            String database = ((SQLDropDatabaseStatement) statement).getDatabase().toString();
            System.out.println("drop database " + "h_"+ database );
        }
        else {

        }
    }

}
