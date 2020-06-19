import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.zengzy.canal_client.action.DdlSqlHandle;
import net.sf.jsqlparser.JSQLParserException;

import java.util.ArrayList;
import java.util.List;

public class DdlSqlHandleTest {
    public static void main(String[] args) throws JSQLParserException {
        String sql = "drop database test";
        String sql1 = "drop database `test`";
        String sql2 = "drop table test";
        String sql3 = "drop table `test`";
        String sql4 = "DROP SCHEMA IF EXISTS sakila;";
        String sql5 = "DROP SCHEMA IF EXISTS `sakila`;";

        ArrayList<String> arrayList = new ArrayList<>();
        arrayList.add(sql);
        arrayList.add(sql1);
        arrayList.add(sql2);
        arrayList.add(sql3);
        arrayList.add(sql4);
        arrayList.add(sql5);

        String schemaName = "";
        String tableName = "";
        CanalEntry.EventType eventType = CanalEntry.EventType.QUERY;
        for (int i = 0 ; i < arrayList.size(); i++)
        {
            // new DdlSqlHandle(eventType,arrayList.get(i),schemaName,tableName).main();
        }

        StringBuilder out = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        for (SQLStatement statement : statementList) {
            statement.accept(visitor);
            visitor.println();
            System.out.println(out.toString());
        }
        String sql6 = "CREATE TRIGGER `ins_film` AFTER INSERT ON `film` FOR EACH ROW BEGIN\n" +
                "    INSERT INTO film_text (film_id, title, description)\n" +
                "        VALUES (new.film_id, new.title, new.description);\n" +
                "  END;;\n" +
                "\n" +
                "\n" +
                "CREATE TRIGGER `upd_film` AFTER UPDATE ON `film` FOR EACH ROW BEGIN\n" +
                "    IF (old.title != new.title) OR (old.description != new.description) OR (old.film_id != new.film_id)\n" +
                "    THEN\n" +
                "        UPDATE film_text\n" +
                "            SET title=new.title,\n" +
                "                description=new.description,\n" +
                "                film_id=new.film_id\n" +
                "        WHERE film_id=old.film_id;\n" +
                "    END IF;\n" +
                "  END;;\n" +
                "\n" +
                "\n" +
                "CREATE TRIGGER `del_film` AFTER DELETE ON `film` FOR EACH ROW BEGIN\n" +
                "    DELETE FROM film_text WHERE film_id = old.film_id;\n" +
                "  END;;";
        new DdlSqlHandle(eventType,sql6,schemaName,tableName).main();

    }
}
