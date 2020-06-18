import com.alibaba.otter.canal.protocol.CanalEntry;
import com.zengzy.canal_client.action.DdlSqlHandle;
import net.sf.jsqlparser.JSQLParserException;

import java.util.ArrayList;

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
            new DdlSqlHandle(eventType,arrayList.get(i),schemaName,tableName).main();
        }


    }
}
