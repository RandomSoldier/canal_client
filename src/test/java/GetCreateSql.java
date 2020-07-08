import com.alibaba.otter.canal.protocol.CanalEntry;
import com.zzy.canal_client.action.DdlSqlHandle;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class GetCreateSql {

    private String DRIVER = "com.mysql.cj.jdbc.Driver";
    private String URL = null;
    private String USERNAME = null;
    private String PASSWORD = null;
    private String schemaName = null;
    private Connection conn = null;

    public GetCreateSql(String ip, String port, String database, String username, String password) {
        try {
            Class.forName(this.DRIVER);
            this.URL = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf8", ip, port, database);
            this.USERNAME = username;
            this.PASSWORD = password;
            this.schemaName = database;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.err.println("can not load jdbc driver");
        }
    }

    /**
     * 获取数据库连接
     *
     * @return
     */
    private Connection getConnection() {
        try {
            if (null == conn) {
                conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("get connection failure");
        }
        return conn;
    }

    /**
     * 关闭数据库连接
     *
     * @param conn
     */
    private void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.err.println("close connection failure");
            }
        }
    }

    /**
     * 获取数据库下的所有表名
     */
    private List<String> getTableNames() {
        List<String> tableNames = new ArrayList<String>();
        Connection connection = getConnection();
        ResultSet resultSet = null;
        try {
            // 获取数据库的元数据
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            // 从元数据中获取到所有的表名
            resultSet = databaseMetaData.getTables(conn.getCatalog(), "%", "%",new String[]{"TABLE"});
            while (resultSet.next()) {
                tableNames.add(resultSet.getString(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("getTableNames failure");
        } finally {
            try {
                if (null != resultSet) {
                    resultSet.close();
                }

            } catch (SQLException e) {
                e.printStackTrace();
                System.err.println("close ResultSet failure");
            }
        }
        return tableNames;
    }

    /**
     * 生成建表语句
     *
     * @param tableName
     * @return
     */
    private String generateCreateTableSql(String tableName) {
        String sql = String.format("SHOW CREATE TABLE %s", tableName);
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            preparedStatement = (PreparedStatement) connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                // 返回建表语句语句，查询结果的第二列是建表语句，第一列是表名
                //System.out.println(rs.getString(2).concat("\n"));
                return resultSet.getString(2).concat("\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (null != preparedStatement) {
                    preparedStatement.close();
                }

            } catch (Exception e2) {
                e.printStackTrace();
                System.err.println("关闭流异常");
            }
        }
        return null;
    }

    public void start() {
        try {
            String sql = "";
            List<String> tableNames = getTableNames();
            System.out.println("tableNames:" + tableNames);
            for (String tableName : tableNames) {
                sql = new DdlSqlHandle(CanalEntry.EventType.CREATE,generateCreateTableSql(tableName), schemaName, tableName).main();
                System.out.println(sql);
            }
            // 统一关闭连接
            closeConnection(conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new GetCreateSql("192.168.42.100", "3306","classicmodels", "root", "root").start();
    }
}