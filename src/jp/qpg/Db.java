/**
 * 
 */
package jp.qpg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author nakazawaken1
 *
 */
public class Db implements AutoCloseable {

    private Connection connection;

    public static void log(Object o) {
        System.out.println(new Date() + ": " + o);
    }

    public Db(Connection connection) {
        this.connection = connection;
    }

    /**
     * test
     * @param args (unuse)
     */
    public static void main(String[] args) {
        try(Db db = Db.connect("postgresql", "user_fund", "fund_user2013", "localhost", 5432, "db_fund")) {
            for(Map<String, Object> row : db.select("name", "password").from("account").where("login_id", "kimoto").fetch()) {
                for(Map.Entry<String, Object> e : row.entrySet()) {
                    System.out.println(e.getKey() + ": " + e.getValue());
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("oracle", "db_kjk", "kjk_db20120401", "localhost", 1521, "OSS")) {
            for(Map<String, Object> row : db.select("name", "passwd").from("M_SYAIN").where("userid", "mor-shim").fetch()) {
                for(Map.Entry<String, Object> e : row.entrySet()) {
                    System.out.println(e.getKey() + ": " + e.getValue());
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("mysql", "root", "", "localhost", 3306, "db_cms")) {
            for(Map<String, Object> row : db.select("user_name", "password").from("t_user").where("login_id", "ShimizuY").fetch()) {
                for(Map.Entry<String, Object> e : row.entrySet()) {
                    System.out.println(e.getKey() + ": " + e.getValue());
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Query select(String... fields) {
        Query q = new Query(connection);
        q.fields = Arrays.asList(fields);
        return q;
    }

    public Query from(String table) {
        Query q = new Query(connection);
        q.table = table;
        return q;
    }

    public static class Query {
        String table;
        List<String> fields;
        List<String> wheres;
        private Connection connection;

        public Query(Connection connection) {
            this.connection = connection;
        }

        public Query from(String table) {
            this.table = table;
            return this;
        }

        public Query addWhere(String where) {
            if(wheres == null) wheres = new ArrayList<>();
            wheres.add(where);
            return this;
        }

        public Iterable<Map<String, Object>> fetch() throws Exception {
            StringBuffer sql = new StringBuffer();
            sql.append("SELECT ");
            if(fields == null) sql.append("*");
            else {
                String pad = "";
                for(String field : fields) {
                    sql.append(pad).append(field);
                    pad = ", ";
                }
            }
            sql.append(" FROM ").append(table);
            if(wheres != null) {
                String pad = " WHERE ";
                for(String where : wheres) {
                    sql.append(pad).append(where);
                    pad = " AND ";
                }
            }
            log(sql + ";");
            try(PreparedStatement s = connection.prepareStatement(sql.toString())) {
                try(ResultSet row = s.executeQuery()) {
                    ResultSetMetaData meta = row.getMetaData();
                    int i2 = meta.getColumnCount();
                    String[] names = new String[i2];
                    for(int i = 0; i < i2; i++) names[i] = meta.getColumnName(i + 1);
                    List<Map<String, Object>> list = new ArrayList<>();
                    while(row.next()) {
                        Map<String, Object> map = new HashMap<>();
                        for(int i = 0; i < i2; i++) {
                            map.put(names[i], row.getObject(i + 1));
                        }
                        list.add(map);
                    }
                    return list;
                }
            }
        }

        public String escape(String text) {
            return text.replace("'", "''");
        }

        public Query where(String field, String value) {
            return addWhere(field + " = '" + escape(value) + "'");
        }
    }

    public static Db connect(String type, String user, String pass, String host, int port, String name) throws SQLException, ClassNotFoundException {
        String pad = "oracle".equals(type) ? ":thin:@" : "://";
        return new Db(DriverManager.getConnection("jdbc:" + type + pad + host + ":" + port + "/" + name, user, pass));
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

}
