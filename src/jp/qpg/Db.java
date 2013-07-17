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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author nakazawaken1
 *
 */
public class Db implements AutoCloseable {

    /**
     * test
     * @param args (unuse)
     */
    public static void main(String[] args) {
        try(Db db = Db.connect("postgresql", "db_fund", "user_fund", "fund_user2013", "localhost", 5432)) {
           System.out.println(db.from("account").count());
           System.out.println(db.one("SELECT COUNT(*) FROM account").value);
           for(Data<Object[]> row : db.select("name", "password").from("account")) {
                for(int i = 0; i < row.names.length; i++) {
                    System.out.println(row.names[i] + ": " + row.value[i]);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("oracle", "OSS", "db_kjk", "kjk_db20120401", "localhost", 1521)) {
            for(Data<Object[]> row : db.select("name", "passwd").from("M_SYAIN").where("userid", "mor-shim")) {
                for(int i = 0; i < row.names.length; i++) {
                    System.out.println(row.names[i] + ": " + row.value[i]);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("mysql", "db_cms", "root", "", "localhost", 3306)) {
            for(Data<Object[]> row : db.select("user_name", "password").from("t_user").where("login_id", "ShimizuY")) {
                for(int i = 0; i < row.names.length; i++) {
                    System.out.println(row.names[i] + ": " + row.value[i]);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("sqlserver", "aspnet-mvc4-20130513194150", "sa", "sql@20130615", "localhost", 1433)) {
            for(Data<Object[]> row : db.select("UserId", "UserName").from("UserProfile").where("UserId", 1)) {
                for(int i = 0; i < row.names.length; i++) {
                    System.out.println(row.names[i] + ": " + row.value[i]);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("h2", "mem:test")) {
            db.execute("create table account (id int primary key,name varchar)");
            db.execute("insert into account values (1, '氏名')");
            for(Data<Object[]> row : db.select("id", "name").from("account").where("id", 1)) {
                for(int i = 0; i < row.names.length; i++) {
                    System.out.println(row.names[i] + ": " + row.value[i]);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("h2", "~/test")) {
            if(!db.from("account").exists()) {
                db.execute("create table account (id int primary key,name varchar)");
                db.execute("insert into account values (1, '氏名')");
            }
            for(Data<Object[]> row : db.select("id", "name").from("account").where("id", 1)) {
                for(int i = 0; i < row.names.length; i++) {
                    System.out.println(row.names[i] + ": " + row.value[i]);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Connection connection;
    public static SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS ");

    public static void log(Object o) {
        System.out.println(format.format(new Date()) + ": " + o);
    }

    public Db(Connection connection) {
        this.connection = connection;
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

    public int execute(String sql) throws SQLException {
        log(sql + ";");
        return connection.prepareStatement(sql).executeUpdate();
    }

    public RowIterator<Object[]> rowIterator(String sql) {
        try {
            log(sql + ";");
            return new RowIterator<>(connection.prepareStatement(sql), new Fetcher<Object[]>() {
                RowIterator<Object[]> it;
                Object[] values;
                int i2;

                @Override
                public void setup(RowIterator<Object[]> it) {
                    this.it = it;
                    i2 = it.columns;
                    values = new Object[i2];
                }

                @Override
                public Object[] fetch(ResultSet row) {
                    try {
                        for(int i = 0; i < i2; i++) values[i] = row.getObject(i + 1);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        it.close();
                        return null;
                    }
                    return values;
                }
            });
        } catch(SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Map<String, Object>> query(String sql) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<>();
        try(RowIterator<Object[]> it = rowIterator(sql)) {
            for(Object[] values : it) {
                Map<String, Object> map = new HashMap<>();
                for(int i = 0; i < values.length; i++) {
                    map.put(it.names[i], values[i]);
                }
                list.add(map);
            }
        }
        return list;
    }

    public static class Data<T> {
        public String[] names;
        public T value;
        public Data(String[] names, T value) {
            this.names = names;
            this.value = value;
        }
    }

    public Data<Object[]> row(String sql) throws SQLException {
        try(RowIterator<Object[]> it = rowIterator(sql)) {
            return new Data<>(it.names, it.hasNext() ? it.next(): new Object[]{});
        }
    }

    public Data<List<Object[]>> rows(String sql) throws SQLException {
        try(RowIterator<Object[]> it = rowIterator(sql)) {
            List<Object[]> list = new ArrayList<>();
            for(Object[] values : it) list.add(values);
            return new Data<>(it.names, list);
        }
    }

    public Data<Object> one(String sql) throws SQLException {
        try(RowIterator<Object[]> it = rowIterator(sql)) {
            return new Data<>(it.names, it.hasNext() ? it.next()[0]: null);
        }
    }

    public Data<List<Object>> ones(String sql) throws SQLException {
        try(RowIterator<Object[]> it = rowIterator(sql)) {
            List<Object> list = new ArrayList<>();
            while(it.hasNext()) list.add(it.next());
            return new Data<>(it.names, list);
        }
    }

    public Data<Map<Object, Object>> map(String sql) throws SQLException {
        try(RowIterator<Object[]> it = rowIterator(sql)) {
            Map<Object, Object> map = new HashMap<>();
            for(Object[] values : it) map.put(values[0], values[1]);
            return new Data<>(it.names, map);
        }
    }

    public Data<Map<Object, Object[]>> maps(String sql) throws SQLException {
        try(RowIterator<Object[]> it = rowIterator(sql)) {
            Map<Object, Object[]> map = new HashMap<>();
            for(Object[] values : it) map.put(values[0], values);
            return new Data<>(it.names, map);
        }
    }

    public boolean exists(String sql) throws SQLException {
        log(sql + ";");
        try(RowIterator<Object[]> it = new RowIterator<>(connection.prepareStatement(sql), null)) {
            return it.hasNext();
        }
    }

    public long count(String sql) throws SQLException {
        log(sql + ";");
        try(RowIterator<Void> it = new RowIterator<>(connection.prepareStatement(sql), null)) {
            long result = 0;
            while(it.hasNext()) {
                result++;
                it.next();
            }
            return result;
        }
    }

    public static class Query implements Iterable<Data<Object[]>> {
        String table;
        List<String> fields;
        List<String> wheres;
        private Db db;

        public Query(Connection connection) {
            db = new Db(connection);
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

        public String sql() {
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
            return sql.toString();
        }

        public String escape(String text) {
            return text.replace("'", "''");
        }

        public Query where(String field, String value) {
            return addWhere(field + " = '" + escape(value) + "'");
        }

        public Query where(String field, Number value) {
            return addWhere(field + " = " + value);
        }

        public RowIterator<Object[]> rowIterator() {
            return db.rowIterator(sql());
        }

        @Override
        public Iterator<Data<Object[]>> iterator() {
            return new Iterator<Data<Object[]>>() {
                RowIterator<Object[]> it = rowIterator();

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Data<Object[]> next() {
                    return new Data<>(it.names, it.next());
                }

                @Override
                public void remove() {
                    it.remove();
                }
            };
        }

        public Data<Object[]> row() throws SQLException {
            return db.row(sql());
        }

        public Data<List<Object[]>> rows() throws SQLException {
            return db.rows(sql());
        }

        public Data<Object> one() throws SQLException {
            return db.one(sql());
        }

        public Data<List<Object>> ones() throws SQLException {
            return db.ones(sql());
       }

        public Data<Map<Object, Object>> map() throws SQLException {
            return db.map(sql());
        }

        public Data<Map<Object, Object[]>> maps() throws SQLException {
            return db.maps(sql());
        }

        public boolean exists() throws SQLException {
            return db.exists(sql());
        }

        public long count() throws SQLException {
            return db.count(sql());
        }
    }

    interface Fetcher<T> {
        void setup(RowIterator<T> it);
        T fetch(ResultSet row);
    }

    public static class RowIterator<T> implements AutoCloseable, Iterator<T>, Iterable<T> {
        PreparedStatement statement;
        Fetcher<T> fetcher;
        ResultSet row;
        ResultSetMetaData meta;
        String[] names;
        int columns;
        boolean pre = true;
        boolean has = false;
        boolean dirty = true;

        RowIterator(PreparedStatement statement, Fetcher<T> fetcher) throws SQLException {
            this.statement = statement;
            this.fetcher = fetcher;
            row = statement.executeQuery();
            meta = row.getMetaData();
            columns = meta.getColumnCount();
            names = new String[columns];
            for(int i = 0; i < columns; i++) names[i] = meta.getColumnName(i + 1);
            if(fetcher != null) fetcher.setup(this);
        }

        @Override
        public boolean hasNext() {
            if(pre) {
                try {
                    has = row.next();
                } catch (SQLException e) {
                    e.printStackTrace();
                    has = false;
                    close();
                }
                pre = false;
            }
            if(!has) close();
            return has;
        }

        @Override
        public T next() {
            if(pre) try {
                if(!row.next()) close();
            } catch (SQLException e) {
                e.printStackTrace();
                has = false;
                close();
            }
            pre = true;
            return fetcher == null ? null : fetcher.fetch(row);
        }

        @Override
        public void remove() {
        }

        @Override
        public Iterator<T> iterator() {
            return this;
        }

        @Override
        public void close() {
            if(dirty) {
                log("#clean");
                try {
                    if(row != null) row.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                try {
                    if(statement != null) statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                dirty = false;
            }
        }
    }

    public static Db connect(String type, String name, String user, String password, String host, int port) throws SQLException {
        String pad = "oracle".equals(type) ? "thin:@" : "//";
        String pad2 = "sqlserver".equals(type) ? ";database=" : "/";
        String url = "jdbc:" + type + ":" + ("h2".equals(type) ? name : pad + host + ":" + port + pad2 + name);
        return new Db(DriverManager.getConnection(url, user, password));
    }

    public static Db connect(String type, String name) throws SQLException {
        return connect(type, name, null, null, null, 0);
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

}
