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
        try(Db db = Db.connect("postgresql", "db_fund", "user_fund", "fund_user2013")) {
            System.out.println(db.tables());
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
        try(Db db = Db.connect("oracle", "OSS", "db_kjk", "kjk_db20120401")) {
            System.out.println(db.tables());
            for(Data<Object[]> row : db.select("name", "passwd").from("M_SYAIN").where("userid", "mor-shim")) {
                for(int i = 0; i < row.names.length; i++) {
                    System.out.println(row.names[i] + ": " + row.value[i]);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("mysql", "db_cms", "root", "")) {
            System.out.println(db.tables());
            for(Data<Object[]> row : db.select("user_name", "password").from("t_user").where("login_id", "ShimizuY")) {
                for(int i = 0; i < row.names.length; i++) {
                    System.out.println(row.names[i] + ": " + row.value[i]);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("sqlserver", "aspnet-mvc4-20130513194150", "sa", "sql@20130615")) {
            System.out.println(db.tables());
            for(Data<Object[]> row : db.select("UserId", "UserName").from("UserProfile").where("UserId", 1)) {
                for(int i = 0; i < row.names.length; i++) {
                    System.out.println(row.names[i] + ": " + row.value[i]);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("h2", "mem:test")) {
            if(!db.tables().contains("account")) db.execute("create table account (id int primary key,name varchar)");
            if(!db.from("account").exists()) db.execute("insert into account values (1, '氏名')");
            for(Data<Object[]> row : db.select("id", "name").from("account").where("id", 1)) {
                for(int i = 0; i < row.names.length; i++) {
                    System.out.println(row.names[i] + ": " + row.value[i]);
                }
            }
            db.truncate("account");
            System.out.println(db.from("account").count());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("h2", "~/test")) {
            if(!db.tables().contains("account")) db.execute("create table account (id int primary key,name varchar)");
            if(!db.from("account").exists()) db.execute("insert into account values (1, '氏名')");
            for(Data<Object[]> row : db.select("id", "name").from("account").where("id", 1)) {
                for(int i = 0; i < row.names.length; i++) {
                    System.out.println(row.names[i] + ": " + row.value[i]);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void truncate(String table) throws SQLException {
        execute("TRUNCATE TABLE " + table);
    }

    private List<String> tables() throws SQLException {
        List<String> list = new ArrayList<>();
        try(RowIterator<Object[]> it = new RowIterator<Object[]>(connection.getMetaData().getTables(null, schema, null, new String[]{"TABLE"}), new ObjectFetcher())) {;
            for(Object[] row : it) {
//                for(int i = 0, i2 = it.names.length; i < i2; i++) System.out.print(it.names[i] + ":" + row[i] + ", "); System.out.println();
                list.add(String.valueOf(row[2]).toLowerCase());
            }
        }
        return list;
    }

    private Connection connection;
    String schema;
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
        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            return statement.executeUpdate();
        }
    }

    public RowIterator<Object[]> rowIterator(String sql) throws SQLException {
        log(sql + ";");
        return new RowIterator<>(connection.prepareStatement(sql), new ObjectFetcher());
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

        public RowIterator<Object[]> rowIterator() throws SQLException {
            return db.rowIterator(sql());
        }

        @Override
        public Iterator<Data<Object[]>> iterator() {
            try {
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
            } catch (SQLException e) {
                e.printStackTrace();
                return new Iterator<Data<Object[]>>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Data<Object[]> next() {
                        return null;
                    }

                    @Override
                    public void remove() {
                    }
                };
            }
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

    public static class ObjectFetcher implements Fetcher<Object[]> {
        RowIterator<Object[]> it;
        Object[] values;
        int columns;

        @Override
        public void setup(RowIterator<Object[]> it) {
            this.it = it;
            columns = it.names.length;
            values = new Object[columns];
        }

        @Override
        public Object[] fetch(ResultSet row) {
            try {
                for(int i = 0; i < columns; i++) values[i] = row.getObject(i + 1);
            } catch (SQLException e) {
                e.printStackTrace();
                it.close();
                return null;
            }
            return values;
        }
    }

    public static class RowIterator<T> implements AutoCloseable, Iterator<T>, Iterable<T> {
        PreparedStatement statement;
        Fetcher<T> fetcher;
        ResultSet row;
        ResultSetMetaData meta;
        String[] names;
        boolean pre = true;
        boolean has = false;
        boolean dirty = true;

        RowIterator(PreparedStatement statement, Fetcher<T> fetcher) throws SQLException {
            this(statement.executeQuery(), fetcher);
            this.statement = statement;
        }

        RowIterator(ResultSet row, Fetcher<T> fetcher) throws SQLException {
            this.row = row;
            this.fetcher = fetcher;
            meta = row.getMetaData();
            names = new String[meta.getColumnCount()];
            for(int i = 0; i < names.length; i++) names[i] = meta.getColumnName(i + 1);
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
        String pad = "//";
        String pad2 = "/";
        String url = null;
        String schema = null;
        switch(type) {
        case "h2":
            url = name;
        break;
        case "postgresql":
            schema = "public";
            if(port <= 0) port = 5432;
        break;
        case "mysql":
            if(port <= 0) port = 3306;
        break;
        case "oracle":
            pad = "thin:@";
            schema = user.toUpperCase();
            if(port <= 0) port = 1521;
        break;
        case "sqlserver":
            pad2 = ";database=";
            schema = "dbo";
            if(port <= 0) port = 1433;
        break;
        }
        if(host == null) host = "localhost";
        if(url == null) url = pad + host + ":" + port + pad2 + name;
        Db db = new Db(DriverManager.getConnection("jdbc:" + type + ":" + url, user, password));
        db.schema = schema;
        return db;
    }

    public static Db connect(String type, String name) throws SQLException {
        return connect(type, name, null, null, null, 0);
    }

    public static Db connect(String type, String name, String user, String password) throws SQLException {
        return connect(type, name, user, password, null, 0);
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

}
