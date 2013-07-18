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
import java.util.Calendar;
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
        class Test {
            public Test(Db db) throws SQLException {
                String table = "test_table";
                System.out.println(db.tables());
                if(db.tables().contains(table)) db.drop(table);
                db.create(table, 1, new Column("id").integer(), new Column("name").text(10), new Column("birthday").date());
                String[] names = { "id", "name", "birthday" };
                for(int i = 1; i <= 20; i++) {
                    Calendar c = Calendar.getInstance();
                    c.add(Calendar.DATE, -i * 31);
                    db.insert(table, names, 1, i, "氏名" + i, c.getTime());
                }
                System.out.println(db.from(table).count());
                Query q = db.select("name", "birthday").from(table).where(db.builder.fn("MONTH", "birthday") + " > 6").asc("id");
                System.out.println(q.count());
                for(Data<Object[]> row : q.limit(3)) {
                    for(int i = 0; i < row.names.length; i++) {
                        System.out.print(row.names[i] + "=" + row.value[i] + "\t");
                    }
                    System.out.println("[1-3]");
                }
                for(Data<Object[]> row : q.offset(3)) {
                    for(int i = 0; i < row.names.length; i++) {
                        System.out.print(row.names[i] + "=" + row.value[i] + "\t");
                    }
                    System.out.println("[4-6]");
                }
                for(Data<Object[]> row : q.offset(6).limit(0)) {
                    for(int i = 0; i < row.names.length; i++) {
                        System.out.print(row.names[i] + "=" + row.value[i] + "\t");
                    }
                    System.out.println("[7-]");
                }
                db.truncate(table);
                System.out.println(db.from(table).count());
                db.drop(table);
            }
        }
        try(Db db = Db.connect("postgresql", "db_fund", "user_fund", "fund_user2013")) {
            new Test(db);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("oracle", "OSS", "db_kjk", "kjk_db20120401")) {
            new Test(db);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("mysql", "db_cms", "root", "")) {
            new Test(db);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("sqlserver", "aspnet-mvc4-20130513194150", "sa", "sql@20130615")) {
            new Test(db);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("h2", "mem:test")) {
            new Test(db);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try(Db db = Db.connect("h2", "~/test")) {
            new Test(db);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean merge(String table, String[] names, int primary, Object... values) throws SQLException {
        Query q = from(table);
        for(int i = 0; i < primary; i++) q.where(names[i], values[i]);
        boolean empty = !q.exists();
        if(empty) insert(table, names, primary, values);
        else update(table, names, primary, values);
        return empty;
    }

    public void update(String table, String[] names, int primary, Object... values) throws SQLException {
        StringBuffer sql = new StringBuffer("UPDATE ");
        sql.append(table);
        String pad = " SET ";
        for(int i = primary; i < values.length; i++) {
            sql.append(pad).append(names[i]).append(" = ").append(builder.escape(values[i]));
            pad = ", ";
        }
        pad = " WHERE ";
        for(int i = 0; i < primary; i++) {
            sql.append(pad).append(names[i]).append(" = ").append(builder.escape(values[i]));
            pad = " AND ";
        }
        execute(sql.toString());
    }

    public void insert(String table, String[] names, int primary, Object... values) throws SQLException {
        StringBuffer sql = new StringBuffer("INSERT INTO ");
        sql.append(table).append(join("(", Arrays.asList(names), ", "));
        String pad = ") VALUES(";
        for(Object value : values) {
            sql.append(pad).append(builder.escape(value));
            pad = ", ";
        }
        execute(sql.append(")").toString());
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
    SqlBuilder builder;
    String schema;
    public static SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS ");

    public static void log(Object o) {
        System.out.println(format.format(new Date()) + ": " + o);
    }

    public static <T> String join(String prefix, Iterable<T> items, String pad) {
        if(pad == null) pad = "";
        StringBuffer result = new StringBuffer();
        if(items != null) for(Object item : items) {
            result.append(pad).append(item);
        }
        return result.length() <= 0 ? "" : prefix + result.substring(pad.length());
    }

    public Db(Connection connection, SqlBuilder builder, String schema) {
        this.connection = connection;
        this.builder = builder;
        this.schema = schema;
    }

    public Query select(String... fields) {
        Query q = new Query(this);
        q.fields = Arrays.asList(fields);
        return q;
    }

    public Query from(String table) {
        Query q = new Query(this);
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
        return ((Number)one(builder.countSql(sql)).value).longValue();
    }

    public static class Query implements Iterable<Data<Object[]>> {
        String table;
        List<String> fields;
        List<String> wheres;
        private Db db;
        List<String> orders;
        long offset = 0;
        long limit = 0;

        public Query(Db db) {
            this.db = db;
        }

        public Query from(String table) {
            this.table = table;
            return this;
        }

        public Query asc(String... fields) {
            if(orders == null) orders = new ArrayList<>();
            for(String field : fields) orders.add(field);
            return this;
        }

        public Query desc(String... fields) {
            if(orders == null) orders = new ArrayList<>();
            for(String field : fields) orders.add(field + " DESC");
            return this;
        }

        public Query where(String condition) {
            if(wheres == null) wheres = new ArrayList<>();
            wheres.add(condition);
            return this;
        }

        public Query offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Query limit(long limit) {
            this.limit = limit;
            return this;
        }

        public String sql() {
            return db.builder.sql(this);
        }

        public Query where(String field, Object value) {
            if(value == null) return where(field + " IS NULL");
            return where(field + " = " + db.builder.escape(value));
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
            for(int i = 0; i < names.length; i++) names[i] = meta.getColumnName(i + 1).toLowerCase();
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
        SqlBuilder builder = null;
        switch(type) {
        case "h2":
            url = name;
        break;
        case "postgresql":
            schema = "public";
            builder = new PostgresqlBuilder();
            if(port <= 0) port = 5432;
        break;
        case "mysql":
            if(port <= 0) port = 3306;
        break;
        case "oracle":
            pad = "thin:@";
            schema = user.toUpperCase();
            builder = new OracleBuilder();
            if(port <= 0) port = 1521;
        break;
        case "sqlserver":
            pad2 = ";database=";
            schema = "dbo";
            builder = new SqlserverBuilder();
            if(port <= 0) port = 1433;
        break;
        }
        if(host == null) host = "localhost";
        if(url == null) url = pad + host + ":" + port + pad2 + name;
        if(builder == null) builder = new SqlBuilder();
        url = "jdbc:" + type + ":" + url;
        log("connecting... " + url);
        return new Db(DriverManager.getConnection(url, user, password), builder, schema);
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

    public static class SqlBuilder {
        public String sql(Query q) {
            StringBuffer sql = new StringBuffer();
            sql.append("SELECT ").append(q.fields == null ? "*" : join("", q.fields, ", "));
            sql.append(" FROM ").append(q.table);
            sql.append(join(" WHERE ", q.wheres, " AND "));
            sql.append(join(" ORDER BY ", q.orders, ", "));
            if(q.limit > 0 || q.offset > 0) {
                long limit = q.limit <= 0 ? Integer.MAX_VALUE : q.limit;
                sql.append(" LIMIT ").append(q.offset).append(", ").append(limit);
            }
            return sql.toString();
        }

        public String countSql(String sql) {
            return "SELECT COUNT(*) FROM (" + sql + ") T__";
        }

        public String fn(String function, String... args) {
            return function + "(" + join("", Arrays.asList(args), ", ") + ")";
        }

        public String escape(Object value) {
            if(value == null) return "NULL";
            if(value instanceof Date) return "TIMESTAMP '" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value) + "'";
            if(value instanceof Number) return String.valueOf(value);
            return "'" + value.toString().replace("'", "''") + "'";
        }

        public Object type(Column column) {
            if(Date.class.isAssignableFrom(column.type)) return "DATE";
            if(Number.class.isAssignableFrom(column.type)) return "INTEGER";
            return "VARCHAR(" + (column.length > 0 ? column.length : 255) + ")";
        }
    }

    public static class PostgresqlBuilder extends SqlBuilder {
        @Override
        public String sql(Query q) {
            StringBuffer sql = new StringBuffer();
            sql.append("SELECT ").append(q.fields == null ? "*" : join("", q.fields, ", "));
            sql.append(" FROM ").append(q.table);
            sql.append(join(" WHERE ", q.wheres, " AND "));
            sql.append(join(" ORDER BY ", q.orders, ", "));
            if(q.limit > 0) sql.append(" LIMIT ").append(q.limit);
            if(q.offset > 0) sql.append(" OFFSET ").append(q.offset);
            return sql.toString();
        }

        @Override
        public String fn(String function, String... args) {
            switch(function) {
            case "YEAR":
            case "MONTH":
            case "DAY":
                return "DATE_PART('" + function + "'" + join(", ", Arrays.asList(args), ", ") + ")";
            }
            return function + "(" + join("", Arrays.asList(args), ", ") + ")";
        }
    }
    public static class SqlserverBuilder extends SqlBuilder {
        @Override
        public String sql(Query q) {
            StringBuffer sql = new StringBuffer();
            sql.append("SELECT ");
            if(q.limit > 0 && q.offset <= 0) sql.append("TOP ").append(q.limit).append(" ");
            sql.append(q.fields == null ? "*" : join("", q.fields, ", "));
            String orderBy = join(" ORDER BY ", q.orders, ",");
            boolean range = q.offset > 0;
            if(range) {
                String select = sql.toString();
                if(orderBy.isEmpty()) orderBy = " ORDER BY " + (q.fields == null ? "id" : q.fields.get(0));
                sql.append(" FROM (").append(select).append(", ROW_NUMBER()");
                if(!orderBy.isEmpty()) sql.append(" OVER(" + orderBy.substring(1) + ")");
                sql.append(" N__ FROM ");
            } else {
                sql.append(" FROM ");
            }
            sql.append(q.table).append(join(" WHERE ", q.wheres, " AND "));
            if(range) {
                sql.append(") T__ WHERE N__");
                if(q.limit <= 0) sql.append(" > ").append(q.offset);
                else sql.append(" BETWEEN ").append(q.offset + 1).append(" AND ").append(q.offset + q.limit);
                sql.append(" ORDER BY N__");
            } else {
                sql.append(orderBy);
            }
            return sql.toString();
        }

        @Override
        public String escape(Object value) {
            if(value != null && value instanceof Date) return "'" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value) + "'";
            return super.escape(value);
        }

        @Override
        public String countSql(String sql) {
            int orderBy = sql.lastIndexOf("ORDER BY");
            if(orderBy > 0 && sql.indexOf(" TOP ") < 0) sql = sql.substring(0, orderBy);
            return "SELECT COUNT(*) FROM (" + sql + ") T__";
        }
    }

    public static class OracleBuilder extends SqlBuilder {
        @Override
        public String fn(String function, String... args) {
            switch(function) {
            case "YEAR":
            case "MONTH":
            case "DAY":
                return "EXTRACT(" + function + " FROM " + join("", Arrays.asList(args), ", ") + ")";
            }
            return function + "(" + join("", Arrays.asList(args), ", ") + ")";
        }
        @Override
        public String sql(Query q) {
            StringBuffer sql = new StringBuffer();
            sql.append("SELECT ");
            sql.append(q.fields == null ? "*" : join("", q.fields, ", "));
            String orderBy = join(" ORDER BY ", q.orders, ",");
            boolean range = q.limit > 0 || q.offset > 0;
            if(range) {
                String select = sql.toString();
                sql.append(" FROM (").append(select).append(", ROWNUM N__ FROM ");
            } else {
                sql.append(" FROM ");
            }
            sql.append(q.table).append(join(" WHERE ", q.wheres, " AND "));
            sql.append(orderBy);
            if(range) {
                sql.append(") WHERE N__");
                if(q.offset <= 0) sql.append(" <= ").append(q.limit);
                else if(q.limit <= 0) sql.append(" > ").append(q.offset);
                else sql.append(" BETWEEN ").append(q.offset + 1).append(" AND ").append(q.offset + q.limit);
                sql.append(" ORDER BY N__");
            }
            return sql.toString();
        }
    }

    public static class Column {
        String name;
        String display;
        Class<?> type;
        int length;
        boolean nullable;
        Object value;
        public Column(String name) {
            this.name = name;
        }
        public Column date() {
            type = Date.class;
            return this;
        }
        public Column integer() {
            type = Long.class;
            return this;
        }
        public Column text() {
            type = String.class;
            return this;
        }
        public Column text(int length) {
            type = String.class;
            this.length = length;
            return this;
        }
        public Column type(Class<?> type) {
            this.type = type;
            return this;
        }
        public Column display(String name) {
            display = name;
            return this;
        }
        public Column length(int length) {
            this.length = length;
            return this;
        }
    }

    public void create(String table, int primary, Column... columns) throws SQLException {
        StringBuffer sql = new StringBuffer("CREATE TABLE ");
        sql.append(table).append("(");
        String pad = "";
        String primaryKey = "";
        for(Column column : columns) {
            if(primary > 0) {
                primaryKey += pad + column.name;
                primary--;
            }
            sql.append(pad).append(column.name).append(" ").append(builder.type(column));
            pad = ", ";
        }
        if(!primaryKey.isEmpty()) sql.append(", PRIMARY KEY(").append(primaryKey).append(")");
        execute(sql.append(")").toString());
    }

    public void drop(String table) throws SQLException {
        execute("DROP TABLE " + table);
    }
}
