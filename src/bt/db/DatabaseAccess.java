package bt.db;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import bt.db.config.DatabaseConfiguration;
import bt.db.constants.SqlType;
import bt.db.constants.SqlValue;
import bt.db.func.Sql;
import bt.db.listener.evnt.DatabaseChangeEvent;
import bt.db.listener.evnt.DeleteEvent;
import bt.db.listener.evnt.InsertEvent;
import bt.db.listener.evnt.UpdateEvent;
import bt.db.statement.Alter;
import bt.db.statement.Create;
import bt.db.statement.clause.ColumnEntry;
import bt.db.statement.clause.TableColumn;
import bt.db.statement.impl.DeleteStatement;
import bt.db.statement.impl.DropStatement;
import bt.db.statement.impl.InsertStatement;
import bt.db.statement.impl.SelectStatement;
import bt.db.statement.impl.UpdateStatement;
import bt.db.statement.result.SqlResult;
import bt.db.statement.result.SqlResultSet;
import bt.db.store.SqlEntry;
import bt.runtime.InstanceKiller;
import bt.runtime.Killable;
import bt.runtime.evnt.Dispatcher;
import bt.types.SimpleTripple;
import bt.types.Tripple;
import bt.utils.collections.array.Array;
import bt.utils.console.ConsoleTable;
import bt.utils.files.FileUtils;
import bt.utils.id.StringID;
import bt.utils.log.Logger;

/**
 * Base class for databases.
 * 
 * @author &#8904
 */
public abstract class DatabaseAccess implements Killable
{
    /**
     * The connection String for a default database located at a ./db folder.
     * <p>
     * <b>jdbc:derby:./db;create=true;useUnicode=true&characterEncoding=utf8&autoReconnect=true</b>
     * </p>
     */
    public static final String DEFAULT_LOCAL_DB = "jdbc:derby:./db;create=true;useUnicode=true&characterEncoding=utf8&autoReconnect=true";

    /**
     * The name of the comment table used for comments on table columns.
     * <p>
     * <b>column_comments</b>
     * </p>
     */
    public static final String COMMENT_TABLE = "column_comments";

    /**
     * The name of the properties table used to store database specific key value pairs.
     * <p>
     * <b>sys_properties</b>
     * </p>
     */
    public static final String PROPERTIES_TABLE = "sys_properties";

    /** The map of all currently active DatabaseAccess instances, mapped by their runtime unique ID. */
    protected static Map<String, DatabaseAccess> instances = new HashMap<>();

    /** The Logger for all database related logging. Writing to 'logs/database_log.log'. */
    public static Logger log = new Logger("logs/database_log.log");

    /** The URL of the database. */
    protected final String dbConnectionString;

    /**
     * The runtime unique ID of this database instance. Used to map this instance in {@link #instances}. This ID will
     * change with every program execution and is only stored during runtime.
     */
    private String instanceID;

    /** The connection to the database. */
    protected Connection connection;

    /** The dispatcher used to distribute insert, update and delete trigger events to the corresponding listeners. */
    protected Dispatcher triggerDispatcher;

    /**
     * Gets the instance with the given ID.
     * 
     * @param id
     *            The ID of the instance to return.
     * @return The instance that was mapped to the given ID or null if no such instance was found.
     */
    protected static DatabaseAccess getInstance(String id)
    {
        return instances.get(id);
    }

    /**
     * Creates a new instance.
     * 
     * <p>
     * This instance is added to the {@link InstanceKiller} with a priority of 1.
     * </p>
     * 
     * <p>
     * {@link #setup()} needs to be called to finish up the initialization.
     * </p>
     * 
     * @param dbURL
     *            The URL for creation or connection of the database.
     */
    protected DatabaseAccess(String dbURL)
    {
        this.dbConnectionString = dbURL;
        log.registerSource(this, getClass().getName());
        InstanceKiller.killOnShutdown(this, 1);
        this.triggerDispatcher = new Dispatcher();
    }

    /**
     * Creates a new instance.
     * 
     * <p>
     * This instance is added to the {@link InstanceKiller} with a priority of 1.
     * </p>
     * 
     * <p>
     * {@link #setup()} needs to be called to finish up the initialization.
     * </p>
     * 
     * @param configuration
     *            The configuration for this DB's connection.
     */
    protected DatabaseAccess(DatabaseConfiguration configuration)
    {
        this(configuration.toString());
    }

    /**
     * Sets the database up.
     * 
     * <p>
     * The methods being called for the setup are (in order):
     * <ul>
     * <li>{@link #createDatabase()}</li>
     * <li>{@link #createCommentTable()}</li>
     * <li>{@link #createPropertiesTable()}</li>
     * <li>{@link #checkID()}</li>
     * <li>The instance is added to {@link #instances} with the unique id as key</li>
     * <li>{@link #createDefaultProcedures()}</li>
     * </ul>
     * </p>
     */
    protected void setup()
    {
        createDatabase();
        createCommentTable();
        createPropertiesTable();
        checkID();
        synchronized (DatabaseAccess.class)
        {
            instances.put(this.instanceID, this);
        }
        createDefaultProcedures();
        log.print(this, "Setup database instance " + this.instanceID);
        log.print(this, "Using connection string: " + this.dbConnectionString);
    }

    /**
     * Gets the runtime unique ID of this instance.
     * 
     * @return The String ID.
     */
    public String getInstanceID()
    {
        return this.instanceID;
    }

    /**
     * Checks whether this instance already has an ID assigned and requests a new one if it does not.
     * 
     * <p>
     * The assigned id will be saved to the {@link #PROPERTIES_TABLE} with the key 'instanceID'.
     * </p>
     */
    protected void checkID()
    {
        this.instanceID = getProperty("instanceID");

        if (this.instanceID == null)
        {
            this.instanceID = StringID.uniqueID();
            setProperty("instanceID", this.instanceID);
        }
    }

    /**
     * Creates the database if it does not exist yet.
     * 
     * <p>
     * This will simply attempt to create a connection to the database which will create it automatically, if it does
     * not exist yet and the option was set in the configuration. This does only work with local databases.
     * </p>
     */
    protected void createDatabase()
    {
        try (Connection connection = DriverManager.getConnection(this.dbConnectionString))
        {
            log.print(this, "Loaded database.");
        }
        catch (SQLException e)
        {
            log.print(this, e);
        }
    }

    /**
     * Gets the dispatcher instance whichs subscribers are the listeners for insert, delete and update triggers.
     * 
     * @return
     */
    protected Dispatcher getTriggerDispatcher()
    {
        return this.triggerDispatcher;
    }

    /**
     * Registers the given runnable as a listener for the given event type (only {@link InsertEvent},
     * {@link DeleteEvent} and {@link UpdateEvent} are accepted.
     * 
     * <p>
     * If no tables are specified, the given listener will be called every time the given event type is dispatched. If
     * tables are specified the listener is only called for triggers that involve one of the given tables.
     * </p>
     * <br>
     * <p>
     * Example:<br>
     * 
     * <pre>
     * // will be called for every dispatched InsertEvent
     * registerListener(InsertEvent.class, myListener::onInsert);
     * </pre>
     * 
     * <pre>
     * // will be called only for inserts in the tables with the names 'myTable' or 'myTable2'
     * registerListener(InsertEvent.class, myListener::onInsert, "myTable", "myTable2");
     * </pre>
     * </p>
     * 
     * @param listenFor
     *            The event type to listen for.
     * @param listener
     *            The runnable that should be called when the given event type is dispatched.
     * @param tables
     *            The tables for which the listener should be called.
     * 
     * @return The consumer that the given listener was wrapped in. This consumer can be used to unregister the given
     *         listener.
     */
    public <T extends DatabaseChangeEvent> Consumer<T> registerListener(Class<T> listenFor, Runnable listener,
            String... tables)
    {
        var cons = new Consumer<T>()
        {
            @Override
            public void accept(T event)
            {
                listener.run();
            }
        };

        return registerListener(listenFor, cons, tables);
    }

    /**
     * Registers the given consumer as a listener for the given event type (only {@link InsertEvent},
     * {@link DeleteEvent} and {@link UpdateEvent} are accepted.
     * 
     * <p>
     * If no tables are specified, the given listener will be called every time the given event type is dispatched. If
     * tables are specified the listener is only called for triggers that involve one of the given tables.
     * </p>
     * <br>
     * <p>
     * Example:<br>
     * 
     * <pre>
     * // will be called for every dispatched InsertEvent
     * registerListener(InsertEvent.class, myListener::onInsert);
     * </pre>
     * 
     * <pre>
     * // will be called only for inserts in the tables with the names 'myTable' or 'myTable2'
     * registerListener(InsertEvent.class, myListener::onInsert, "myTable", "myTable2");
     * </pre>
     * </p>
     * 
     * @param listenFor
     *            The event type to listen for.
     * @param listener
     *            The consumer method that should be called when the given event type is dispatched.
     * @param tables
     *            The tables for which the listener should be called.
     * 
     * @return The consumer that the given listener was wrapped in. This consumer can be used to unregister the given
     *         listener.
     */
    public <T extends DatabaseChangeEvent> Consumer<T> registerListener(Class<T> listenFor, Consumer<T> listener,
            String... tables)
    {
        var cons = new Consumer<T>()
        {
            @Override
            public void accept(T event)
            {
                boolean dispatch = tables == null || tables.length == 0; // no tables specified = always dispatch

                if (!dispatch)
                {
                    for (String table : tables)
                    {
                        if (table.equalsIgnoreCase(event.getTable()))
                        {
                            dispatch = true;
                            break;
                        }
                    }
                }

                if (dispatch)
                {
                    listener.accept(event);
                }
            }
        };

        this.triggerDispatcher.subscribeTo(listenFor, cons);

        log.print(this,
                "Registered database listener of type '" + listener.getClass().getName() + "' for '"
                        + listenFor.getName() + "' to instance "
                        + this.getInstanceID() + ".");

        return cons;
    }

    /**
     * Unregisters the given listener from the given event type.
     * 
     * <p>
     * The listener will no longer be called on dispatched events of the given type.
     * </p>
     * 
     * @param type
     * @param listener
     */
    public <T extends DatabaseChangeEvent> void unregisterListener(Class<T> type, Consumer<T> listener)
    {
        if (this.triggerDispatcher.unsubscribeFrom(type, listener))
        {
            log.print(this,
                    "Unregistered database listener of type '" + listener.getClass().getName() + "' for '"
                            + type.getName() + "' to instance "
                            + this.getInstanceID() + ".");
        }
    }

    /**
     * Gets a connection to the database.
     * 
     * @param autocommit
     *            Indicates whether the returned connection should use autocommit or not.
     * @return The connection.
     */
    public Connection getConnection(boolean autocommit)
    {
        try
        {
            if (this.connection == null || (this.connection != null && this.connection.isClosed()))
            {
                try
                {
                    this.connection = DriverManager.getConnection(dbConnectionString);
                    this.connection.setAutoCommit(autocommit);
                }
                catch (SQLException e)
                {
                    log.print(this, e);
                }
            }
        }
        catch (SQLException e)
        {
            log.print(this, e);
        }

        return this.connection;
    }

    /**
     * Gets a connection to the database.
     * 
     * <p>
     * The returned connection has auto commit turned off.
     * </p>
     * 
     * @return The connection.
     */
    public Connection getConnection()
    {
        return getConnection(false);
    }

    /**
     * Closes the connection to the database if it exists.
     * 
     * <p>
     * This will commit the current transaction if the connection is not in auto commit mode.
     * </p>
     */
    @Override
    public void kill()
    {
        try
        {
            if (this.connection != null && !this.connection.isClosed())
            {
                commit();
                this.connection.close();
                synchronized (DatabaseAccess.class)
                {
                    instances.remove(this.instanceID);
                }
                log.print(this, "Closed database.");
            }
        }
        catch (SQLException e)
        {
            log.print(this, e);
        }
    }

    /**
     * Adds or updates the given property key with the given value.
     * 
     * @param key
     *            The unique key of the property.
     * @param value
     *            The value of the property.
     */
    public void setProperty(String key, String value)
    {
        insert().into(PROPERTIES_TABLE)
                .set("property_key", key)
                .set("property_value", value)
                .onDuplicateKey(
                        update(PROPERTIES_TABLE)
                                .set("property_value", value)
                                .where("property_key").equals(key)
                                .commit())
                .commit()
                .execute();
    }

    /**
     * Gets the property value for the given key from the {@link #PROPERTIES_TABLE}.
     * 
     * @param key
     *            The key of the property to return.
     * @return The value bound to the given key or null if the key was not found or null.
     */
    public String getProperty(String key)
    {
        if (key == null)
        {
            return null;
        }

        SqlResultSet result = select(Sql.column("property_value").as("value"))
                .from(PROPERTIES_TABLE)
                .where("property_key").equals(key)
                .onLessThan(1, (i, set) ->
                {
                    return null;
                })
                .execute();

        if (result != null && result.size() > 0)
        {
            return result.get(0).getString("value");
        }

        return null;
    }

    /**
     * Creates the {@link #PROPERTIES_TABLE} if it does not exist yet.
     */
    protected void createPropertiesTable()
    {
        int success = create().table(PROPERTIES_TABLE)
                .column("property_key", SqlType.VARCHAR).size(200).notNull().unique()
                .comment("The unique key of the key-value mapping.").add()

                .column("property_value", SqlType.VARCHAR).size(500)
                .comment("The value of the key-value mapping.").add()

                .createDefaultTriggers(false)
                .onFail((s, e) ->
                {
                    return 0;
                })
                .execute();

        if (success == 1)
        {
            log.print(this, "Created properties table.");
            commit();
        }
    }

    /**
     * Creates the {@link #COMMENT_TABLE} if it does not exist yet.
     */
    protected void createCommentTable()
    {
        int success = create().table(COMMENT_TABLE)
                .column("table_Name", SqlType.VARCHAR).size(50).primaryKey().notNull().comment("The name of the table.")
                .add()

                .column("column_Name", SqlType.VARCHAR).size(50).primaryKey().notNull()
                .comment("The name of the column that this comment is for.").add()

                .column("column_Comment", SqlType.VARCHAR).size(TableColumn.COMMENT_SIZE)
                .comment("The comment for this column.").add()

                .createDefaultTriggers(false)
                .onFail((s, e) ->
                {
                    return 0;
                })
                .execute();

        if (success == 1)
        {
            log.print(this, "Created comment table.");
            commit();
        }
    }

    /**
     * Executes the given raw SQL String of a modify (update, insert, delete, alter, drop, ...) statement.
     * 
     * @param sql
     *            The raw SQL String to execute.
     * @return The return value of the statement execution. See {@link Statement#executeUpdate(String)}.
     */
    public int executeUpdate(String sql)
    {
        try (Statement statement = getConnection().createStatement())
        {
            return statement.executeUpdate(sql);
        }
        catch (SQLException e)
        {
            log.print(this, e);
            return -1;
        }
    }

    /**
     * Executes the given raw SQL String of a select statement.
     * 
     * @param sql
     *            The raw SQL String to execute.
     * @return The {@link SqlResultSet} resulting from the query or null if an error occured.
     */
    public SqlResultSet executeQuery(String sql)
    {
        try (Statement statement = getConnection().createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY))
        {
            return new SqlResultSet(statement.executeQuery(sql));
        }
        catch (SQLException e)
        {
            log.print(this, e);
            return null;
        }
    }

    /**
     * Rolls back the current transaction.
     * 
     * <p>
     * This method has no effect on a connection that is in auto commit mode.
     * </p>
     */
    public void rollback()
    {
        try
        {
            if (this.connection != null && !this.connection.getAutoCommit())
            {
                this.connection.rollback();
                log.print(this, "Rolled back transaction.");
            }
        }
        catch (SQLException e)
        {
            log.print(this, e);
        }
    }

    /**
     * Commits the current transaction.
     * 
     * <p>
     * This method has no effect on a connection that is in auto commit mode.
     * </p>
     */
    public void commit()
    {
        try
        {
            if (this.connection != null && !this.connection.getAutoCommit())
            {
                this.connection.commit();
                log.print(this, "Committed transaction.");
            }
        }
        catch (SQLException e)
        {
            log.print(this, e);
        }
    }
    
    /**
     * Imports all data formatted as insert statements inside the given file.
     * 
     * <p>
     * There can only be one statement in each line and a statement can not exceed over multiple lines.
     * </p>
     * 
     * <p>
     * All lines in the file will be read and are expected to be DML statements. Invalid SQL and inserts that violate
     * unique constraints will be skipped.
     * </p>
     * 
     * @param importFile
     *            The file from where to import.
     */
    public void importData(File importFile)
    {
        String[] lines = FileUtils.readLines(importFile);
        int count = 0;

        for (String statement : lines)
        {
            count += executeUpdate(statement);
        }

        log.print(this, "Imported " + count + " rows from " + importFile.getAbsolutePath() + ".");
    }

    /**
     * Exports all entries in the given table.
     * 
     * <p>
     * The entries are exported as insert statements and saved to the given file. The insert statements will include all
     * columns of the given table except the ones that are passed in excludeColumns (case insensitive).
     * </p>
     * 
     * @param table
     *            The name of the table whichs data should be exported.
     * @param exportFile
     *            The file to save the insert statements to. If the file does not exist it will be created.
     * @param excludeColumns
     *            An array of column names which should not be included in the exported insert statements. Keep in mind
     *            that all tables created by this library will have an id column 'DEFAULT_ID' unless a fitting id column
     *            was explicitly added. These columns will be 'generated always as identity' meaning that values can't
     *            be manually added or changed, which means they can't be used in insert statements.
     */
    public void exportData(String table, File exportFile, String... excludeColumns)
    {
        SqlResultSet set = select()
                .from(table)
                .onLessThan(1, (num, res) ->
                {
                    return res;
                }).execute();

        if (!exportFile.exists())
        {
            try
            {
                exportFile.getParentFile().mkdirs();
                exportFile.createNewFile();
            }
            catch (Exception e)
            {
                log.print(this, e);
                return;
            }
        }

        try (PrintWriter writer = new PrintWriter(new FileWriter(exportFile)))
        {
            for (SqlResult row : set)
            {
                writer.println(row.export(table, excludeColumns));
            }

            log.print(this, "Exported " + set.size() + " rows to " + exportFile.getAbsolutePath() + ".");
        }
        catch (IOException e)
        {
            log.print(this, e);
        }
    }

    /**
     * Exports all entries from all user table.
     * 
     * <p>
     * The entries are exported as insert statements and saved to separate files in <i>./EXPORT_DATA</i> (Will be
     * created if it does not exist). The files will have the name '<i>NAME_OF_TABLE.sql</i>' The insert statements will
     * include all columns of the respective table except the ones that are passed in excludeColumns (case insensitive).
     * </p>
     * 
     * @param excludeColumns
     *            An array of column names which should not be included in the exported insert statements. Keep in mind
     *            that all tables created by this library will have an id column 'DEFAULT_ID' unless a fitting id column
     *            was explicitly added. These columns will be 'generated always as identity' meaning that values can't
     *            be manually added or changed, which means they can't be used in insert statements.
     */
    public void exportData(String... excludeColumns)
    {
        SqlResultSet set = select("tablename")
                .from(SqlValue.SYSTABLE)
                .where("tabletype").equals("T")
                .onLessThan(1, (num, res) ->
                {
                    return res;
                }).execute();

        String tableName;

        for (SqlResult table : set)
        {
            tableName = table.getString("tableName");
            exportData(tableName, new File("./DATA_EXPORT/" + tableName + ".sql"), excludeColumns);
        }
    }

    /**
     * Creates a new {@link DropStatement}.
     * 
     * @return The statement.
     */
    public DropStatement drop()
    {
        return new DropStatement(this);
    }

    /**
     * Creates a new {@link Create}statement.
     * 
     * @return The statement.
     */
    public Create create()
    {
        return new Create(this);
    }

    /**
     * Creates a new {@link Alter}statement.
     * 
     * @return The statement.
     */
    public Alter alter()
    {
        return new Alter(this);
    }

    /**
     * Creates a select statement which selects all columns (*).
     * 
     * @return The statement.
     */
    public SelectStatement select()
    {
        return new SelectStatement(this);
    }

    /**
     * Creates a select statement which selects the toString() values of the given objects.
     * 
     * <p>
     * This method is meant to be used with {@link ColumnEntry} parameters to represent table.column pairs in selects
     * from multiple tables. Call {@link Sql#column(String, String)} to create a new columnentry.
     * </p>
     * 
     * @return The statement.
     */
    public SelectStatement select(Object... values)
    {
        String[] columns = new String[values.length];

        for (int i = 0; i < values.length; i ++ )
        {
            columns[i] = values[i].toString();
        }

        return new SelectStatement(this, columns);
    }

    /**
     * Creates a select statement which selects the given columns.
     * 
     * @return The statement.
     */
    public SelectStatement select(String... columns)
    {
        return new SelectStatement(this, columns);
    }

    /**
     * Creates a delete statement.
     * 
     * @return The statement.
     */
    public DeleteStatement delete()
    {
        return new DeleteStatement(this);
    }

    /**
     * Creates an insert statement.
     * 
     * @return The statement.
     */
    public InsertStatement insert()
    {
        return new InsertStatement(this);
    }

    /**
     * Creates an update statement for the given table.
     * 
     * @param table
     *            The table to update.
     * @return The statement.
     */
    public UpdateStatement update(String table)
    {
        return new UpdateStatement(this, table);
    }

    /**
     * Persists the given object into the database by using the provided static methods of {@link SqlEntry} or by
     * calling the persist method of SqlEntry implementations.
     * 
     * @param entry
     */
    public void persist(Object entry)
    {
        if (entry instanceof SqlEntry)
        {
            ((SqlEntry)entry).persist(this);
        }
        else
        {
            SqlEntry.persist(this, entry);
        }
    }

    /**
     * Initializes the given object into the database by using the provided static methods of {@link SqlEntry} or by
     * calling the init method of SqlEntry implementations.
     * 
     * @param entry
     */
    public void init(Object entry)
    {
        if (entry instanceof SqlEntry)
        {
            ((SqlEntry)entry).init(this);
        }
        else
        {
            SqlEntry.init(this, entry);
        }
    }

    /**
     * Formats a String table containing information about the columns of the table with the given name.
     * 
     * <p>
     * The contained information are:
     * <ul>
     * <li>Column name</li>
     * <li>Data type</li>
     * <li>Comment</li>
     * </ul>
     * 
     * @param table
     * @return
     */
    public String info(String table)
    {
        List<Tripple<String, String, String>> columnInfo = columnInfo(table);

        ConsoleTable rows = new ConsoleTable(20, 18, TableColumn.COMMENT_SIZE + 2);
        rows.setTitle(true, Array.of("Column", "Type", "Comment"));

        for (Tripple<String, String, String> column : columnInfo)
        {
            rows.addRow(column.getKey(), column.getFirstValue(),
                    column.getSecondValue() == null ? "" : column.getSecondValue());
        }

        return rows.toString();
    }

    private List<Tripple<String, String, String>> columnInfo(String table)
    {
        List<Entry<String, String>> columnTypes = select()
                .from(table)
                .onLessThan(1, (num, res) ->
                {
                    return res;
                })
                .execute().getColumnTypes();
        Map<String, String> typeMap = new HashMap<>();

        for (Entry<String, String> column : columnTypes)
        {
            typeMap.put(column.getKey().toUpperCase(), column.getValue());
        }

        List<Tripple<String, String, String>> columnInfo = new ArrayList<>();

        SqlResultSet set = select()
                .from(COMMENT_TABLE)
                .where("table_name").equals(table.toUpperCase())
                .onLessThan(1, (num, res) ->
                {
                    return res;
                })
                .execute();
        
        for (SqlResult result : set)
        {
            String col = result.getString("column_name").toUpperCase();
            String type = typeMap.get(col);
            columnInfo.add(new SimpleTripple<String, String, String>(col, type, result.getString("column_comment")));
        }

        return columnInfo;
    }

    /**
     * Calls {@link #onInsert(InsertEvent)} of the database with the given instanceID.
     * 
     * @param instanceID
     *            The instanceID of the database that is concerned.
     * @param table
     *            The table that has been inserted into.
     * @param idFieldName
     *            The name of the identity field inside the table.
     * @param id
     *            The id (identity value of the new row).
     */
    public synchronized static void onInsert(String instanceID, String table, String idFieldName, long id)
    {
        DatabaseAccess instance = getInstance(instanceID);

        if (instance != null)
        {
            instance.onInsert(new InsertEvent(instance, table, idFieldName, id));
        }
    }

    /**
     * Dispatches the given {@link InsertEvent} to all concerned listeners.
     * 
     * <p>
     * This method exists alongside the static {@link #onInsert(String, String, String, long) onInsert} to allow
     * overriding of this logic.
     * </p>
     * 
     * @param event
     *            The event to dispatch.
     */
    protected void onInsert(InsertEvent event)
    {
        int count = this.triggerDispatcher.dispatch(event);
        log.print(this, "Dispatched insert event to " + count + " listeners.");
    }

    /**
     * Calls {@link #onUpdate(UpdateEvent)} of the database with the given instanceID.
     * 
     * @param instanceID
     *            The instanceID of the database that is concerned.
     * @param table
     *            The table that has been updated.
     * @param idFieldName
     *            The name of the identity field inside the table.
     * @param id
     *            The id (identity value of the updated row).
     */
    public synchronized static void onUpdate(String instanceID, String table, String idFieldName, long id)
    {
        DatabaseAccess instance = getInstance(instanceID);

        if (instance != null)
        {
            instance.onUpdate(new UpdateEvent(instance, table, idFieldName, id));
        }
    }

    /**
     * Dispatches the given {@link UpdateEvent} to all concerned listeners.
     * 
     * <p>
     * This method exists alongside the static {@link #onUpdate(String, String, String, long) onUpdate} to allow
     * overriding of this logic.
     * </p>
     * 
     * @param event
     *            The event to dispatch.
     */
    protected void onUpdate(UpdateEvent event)
    {
        int count = this.triggerDispatcher.dispatch(event);
        log.print(this, "Dispatched update event to " + count + " listeners.");
    }

    /**
     * Calls {@link #onDelete(DeleteEvent)} of the database with the given instanceID.
     * 
     * @param instanceID
     *            The instanceID of the database that is concerned.
     * @param table
     *            The table that has been deleted from.
     * @param idFieldName
     *            The name of the identity field inside the table.
     * @param id
     *            The id (identity value of the deleted row).
     */
    public synchronized static void onDelete(String instanceID, String table, String idFieldName, long id)
    {
        DatabaseAccess instance = getInstance(instanceID);

        if (instance != null)
        {
            instance.onDelete(new DeleteEvent(instance, table, idFieldName, id));
        }
    }

    /**
     * Dispatches the given {@link DeleteEvent} to all concerned listeners.
     * 
     * <p>
     * This method exists alongside the static {@link #onDelete(String, String, String, long) onDelete} to allow
     * overriding of this logic.
     * </p>
     * 
     * @param event
     *            The event to dispatch.
     */
    protected void onDelete(DeleteEvent event)
    {
        int count = this.triggerDispatcher.dispatch(event);
        log.print(this, "Dispatched delete event to " + count + " listeners.");
    }

    /**
     * Defines the tables that should be created.
     * 
     * <p>
     * This is called by the {@link #setup()} of the implementation.
     * </p>
     */
    protected abstract void createTables();

    /**
     * Defines the default procedures used by this implementation.
     * 
     * <p>
     * Those procedures should for example be the trigger procedures for insert, delete and update triggers.
     * </p>
     */
    protected abstract void createDefaultProcedures();
}