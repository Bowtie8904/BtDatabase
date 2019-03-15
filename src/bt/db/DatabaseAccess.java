package bt.db;

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

import bt.db.config.DatabaseConfiguration;
import bt.db.constants.SqlType;
import bt.db.func.Sql;
import bt.db.listener.DatabaseListener;
import bt.db.listener.DeleteListener;
import bt.db.listener.InsertListener;
import bt.db.listener.UpdateListener;
import bt.db.statement.Alter;
import bt.db.statement.Create;
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
import bt.types.SimpleTripple;
import bt.types.Tripple;
import bt.utils.console.ConsoleRowList;
import bt.utils.id.StringID;
import bt.utils.log.Logger;

/**
 * Base class for databases.
 * 
 * @author &#8904
 */
public abstract class DatabaseAccess<T extends DatabaseAccess> implements Killable
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
    private String id;

    /** The connection to the database. */
    protected Connection connection;

    /** A list of all registered {@link DatabaseListener}s that will be notified on their specififc triggers. */
    protected List<DatabaseListener> listeners = new ArrayList<>();

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
        InstanceKiller.closeOnShutdown(this, 1);
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
            instances.put(this.id, this);
        }
        createDefaultProcedures();
        log.print(this, "Setup database instance " + this.id);
    }

    /**
     * Gets the runtime unique ID of this instance.
     * 
     * @return The String ID.
     */
    public String getID()
    {
        return this.id;
    }

    /**
     * Checks whether this instance already has an ID assigned and requests a new one if it does not.
     */
    protected void checkID()
    {
        this.id = getProperty("instanceID");

        if (this.id == null)
        {
            this.id = StringID.uniqueID();
            setProperty("instanceID", this.id);
        }
    }

    /**
     * Creates the database if it does not exist yet.
     * 
     * <p>
     * This will simply attempt to create a connection to the database which will create it automatically, if it does
     * not exist yet. This does only work with local databases.
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
     * Gets a list containing all registered {@link DatabaseListener}s.
     * 
     * @return
     */
    protected List<DatabaseListener> getListeners()
    {
        return listeners;
    }

    /**
     * Registeres the given delete listener to this database instance.
     * 
     * @param listener
     */
    public void registerListener(DeleteListener listener)
    {
        this.listeners.add(listener);
        log.print(this,
                "Registered database delete listener of type '" + listener.getClass().getName() + "' to instance "
                + this.getID() + ".");
    }

    /**
     * Registeres the given insert listener to this database instance.
     * 
     * @param listener
     */
    public void registerListener(InsertListener listener)
    {
        this.listeners.add(listener);
        log.print(this,
                "Registered database insert listener of type '" + listener.getClass().getName() + "' to instance "
                + this.getID() + ".");
    }

    /**
     * Registeres the given update listener to this database instance.
     * 
     * @param listener
     */
    public void registerListener(UpdateListener listener)
    {
        this.listeners.add(listener);
        log.print(this,
                "Registered database update listener of type '" + listener.getClass().getName() + "' to instance "
                + this.getID() + ".");
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
                    instances.remove(this.id);
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
        System.out.println("\nColumn information of table: " + table.toUpperCase());
        List<Tripple<String, String, String>> columnInfo = columnInfo(table);

        ConsoleRowList rows = new ConsoleRowList(20, 18, TableColumn.COMMENT_SIZE + 2);
        rows.addTitle(true, "Column", "Type", "Comment");

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
                .from("column_comments")
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
     * Defines the tables that should be created.
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