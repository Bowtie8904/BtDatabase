package bt.db.listener.evnt;

import bt.db.DatabaseAccess;

/**
 * Supertype of all events fired by the {@link DatabaseAccess} implementation in case of an insert, update or delete
 * trigger.
 * 
 * @author &#8904
 */
public class DatabaseChangeEvent
{
    /** The name of the table that the event occurred on. */
    protected String table;

    /** The name of the identifying field which can be used to select the changed row. */
    protected String idFieldName;

    /** The value of the identifying field which can be used to select the changed row. */
    protected long id;

    /** The {@link DatabaseAccess} instance that fired this event. */
    protected DatabaseAccess sourceDb;

    /** Additional custom data sent by the trigger. */
    protected String[] data = new String[] {};

    /**
     * Creates a new instance and sets the fields.
     * 
     * @param sourceDB
     *            The {@link DatabaseAccess} instance that fired this event.
     * @param table
     *            The name of the table that the event occurred on.
     * @param idFieldName
     *            The name of the identifying field which can be used to select the changed row.
     * @param id
     *            The value of the identifying field which can be used to select the changed row.
     * @param data
     *            Additional custom data sent by the trigger.
     */
    public DatabaseChangeEvent(DatabaseAccess sourceDB, String table, String idFieldName, long id, String... data)
    {
        this.table = table;
        this.id = id;
        this.idFieldName = idFieldName;
        this.sourceDb = sourceDB;
        this.data = data;
    }

    /**
     * Returns the name of the table that this event ocurred on.
     * 
     * @return The name of the table.
     */
    public String getTable()
    {
        return this.table;
    }

    /**
     * Returns the value of the identifying field which can be used to select the changed row.
     * 
     * @return The id value.
     */
    public long getID()
    {
        return this.id;
    }

    /**
     * Returns the name of the identifying field which can be used to select the changed row.
     * 
     * @return The name of the id field.
     */
    public String getIDFieldName()
    {
        return this.idFieldName;
    }

    /**
     * Returns the {@link DatabaseAccess} instance that fired this event.
     * 
     * @return The database instance.
     */
    public DatabaseAccess getSourceDatabase()
    {
        return this.sourceDb;
    }

    /**
     * Sets the {@link DatabaseAccess} instance that fired this event.
     * 
     * @param sourceDb
     */
    public void setSourceDatabase(DatabaseAccess sourceDb)
    {
        this.sourceDb = sourceDb;
    }

    /**
     * Returns the size of the additional data sent with the trigger.
     * 
     * @return The size of the data.
     */
    public int getDataSize()
    {
        return this.data.length;
    }

    /**
     * Returns the additional data that was sent by the trigger.
     * 
     * <p>
     * This array will never be null. It will be empty in most cases though.
     * </p>
     * 
     * @return The additional data array.
     */
    public String[] getData()
    {
        return this.data;
    }
}