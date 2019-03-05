package bt.db.listener.evnt;

import bt.db.DatabaseAccess;

/**
 * Fired by the {@link DatabaseAccess} implementation when an UPDATE trigger is detected.
 * 
 * @author &#8904
 */
public class UpdateEvent extends DatabaseChangeEvent
{
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
    public UpdateEvent(DatabaseAccess sourceDB, String table, String idFieldName, long id, String... data)
    {
        super(sourceDB, table, idFieldName, id, data);
    }
}