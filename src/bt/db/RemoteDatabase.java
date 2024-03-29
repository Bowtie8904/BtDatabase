package bt.db;

import bt.db.config.DatabaseConfiguration;
import bt.db.constants.Generated;
import bt.db.constants.SqlType;
import bt.db.constants.SqlValue;
import bt.db.statement.clause.Column;
import bt.db.statement.result.SqlResult;
import bt.db.statement.result.SqlResultSet;
import bt.log.Log;
import bt.scheduler.Threads;
import bt.utils.Null;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A class which creates and keeps a connection to a remote database.
 *
 * <p>
 * This implementation will not receive triggers directly. Instead it will check for new triggers every n (configurable)
 * seconds. By default it will check for new entries in the RECENT_TRIGGERS table (automatically created) every 3
 * seconds.
 * </p>
 *
 * @author &#8904
 */
public abstract class RemoteDatabase extends DatabaseAccess
{
    protected ScheduledFuture triggerCheck;
    protected long triggerCheckInterval;

    /**
     * Creates a new instance which uses the default local db connection string and a trigger check interval of 3
     * seconds.
     */
    protected RemoteDatabase()
    {
        this(DatabaseAccess.DEFAULT_LOCAL_DB,
             3000);
    }

    /**
     * Creates a new instance which uses the given connection string and a trigger check interval of 3 seconds.
     *
     * @param dbURL The DB connection string.
     */
    protected RemoteDatabase(String dbURL)
    {
        this(dbURL,
             3000);
    }

    /**
     * Creates a new instance which uses the given connection string and a given trigger check interval.
     *
     * @param dbURL                The DB connection string.
     * @param triggerCheckInterval The trigger check interval in milliseconds.
     */
    protected RemoteDatabase(String dbURL, long triggerCheckInterval)
    {
        super(dbURL);
        this.triggerCheckInterval = triggerCheckInterval;
        setup();
        createTriggerTable();
        createTables();
        startTriggerCheck();
    }

    /**
     * Creates a new instance which uses the given configuration and a trigger check interval of 3 seconds..
     *
     * @param configuration
     */
    protected RemoteDatabase(DatabaseConfiguration configuration)
    {
        this(configuration.toString(),
             3000);
    }

    /**
     * Creates a new instance which uses the given configuration and trigger check interval.
     *
     * @param configuration
     * @param triggerCheckInterval
     */
    protected RemoteDatabase(DatabaseConfiguration configuration, long triggerCheckInterval)
    {
        this(configuration.toString(),
             triggerCheckInterval);
    }

    private void createTriggerTable()
    {
        int success = create().table("recent_triggers")
                              .column(new Column("ID", SqlType.LONG).generated(Generated.ALWAYS).asIdentity().autoIncrement(1))
                              .column(new Column("tableName", SqlType.VARCHAR).size(40))
                              .column(new Column("rowIdFieldName", SqlType.VARCHAR).size(40))
                              .column(new Column("triggerType", SqlType.VARCHAR).size(30))
                              .column(new Column("idRow", SqlType.LONG))
                              .column(new Column("insertTime", SqlType.TIMESTAMP)
                                              .defaultValue(SqlValue.CURRENT_TIMESTAMP))
                              .createDefaultTriggers(false)
                              .commit()
                              .execute();

        int success2 = create().table("handled_triggers")
                               .column(new Column("ID", SqlType.LONG).generated(Generated.ALWAYS).asIdentity().autoIncrement(1))
                               .column(new Column("trigger_id", SqlType.LONG))
                               .column(new Column("db_id", SqlType.VARCHAR).size(50))
                               .column(new Column("handleTime", SqlType.TIMESTAMP).defaultValue(SqlValue.CURRENT_TIMESTAMP))
                               .createDefaultTriggers(false)
                               .commit()
                               .execute();

        if (success * success2 == 1)
        {
            Log.debug("Created trigger tables.");
        }
    }

    /**
     * @see bowt.db.DatabaseAccess#createDefaultProcedures()
     */
    @Override
    protected void createDefaultProcedures()
    {
    }

    /**
     * Cancels the trigger check thread and calls {@link DatabaseAccess#kill()}.
     *
     * @see bt.db.DatabaseAccess#kill()
     */
    @Override
    public void kill()
    {
        Null.checkRun(this.triggerCheck, () -> this.triggerCheck.cancel(true));
        super.kill();
    }

    private void startTriggerCheck()
    {
        Null.checkRun(this.triggerCheck, () -> this.triggerCheck.cancel(false));

        this.triggerCheck = Threads.get()
                                   .scheduleAtFixedRateDaemon(
                                           () ->
                                           {
                                               checkTriggers();
                                           },
                                           this.triggerCheckInterval,
                                           this.triggerCheckInterval,
                                           TimeUnit.MILLISECONDS,
                                           "DATABASE_TRIGGER_CHECK");
    }

    private void checkTriggers()
    {
        try
        {
            SqlResultSet set = select()
                    .from("recent_triggers")
                    .where("ID")
                    .notIn(select("trigger_id")
                                   .from("handled_triggers")
                                   .where("db_id")
                                   .equal(getInstanceID())
                                   .unprepared())
                    .onLessThan(1,
                                (num, res) ->
                                {
                                    return res;
                                })
                    .execute();

            long[] ids = new long[set.size()];

            for (int i = 0; i < set.size(); i++)
            {
                SqlResult result = set.get(i);
                ids[i] = result.getLong("ID");
                String table = result.getString("tableName");
                String idFieldName = result.getString("rowIdFieldName");
                long rowId = result.getLong("idRow");
                String triggerType = result.getString("triggerType");

                switch (triggerType.toUpperCase())
                {
                    case "INSERT":
                        DatabaseAccess.onInsert(getInstanceID(),
                                                table,
                                                idFieldName,
                                                rowId);
                        break;
                    case "UPDATE":
                        DatabaseAccess.onUpdate(getInstanceID(),
                                                table,
                                                idFieldName,
                                                rowId);
                        break;
                    case "DELETE":
                        DatabaseAccess.onDelete(getInstanceID(),
                                                table,
                                                idFieldName,
                                                rowId);
                        break;
                }
            }

            for (long id : ids)
            {
                insert().into("handled_triggers")
                        .set("db_id",
                             getInstanceID())
                        .set("trigger_id",
                             id)
                        .execute();
            }
        }
        catch (Exception e)
        {
            Log.error("Failed to check triggers", e);
        }
    }
}