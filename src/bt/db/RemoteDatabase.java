package bt.db;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import bt.db.config.DatabaseConfiguration;
import bt.db.constants.Generated;
import bt.db.constants.SqlType;
import bt.db.constants.SqlValue;
import bt.db.statement.result.SqlResult;
import bt.db.statement.result.SqlResultSet;
import bt.utils.thread.Threads;

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
public abstract class RemoteDatabase extends DatabaseAccess {
    protected ScheduledFuture triggerCheck;
    protected long triggerCheckInterval;

    /**
     * Creates a new instance which uses the default local db connection string and a trigger check interval of 3
     * seconds.
     */
    protected RemoteDatabase()
    {
        this(DEFAULT_LOCAL_DB,
             3000);
    }

    /**
     * Creates a new instance which uses the given connection string and a trigger check interval of 3 seconds.
     *
     * @param dbURL
     *            The DB connection string.
     */
    protected RemoteDatabase(String dbURL)
    {
        this(dbURL,
             3000);
    }

    /**
     * Creates a new instance which uses the given connection string and a given trigger check interval.
     *
     * @param dbURL
     *            The DB connection string.
     * @param triggerCheckInterval
     *            The trigger check interval in milliseconds.
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
                              .column("ID",
                                      SqlType.LONG)
                              .asIdentity(Generated.ALWAYS)
                              .autoIncrement(1)
                              .add()
                              .column("tableName",
                                      SqlType.VARCHAR)
                              .size(40)
                              .add()
                              .column("rowIdFieldName",
                                      SqlType.VARCHAR)
                              .size(40)
                              .add()
                              .column("triggerType",
                                      SqlType.VARCHAR)
                              .size(30)
                              .add()
                              .column("idRow",
                                      SqlType.LONG)
                              .add()
                              .column("insertTime",
                                      SqlType.TIMESTAMP)
                              .defaultValue(SqlValue.CURRENT_TIMESTAMP)
                              .add()
                              .createDefaultTriggers(false)
                              .commit()
                              .execute();

        int success2 = create().table("handled_triggers")
                               .column("ID",
                                       SqlType.LONG)
                               .asIdentity(Generated.ALWAYS)
                               .autoIncrement(1)
                               .add()
                               .column("trigger_id",
                                       SqlType.LONG)
                               .add()
                               .column("db_id",
                                       SqlType.VARCHAR)
                               .size(50)
                               .add()
                               .column("handleTime",
                                       SqlType.TIMESTAMP)
                               .defaultValue(SqlValue.CURRENT_TIMESTAMP)
                               .add()
                               .createDefaultTriggers(false)
                               .commit()
                               .execute();

        if (success * success2 == 1)
        {
            log.print(this,
                      "Created trigger tables.");
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
        if (this.triggerCheck != null)
        {
            this.triggerCheck.cancel(true);
        }

        super.kill();
    }

    private void startTriggerCheck()
    {
        if (this.triggerCheck != null)
        {
            this.triggerCheck.cancel(false);
        }

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
                                                                  .equals(getInstanceID())
                                                                  .unprepared())
                                       .onLessThan(1,
                                                   (num, res) ->
                                                   {
                                                       return res;
                                                   })
                                       .execute();

            long[] ids = new long[set.size()];

            for (int i = 0; i < set.size(); i ++ )
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
                        onInsert(getInstanceID(),
                                 table,
                                 idFieldName,
                                 rowId);
                        break;
                    case "UPDATE":
                        onUpdate(getInstanceID(),
                                 table,
                                 idFieldName,
                                 rowId);
                        break;
                    case "DELETE":
                        onDelete(getInstanceID(),
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
            log.print(e);
        }
    }
}