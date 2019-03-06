package bt.db;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import bt.db.constants.Generated;
import bt.db.constants.SqlType;
import bt.db.constants.SqlValue;
import bt.db.listener.DatabaseListener;
import bt.db.listener.DeleteListener;
import bt.db.listener.InsertListener;
import bt.db.listener.UpdateListener;
import bt.db.listener.anot.ListenOn;
import bt.db.listener.evnt.DeleteEvent;
import bt.db.listener.evnt.InsertEvent;
import bt.db.listener.evnt.UpdateEvent;
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
public abstract class RemoteDatabase extends DatabaseAccess
{
    protected ScheduledFuture triggerCheck;
    protected long triggerCheckInterval;

    protected RemoteDatabase()
    {
        this(DEFAULT_LOCAL_DB, 3000);
    }

    /**
     * @param dbURL
     */
    protected RemoteDatabase(String dbURL)
    {
        this(dbURL, 3000);
    }

    protected RemoteDatabase(String dbURL, long triggerCheckInterval)
    {
        super(dbURL);
        this.triggerCheckInterval = triggerCheckInterval;
        setup();
        createTriggerTable();
        createTables();
        startTriggerCheck();
    }

    private void createTriggerTable()
    {
        int success = create().table("recent_triggers")
                .column("ID", SqlType.LONG).asIdentity(Generated.ALWAYS).autoIncrement(1).add()
                .column("tableName", SqlType.VARCHAR).size(40).add()
                .column("rowIdFieldName", SqlType.VARCHAR).size(40).add()
                .column("triggerType", SqlType.VARCHAR).size(30).add()
                .column("idRow", SqlType.LONG).add()
                .column("insertTime", SqlType.TIMESTAMP).defaultValue(SqlValue.CURRENT_TIMESTAMP).add()
                .createDefaultTriggers(false)
                .commit()
                .execute();

        int success2 = create().table("handled_triggers")
                .column("ID", SqlType.LONG).asIdentity(Generated.ALWAYS).autoIncrement(1).add()
                .column("trigger_id", SqlType.LONG).add()
                .column("db_id", SqlType.VARCHAR).size(50).add()
                .column("handleTime", SqlType.TIMESTAMP).defaultValue(SqlValue.CURRENT_TIMESTAMP).add()
                .createDefaultTriggers(false)
                .commit()
                .execute();

        if (success * success2 == 1)
        {
            log.print(this, "Created trigger tables.");
        }
    }

    /**
     * @see bowt.db.DatabaseAccess#createDefaultProcedures()
     */
    @Override
    protected void createDefaultProcedures()
    {
    }

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

        this.triggerCheck = Threads.get().scheduleAtFixedRateDaemon(() ->
        {
            try
            {
                SqlResultSet set = select()
                        .from("recent_triggers")
                        .where("ID").notIn(select("trigger_id")
                                .from("handled_triggers")
                                .where("db_id").equals(getID())
                                .unprepared())
                        .onLessThan(1, (num, res) ->
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
                        onInsert(table, idFieldName, rowId);
                        break;
                    case "UPDATE":
                        onUpdate(table, idFieldName, rowId);
                        break;
                    case "DELETE":
                        onDelete(table, idFieldName, rowId);
                        break;
                    }
                }

                for (long id : ids)
                {
                    insert().into("handled_triggers")
                            .set("db_id", getID())
                            .set("trigger_id", id)
                            .execute();
                }
            }
            catch (Exception e)
            {
                log.print(e);
            }

        }, this.triggerCheckInterval, this.triggerCheckInterval, TimeUnit.MILLISECONDS, "DATABASE_TRIGGER_CHECK");
    }

    public synchronized void onInsert(String table, String idFieldName, long id)
    {
        onInsert(table, idFieldName, id, new String[] {});
    }

    public synchronized void onInsert(String table, String idFieldName, long id, String data1)
    {
        onInsert(table, idFieldName, id, new String[]
        {
                data1
        });
    }

    public synchronized void onInsert(String table, String idFieldName, long id, String data1, String data2)
    {
        onInsert(table, idFieldName, id, new String[]
        {
                data1, data1
        });
    }

    public synchronized void onInsert(String table, String idFieldName, long id, String data1, String data2,
            String data3)
    {
        onInsert(table, idFieldName, id, new String[]
        {
                data1, data2, data3
        });
    }

    public synchronized void onInsert(String table, String idFieldName, long id, String data1, String data2,
            String data3, String data4)
    {
        onInsert(table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4
        });
    }

    public synchronized void onInsert(String table, String idFieldName, long id, String data1, String data2,
            String data3, String data4, String data5)
    {
        onInsert(table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4, data5
        });
    }

    public synchronized void onInsert(String table, String idFieldName, long id, String... data)
    {
        List<DatabaseListener> insertListeners = (List<DatabaseListener>)listeners
                .stream()
                .filter(l -> l instanceof InsertListener)
                .collect(Collectors.toList());

        for (DatabaseListener listener : insertListeners)
        {
            InsertListener insertListener = (InsertListener)listener;
            Method method = null;
            try
            {
                method = insertListener.getClass().getMethod("onInsert", InsertEvent.class);
            }
            catch (NoSuchMethodException | SecurityException e)
            {
                log.print(e);
                return;
            }

            ListenOn[] annotations = method.getAnnotationsByType(ListenOn.class);

            if (annotations.length == 0)
            {
                insertListener.onInsert(new InsertEvent(this, table, idFieldName, id, data));
            }
            else
            {
                for (ListenOn an : annotations)
                {
                    if (an != null && an.value().toUpperCase().equals(table.toUpperCase()))
                    {
                        insertListener.onInsert(new InsertEvent(this, table, idFieldName, id, data));
                        break;
                    }
                }
            }
        }
    }

    public synchronized void onUpdate(String table, String idFieldName, long id)
    {
        onUpdate(table, idFieldName, id, new String[] {});
    }

    public synchronized void onUpdate(String table, String idFieldName, long id, String data1)
    {
        onUpdate(table, idFieldName, id, new String[]
        {
                data1
        });
    }

    public synchronized void onUpdate(String table, String idFieldName, long id, String data1, String data2)
    {
        onUpdate(table, idFieldName, id, new String[]
        {
                data1, data1
        });
    }

    public synchronized void onUpdate(String table, String idFieldName, long id, String data1, String data2,
            String data3)
    {
        onUpdate(table, idFieldName, id, new String[]
        {
                data1, data2, data3
        });
    }

    public synchronized void onUpdate(String table, String idFieldName, long id, String data1, String data2,
            String data3, String data4)
    {
        onUpdate(table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4
        });
    }

    public synchronized void onUpdate(String table, String idFieldName, long id, String data1, String data2,
            String data3, String data4, String data5)
    {
        onUpdate(table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4, data5
        });
    }

    public synchronized void onUpdate(String table, String idFieldName, long id, String... data)
    {
        List<DatabaseListener> updateListeners = (List<DatabaseListener>)listeners
                .stream()
                .filter(l -> l instanceof UpdateListener)
                .collect(Collectors.toList());

        for (DatabaseListener listener : updateListeners)
        {
            UpdateListener updateListener = (UpdateListener)listener;
            Method method = null;
            try
            {
                method = updateListener.getClass().getMethod("onUpdate", UpdateEvent.class);
            }
            catch (NoSuchMethodException | SecurityException e)
            {
                log.print(e);
                return;
            }

            ListenOn[] annotations = method.getAnnotationsByType(ListenOn.class);

            if (annotations.length == 0)
            {
                updateListener.onUpdate(new UpdateEvent(this, table, idFieldName, id, data));
            }
            else
            {
                for (ListenOn an : annotations)
                {
                    if (an != null && an.value().toUpperCase().equals(table.toUpperCase()))
                    {
                        updateListener.onUpdate(new UpdateEvent(this, table, idFieldName, id, data));
                        break;
                    }
                }
            }
        }
    }

    public synchronized void onDelete(String table, String idFieldName, long id)
    {
        onDelete(table, idFieldName, id, new String[] {});
    }

    public synchronized void onDelete(String table, String idFieldName, long id, String data1)
    {
        onDelete(table, idFieldName, id, new String[]
        {
                data1
        });
    }

    public synchronized void onDelete(String table, String idFieldName, long id, String data1, String data2)
    {
        onDelete(table, idFieldName, id, new String[]
        {
                data1, data1
        });
    }

    public synchronized void onDelete(String table, String idFieldName, long id, String data1, String data2,
            String data3)
    {
        onDelete(table, idFieldName, id, new String[]
        {
                data1, data2, data3
        });
    }

    public synchronized void onDelete(String table, String idFieldName, long id, String data1, String data2,
            String data3, String data4)
    {
        onDelete(table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4
        });
    }

    public synchronized void onDelete(String table, String idFieldName, long id, String data1, String data2,
            String data3, String data4, String data5)
    {
        onDelete(table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4, data5
        });
    }

    public synchronized void onDelete(String table, String idFieldName, long id, String... data)
    {
        List<DatabaseListener> deleteListeners = (List<DatabaseListener>)listeners
                .stream()
                .filter(l -> l instanceof DeleteListener)
                .collect(Collectors.toList());

        for (DatabaseListener listener : deleteListeners)
        {
            DeleteListener deleteListener = (DeleteListener)listener;
            Method method = null;
            try
            {
                method = deleteListener.getClass().getMethod("onDelete", DeleteEvent.class);
            }
            catch (NoSuchMethodException | SecurityException e)
            {
                log.print(e);
                return;
            }

            ListenOn[] annotations = method.getAnnotationsByType(ListenOn.class);

            if (annotations.length == 0)
            {
                deleteListener.onDelete(new DeleteEvent(this, table, idFieldName, id, data));
            }
            else
            {
                for (ListenOn an : annotations)
                {
                    if (an != null && an.value().toUpperCase().equals(table.toUpperCase()))
                    {
                        deleteListener.onDelete(new DeleteEvent(this, table, idFieldName, id, data));
                        break;
                    }
                }
            }
        }
    }
}