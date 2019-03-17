package bt.db;

import java.io.File;
import java.net.URLDecoder;
import java.security.CodeSource;
import java.sql.CallableStatement;
import java.sql.SQLException;

import bt.db.config.DatabaseConfiguration;
import bt.db.constants.SqlType;
import bt.db.listener.evnt.DeleteEvent;
import bt.db.listener.evnt.InsertEvent;
import bt.db.listener.evnt.UpdateEvent;

/**
 * A class which creates and keeps a connection to an embedded database.
 * 
 * @author &#8904
 */
public abstract class LocalDatabase extends DatabaseAccess
{
    /** The set derby home path. */
    protected static String derbyHome;

    public LocalDatabase()
    {
        this(DEFAULT_LOCAL_DB);
    }

    public LocalDatabase(String dbURL)
    {
        super(dbURL);
        setDerbyHome();
        addJarToDerby();
        setup();
        setProperty("derby_home", derbyHome);
        createTables();
    }

    protected LocalDatabase(DatabaseConfiguration configuration)
    {
        this(configuration.toString());
    }

    @Override
    protected void createDefaultProcedures()
    {
        boolean created = false;
        int success = create().procedure("onInsert")
                .parameter("instanceID", SqlType.VARCHAR).size(40)
                .parameter("tableName", SqlType.VARCHAR).size(40)
                .parameter("rowIdFieldName", SqlType.VARCHAR).size(40)
                .parameter("newRowID", SqlType.LONG)
                .call(this.getClass().getName() + ".onInsert")
                .replace()
                .onFail((s, e) ->
                {
                    return 0;
                })
                .execute();

        if (success == 1)
        {
            log.print(this, "Created onInsert procedure.");
            created = true;
        }

        success = create().procedure("onDelete")
                .parameter("instanceID", SqlType.VARCHAR).size(40)
                .parameter("tableName", SqlType.VARCHAR).size(40)
                .parameter("rowIdFieldName", SqlType.VARCHAR).size(40)
                .parameter("oldRowID", SqlType.LONG)
                .call(this.getClass().getName() + ".onDelete")
                .replace()
                .onFail((s, e) ->
                {
                    return 0;
                })
                .execute();

        if (success == 1)
        {
            log.print(this, "Created onDelete procedure.");
            created = true;
        }

        success = create().procedure("onUpdate")
                .parameter("instanceID", SqlType.VARCHAR).size(40)
                .parameter("tableName", SqlType.VARCHAR).size(40)
                .parameter("rowIdFieldName", SqlType.VARCHAR).size(40)
                .parameter("newRowID", SqlType.LONG)
                .call(this.getClass().getName() + ".onUpdate")
                .replace()
                .onFail((s, e) ->
                {
                    return 0;
                })
                .execute();

        if (success == 1)
        {
            log.print(this, "Created onUpdate procedure.");
            created = true;
        }

        if (created)
        {
            commit();
        }
    }

    public synchronized static void onInsert(String instanceID, String table, String idFieldName, long id)
    {
        onInsert(instanceID, table, idFieldName, id, new String[] {});
    }

    public synchronized static void onInsert(String instanceID, String table, String idFieldName, long id, String data1)
    {
        onInsert(instanceID, table, idFieldName, id, new String[]
        {
                data1
        });
    }

    public synchronized static void onInsert(String instanceID, String table, String idFieldName, long id, String data1,
            String data2)
    {
        onInsert(instanceID, table, idFieldName, id, new String[]
        {
                data1, data1
        });
    }

    public synchronized static void onInsert(String instanceID, String table, String idFieldName, long id, String data1,
            String data2,
            String data3)
    {
        onInsert(instanceID, table, idFieldName, id, new String[]
        {
                data1, data2, data3
        });
    }

    public synchronized static void onInsert(String instanceID, String table, String idFieldName, long id, String data1,
            String data2,
            String data3, String data4)
    {
        onInsert(instanceID, table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4
        });
    }

    public synchronized static void onInsert(String instanceID, String table, String idFieldName, long id, String data1,
            String data2,
            String data3, String data4, String data5)
    {
        onInsert(instanceID, table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4, data5
        });
    }

    public synchronized static void onInsert(String instanceID, String table, String idFieldName, long id,
            String... data)
    {
        DatabaseAccess instance = getInstance(instanceID);

        if (instance != null)
        {
            int count = instance.getTriggerDispatcher()
                    .dispatch(new InsertEvent(instance, table, idFieldName, id, data));

            log.print(instance, "Dispatched insert event to " + count + " listeners.");
        }
    }

    public synchronized static void onUpdate(String instanceID, String table, String idFieldName, long id)
    {
        onUpdate(instanceID, table, idFieldName, id, new String[] {});
    }

    public synchronized static void onUpdate(String instanceID, String table, String idFieldName, long id, String data1)
    {
        onUpdate(instanceID, table, idFieldName, id, new String[]
        {
                data1
        });
    }

    public synchronized static void onUpdate(String instanceID, String table, String idFieldName, long id, String data1,
            String data2)
    {
        onUpdate(instanceID, table, idFieldName, id, new String[]
        {
                data1, data1
        });
    }

    public synchronized static void onUpdate(String instanceID, String table, String idFieldName, long id, String data1,
            String data2,
            String data3)
    {
        onUpdate(instanceID, table, idFieldName, id, new String[]
        {
                data1, data2, data3
        });
    }

    public synchronized static void onUpdate(String instanceID, String table, String idFieldName, long id, String data1,
            String data2,
            String data3, String data4)
    {
        onUpdate(instanceID, table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4
        });
    }

    public synchronized static void onUpdate(String instanceID, String table, String idFieldName, long id, String data1,
            String data2,
            String data3, String data4, String data5)
    {
        onUpdate(instanceID, table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4, data5
        });
    }

    public synchronized static void onUpdate(String instanceID, String table, String idFieldName, long id,
            String... data)
    {
        DatabaseAccess instance = getInstance(instanceID);

        if (instance != null)
        {
            int count = instance.getTriggerDispatcher()
                    .dispatch(new UpdateEvent(instance, table, idFieldName, id, data));

            log.print(instance, "Dispatched update event to " + count + " listeners.");
        }
    }

    public synchronized static void onDelete(String instanceID, String table, String idFieldName, long id)
    {
        onDelete(instanceID, table, idFieldName, id, new String[] {});
    }

    public synchronized static void onDelete(String instanceID, String table, String idFieldName, long id, String data1)
    {
        onDelete(instanceID, table, idFieldName, id, new String[]
        {
                data1
        });
    }

    public synchronized static void onDelete(String instanceID, String table, String idFieldName, long id, String data1,
            String data2)
    {
        onDelete(instanceID, table, idFieldName, id, new String[]
        {
                data1, data1
        });
    }

    public synchronized static void onDelete(String instanceID, String table, String idFieldName, long id, String data1,
            String data2,
            String data3)
    {
        onDelete(instanceID, table, idFieldName, id, new String[]
        {
                data1, data2, data3
        });
    }

    public synchronized static void onDelete(String instanceID, String table, String idFieldName, long id, String data1,
            String data2,
            String data3, String data4)
    {
        onDelete(instanceID, table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4
        });
    }

    public synchronized static void onDelete(String instanceID, String table, String idFieldName, long id, String data1,
            String data2,
            String data3, String data4, String data5)
    {
        onDelete(instanceID, table, idFieldName, id, new String[]
        {
                data1, data2, data3, data4, data5
        });
    }

    public synchronized static void onDelete(String instanceID, String table, String idFieldName, long id,
            String... data)
    {
        DatabaseAccess instance = getInstance(instanceID);

        if (instance != null)
        {
            int count = instance.getTriggerDispatcher()
                    .dispatch(new DeleteEvent(instance, table, idFieldName, id, data));

            log.print(instance, "Dispatched delete event to " + count + " listeners.");
        }
    }

    /**
     * Sets the home of Derby to the folder of this jar.
     */
    private void setDerbyHome()
    {
        try
        {
            CodeSource codeSource = getClass().getProtectionDomain().getCodeSource();
            File jarFile;
            if (codeSource.getLocation() != null)
            {
                jarFile = new File(codeSource.getLocation().toURI());
            }
            else
            {
                String path = getClass().getResource(getClass().getSimpleName() + ".class")
                        .getPath();
                String jarFilePath = path.substring(path.indexOf(":") + 1, path.indexOf("!"));
                jarFilePath = URLDecoder.decode(jarFilePath, "UTF-8");
                jarFile = new File(jarFilePath);
            }
            derbyHome = jarFile.getParentFile().getAbsolutePath();
            System.setProperty("derby.system.home", derbyHome);
            log.print(this, "Set derby home to " + derbyHome);
        }
        catch (Exception e)
        {
            log.print(this, e);
        }
    }

    private String getJarPath()
    {
        try
        {
            CodeSource codeSource = getClass().getProtectionDomain().getCodeSource();
            File jarFile;
            if (codeSource.getLocation() != null)
            {
                jarFile = new File(codeSource.getLocation().toURI());
            }
            else
            {
                String path = getClass().getResource(getClass().getSimpleName() + ".class")
                        .getPath();
                String jarFilePath = path.substring(path.indexOf(":") + 1, path.indexOf("!"));
                jarFilePath = URLDecoder.decode(jarFilePath, "UTF-8");
                jarFile = new File(jarFilePath);
            }

            return jarFile.getAbsolutePath();
        }
        catch (Exception e)
        {
            log.print(this, e);
        }
        return "X:\\Workspace\\Utilities\\target\\Utilities-1.jar";
    }

    private void addJarToDerby()
    {
        try
        {
            String path = getJarPath();
            String sql = "CALL sqlj.install_jar('" + path + "', 'APP.BowtieDatabase', 0)";

            try (CallableStatement statement = getConnection().prepareCall(sql))
            {
                statement.executeUpdate();
                log.print(this, "Added " + path + " to the database.");
            }
            catch (SQLException e)
            {
                sql = "CALL sqlj.replace_jar('" + path + "', 'APP.BowtieDatabase')";

                try (CallableStatement statement = getConnection().prepareCall(sql))
                {
                    statement.executeUpdate();
                    log.print(this, "Replaced " + path + " in the database.");
                }
                catch (SQLException e1)
                {
                }
            }

            sql = "CALL syscs_util.syscs_set_database_property('derby.database.classpath', 'APP.BowtieDatabase')";

            try (CallableStatement statement = getConnection().prepareCall(sql))
            {
                statement.executeUpdate();
                log.print(this, "Added classpath to the database.");
            }
            catch (SQLException e1)
            {
            }
        }
        catch (Exception ex)
        {

        }
    }

    /**
     * Gets the derby home.
     * 
     * @return The derby home path. This returns null until {@link #setDerbyHome()} was called.
     */
    public static String getDerbyHome()
    {
        return derbyHome;
    }
}