package bt.db;

import java.io.File;
import java.net.URLDecoder;
import java.security.CodeSource;
import java.sql.CallableStatement;
import java.sql.SQLException;

import bt.db.config.DatabaseConfiguration;
import bt.db.constants.SqlType;

/**
 * A class which creates and keeps a connection to an embedded database.
 *
 * @author &#8904
 */
public abstract class EmbeddedDatabase extends DatabaseAccess
{
    /** The set derby home path. */
    protected static String derbyHome;

    /**
     * Creates a new instance which uses the default local db connection string.
     */
    public EmbeddedDatabase()
    {
        this(DEFAULT_LOCAL_DB);
    }

    /**
     * Creates a new instance which uses the given connection string.
     *
     * @param dbURL
     *            The DB connection string.
     */
    public EmbeddedDatabase(String dbURL)
    {
        super(dbURL);
        setDerbyHome();
        addJarToDerby();
        setup();
        setProperty("derby_home",
                    derbyHome);
        createTables();
    }

    /**
     * Creates a new instance which uses the given configuration.
     *
     * @param configuration
     */
    protected EmbeddedDatabase(DatabaseConfiguration configuration)
    {
        this(configuration.toString());
    }

    /**
     * @see bt.db.DatabaseAccess#createDefaultProcedures()
     */
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
            log.print(this,
                      "Created onInsert procedure.");
            created = true;
        }

        success = create().procedure("onDelete")
                          .parameter("instanceID",
                                     SqlType.VARCHAR)
                          .size(40)
                          .parameter("tableName",
                                     SqlType.VARCHAR)
                          .size(40)
                          .parameter("rowIdFieldName",
                                     SqlType.VARCHAR)
                          .size(40)
                          .parameter("oldRowID",
                                     SqlType.LONG)
                          .call(this.getClass().getName() + ".onDelete")
                          .replace()
                          .onFail((s, e) ->
                          {
                              return 0;
                          })
                          .execute();

        if (success == 1)
        {
            log.print(this,
                      "Created onDelete procedure.");
            created = true;
        }

        success = create().procedure("onUpdate")
                          .parameter("instanceID",
                                     SqlType.VARCHAR)
                          .size(40)
                          .parameter("tableName",
                                     SqlType.VARCHAR)
                          .size(40)
                          .parameter("rowIdFieldName",
                                     SqlType.VARCHAR)
                          .size(40)
                          .parameter("newRowID",
                                     SqlType.LONG)
                          .call(this.getClass().getName() + ".onUpdate")
                          .replace()
                          .onFail((s, e) ->
                          {
                              return 0;
                          })
                          .execute();

        if (success == 1)
        {
            log.print(this,
                      "Created onUpdate procedure.");
            created = true;
        }

        if (created)
        {
            commit();
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
                String jarFilePath = path.substring(path.indexOf(":") + 1,
                                                    path.indexOf("!"));
                jarFilePath = URLDecoder.decode(jarFilePath,
                                                "UTF-8");
                jarFile = new File(jarFilePath);
            }
            derbyHome = jarFile.getParentFile().getAbsolutePath();
            System.setProperty("derby.system.home",
                               derbyHome);
            log.printfSrc(this,
                          "Set derby home to %s",
                          derbyHome);
        }
        catch (Exception e)
        {
            log.print(this,
                      e);
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
                String jarFilePath = path.substring(path.indexOf(":") + 1,
                                                    path.indexOf("!"));
                jarFilePath = URLDecoder.decode(jarFilePath,
                                                "UTF-8");
                jarFile = new File(jarFilePath);
            }

            return jarFile.getAbsolutePath();
        }
        catch (Exception e)
        {
            log.print(this,
                      e);
        }
        return null;
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
                log.printfSrc(this,
                              "Added %s to the database.",
                              path);
            }
            catch (SQLException e)
            {
                sql = "CALL sqlj.replace_jar('" + path + "', 'APP.BowtieDatabase')";

                try (CallableStatement statement = getConnection().prepareCall(sql))
                {
                    statement.executeUpdate();
                    log.printfSrc(this,
                                  "Replaced %s in the database.",
                                  path);
                }
                catch (SQLException e1)
                {}
            }

            sql = "CALL syscs_util.syscs_set_database_property('derby.database.classpath', 'APP.BowtieDatabase')";

            try (CallableStatement statement = getConnection().prepareCall(sql))
            {
                statement.executeUpdate();
                log.print(this,
                          "Added classpath to the database.");
            }
            catch (SQLException e1)
            {}
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