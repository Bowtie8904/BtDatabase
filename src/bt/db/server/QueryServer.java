package bt.db.server;

import bt.async.Data;
import bt.console.output.styled.Style;
import bt.db.DatabaseAccess;
import bt.db.func.Sql;
import bt.db.statement.result.SqlResultSet;
import bt.remote.socket.Server;
import bt.remote.socket.ServerClient;
import bt.remote.socket.data.DataProcessor;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author &#8904
 */
public class QueryServer extends Server implements DataProcessor
{
    private DatabaseAccess db;

    /**
     * @param port
     * @throws IOException
     */
    public QueryServer(DatabaseAccess db, int port) throws IOException
    {
        super(port);
        this.db = db;
    }

    @Override
    protected ServerClient createClient(Socket socket) throws IOException
    {
        ServerClient client = super.createClient(socket);
        client.setDataProcessor(this);

        System.out.println("New QueryServer connection established to " + client.getHost() + ":" + client.getPort());

        return client;
    }

    /**
     * @see bt.remote.socket.data.DataProcessor#process(bt.async.Data)
     */
    @Override
    public Object process(Data incoming)
    {
        Object ret = null;

        System.out.println("Received '" + incoming.get() + "'.");

        if (incoming.get() instanceof String)
        {
            if (incoming.get().toString().trim().equalsIgnoreCase("commit"))
            {
                this.db.commit();
                ret = Style.apply("Comitted transaction.", "lime");
            }
            else if (incoming.get().toString().trim().equalsIgnoreCase("rollback"))
            {
                this.db.rollback();
                ret = Style.apply("Rolled transaction back.", "lime");
            }
            else if (incoming.get().toString().trim().equalsIgnoreCase("backup"))
            {
                File backup = new File("./backup/" + System.currentTimeMillis());
                this.db.backup(backup);
                ret = Style.apply("Created backup under " + backup.getAbsolutePath(), "lime");
            }
            else if (incoming.get().toString().trim().toLowerCase().startsWith("info"))
            {
                String[] parts = incoming.get().toString().split(" ");

                if (parts.length < 2)
                {
                    ret = Style.apply("Format: info <tablename>", "red", "bold");
                }
                else
                {
                    String table = parts[1];

                    try
                    {
                        ret = this.db.select(Sql.column("column_name"),
                                             Sql.column("data_type").as("type"),
                                             Sql.column("comment"),
                                             Sql.column("primary_key"),
                                             Sql.column("is_identity"),
                                             Sql.column("generation"),
                                             Sql.column("not_null"),
                                             Sql.column("is_unique"),
                                             Sql.column("default_value"),
                                             Sql.column("foreign_keys"),
                                             Sql.column("checks"),
                                             Sql.column("created"),
                                             Sql.column("updated"))
                                     .from(DatabaseAccess.COLUMN_DATA)
                                     .where("table_name").equal(table.toUpperCase())
                                     .execute();
                    }
                    catch (Exception e)
                    {
                        ret = e;
                    }
                }
            }
            else if (incoming.get().toString().trim().equalsIgnoreCase("select * from tables"))
            {
                ret = executeSql("select * from sys.systables");
            }
            else
            {
                ret = executeSql(incoming.get().toString());
            }
        }

        return ret;
    }

    private Object executeSql(String sql)
    {
        Object ret = null;

        try (Statement st = this.db.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY))
        {
            boolean hasResultSet = st.execute(sql);
            int updateCount = st.getUpdateCount();

            if (hasResultSet)
            {
                ret = new SqlResultSet(st.getResultSet());
            }
            else if (updateCount >= 0)
            {
                ret = Style.apply("Updated " + updateCount + " rows.", "lime");
            }
            else
            {
                ret = Style.apply("Executed successfully.", "lime");
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            ret = e;
        }

        return ret;
    }
}