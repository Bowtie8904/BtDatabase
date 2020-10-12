package bt.db.server;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import bt.async.Data;
import bt.db.DatabaseAccess;
import bt.db.statement.result.SqlResultSet;
import bt.remote.socket.Server;
import bt.remote.socket.ServerClient;
import bt.remote.socket.data.DataProcessor;

/**
 * @author &#8904
 *
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
        client.setRequestProcessor(this);

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
                ret = "Comitted transaction.";
            }
            else if (incoming.get().toString().trim().equalsIgnoreCase("rollback"))
            {
                this.db.rollback();
                ret = "Rolled transaction back.";
            }
            else if (incoming.get().toString().trim().equalsIgnoreCase("backup"))
            {
                File backup = new File("./backup/" + System.currentTimeMillis());
                this.db.backup(backup);
                ret = "Created backup under " + backup.getAbsolutePath();
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
                ret = "Updated " + updateCount + " rows.";
            }
            else
            {
                ret = "Executed successfully.";
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