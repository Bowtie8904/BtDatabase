package bt.db.server;

import bt.db.DatabaseAccess;
import bt.db.exc.SqlClientException;
import bt.db.listener.evnt.DatabaseChangeEvent;
import bt.remote.socket.ServerClient;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class QueryClient extends ServerClient
{
    private List<TriggerListener> triggerListeners;
    private DatabaseAccess db;

    public QueryClient(Socket socket, DatabaseAccess db) throws IOException
    {
        super(socket);
        this.db = db;
        this.triggerListeners = new ArrayList<>();
    }

    public synchronized <T extends DatabaseChangeEvent> void listenToTrigger(Class<T> triggerType, String tableName) throws SqlClientException
    {
        var listener = new TriggerListener<>(triggerType, tableName, this);

        if (!this.triggerListeners.contains(listener))
        {
            listener.listen(this.db);
            this.triggerListeners.add(listener);
        }
        else
        {
            throw new SqlClientException("Client is already listening to triggers of type " + triggerType.getSimpleName() + ".");
        }
    }

    @Override
    public void kill()
    {
        for (var listener : this.triggerListeners)
        {
            listener.stopListening(this.db);
        }

        super.kill();
    }
}