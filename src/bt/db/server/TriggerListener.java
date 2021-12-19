package bt.db.server;

import bt.db.DatabaseAccess;
import bt.db.listener.evnt.DatabaseChangeEvent;
import bt.log.Log;
import bt.remote.socket.ServerClient;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

public class TriggerListener<T extends DatabaseChangeEvent>
{
    private String tableName;
    private Consumer<T> triggerConsumer;
    private Class<T> triggerType;
    private ServerClient client;

    public TriggerListener(Class<T> triggerType, String tableName, ServerClient client)
    {
        this.triggerType = triggerType;
        this.tableName = tableName;
        this.client = client;
    }

    protected void onReceive(T event)
    {
        try
        {
            event.setSourceDatabase(null);
            this.client.send(event);
        }
        catch (IOException e)
        {
            Log.error("Failed to send event", e);
        }
    }

    public void listen(DatabaseAccess db)
    {
        this.triggerConsumer = db.registerListener(this.triggerType, this::onReceive, this.tableName);
    }

    public void stopListening(DatabaseAccess db)
    {
        db.unregisterListener(this.triggerType, this.triggerConsumer);
    }

    public String getTableName()
    {
        return this.tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public Consumer<T> getTriggerConsumer()
    {
        return this.triggerConsumer;
    }

    public void setTriggerConsumer(Consumer<T> triggerConsumer)
    {
        this.triggerConsumer = triggerConsumer;
    }

    public Class<T> getTriggerType()
    {
        return this.triggerType;
    }

    public void setTriggerType(Class<T> triggerType)
    {
        this.triggerType = triggerType;
    }

    public ServerClient getClient()
    {
        return this.client;
    }

    public void setClient(ServerClient client)
    {
        this.client = client;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        TriggerListener<?> that = (TriggerListener<?>)o;
        return Objects.equals(this.tableName, that.tableName) && Objects.equals(this.triggerType, that.triggerType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.tableName, this.triggerType);
    }
}