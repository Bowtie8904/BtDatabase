package bt.db.listener.impl;

import java.util.HashMap;
import java.util.Map;

import bt.db.listener.evnt.DatabaseChangeEvent;
import bt.db.listener.evnt.DeleteEvent;

/**
 * Listens to updates and inserts. Saves the last used identity field value for any received table.
 *
 * @author &#8904
 */
public class IdentityListener
{
    private static Map<String, Long> identities = new HashMap<>();

    public static void receive(DatabaseChangeEvent e)
    {
        if (!(e instanceof DeleteEvent))
        {
            identities.put(e.getTable().toUpperCase(), e.getID());
        }
    }

    /**
     * Gets the last saved identity field value for the given table.
     *
     * @param table
     * @return The identity value or null if no value was saved for that table.
     */
    public static Long getLast(String table)
    {
        return identities.get(table.toUpperCase());
    }
}