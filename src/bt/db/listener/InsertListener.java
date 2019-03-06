package bt.db.listener;

import bt.db.DatabaseAccess;
import bt.db.listener.evnt.InsertEvent;

/**
 * {@link DatabaseListener} extension to receive {@link InsertEvent}s.
 * 
 * @author &#8904
 */
@FunctionalInterface
public interface InsertListener extends DatabaseListener
{
    /**
     * Defines behavior when the {@link DatabaseAccess} implementations onInsert method was called by an INSERT trigger.
     * 
     * @param e
     *            The fired event containing all relevant data sent by the trigger.
     */
    public void onInsert(InsertEvent e);
}