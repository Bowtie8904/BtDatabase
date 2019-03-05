package bt.db.listener;

import bt.db.DatabaseAccess;
import bt.db.listener.evnt.DeleteEvent;

/**
 * {@link DatabaseListener} extension to receive {@link DeleteEvent}s.
 * 
 * @author &#8904
 */
@FunctionalInterface
public interface DeleteListener extends DatabaseListener
{
    /**
     * Defines behavior when the {@link DatabaseAccess} implementations onDelete method was called by a DELETE trigger.
     * 
     * @param e
     *            The fired event containing all relevant data sent by the trigger.
     */
    public void onDelete(DeleteEvent e);
}