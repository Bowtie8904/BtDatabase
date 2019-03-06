package bt.db.listener;

import bt.db.DatabaseAccess;
import bt.db.listener.evnt.UpdateEvent;

/**
 * {@link DatabaseListener} extension to receive {@link UpdateEvent}s.
 * 
 * @author &#8904
 */
@FunctionalInterface
public interface UpdateListener extends DatabaseListener
{
    /**
     * Defines behavior when the {@link DatabaseAccess} implementations onUpdate method was called by an UPDATE trigger.
     * 
     * @param e
     *            The fired event containing all relevant data sent by the trigger.
     */
    public void onUpdate(UpdateEvent e);
}
