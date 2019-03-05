package bt.db.listener;

import bt.db.listener.evnt.UpdateEvent;
import bt.runtime.evnt.Listener;

/**
 * {@link DatabaseListener} extension to receive {@link UpdateEvent}s.
 * 
 * @author &#8904
 */
@FunctionalInterface
public interface UpdateListener extends Listener<UpdateEvent>
{
}
