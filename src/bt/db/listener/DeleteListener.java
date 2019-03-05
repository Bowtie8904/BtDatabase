package bt.db.listener;

import bt.db.listener.evnt.DeleteEvent;
import bt.runtime.evnt.Listener;

/**
 * {@link DatabaseListener} extension to receive {@link DeleteEvent}s.
 * 
 * @author &#8904
 */
@FunctionalInterface
public interface DeleteListener extends Listener<DeleteEvent>
{
}