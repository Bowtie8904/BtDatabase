package bt.db.listener;

import bt.db.listener.evnt.InsertEvent;
import bt.runtime.evnt.Listener;

/**
 * {@link DatabaseListener} extension to receive {@link InsertEvent}s.
 * 
 * @author &#8904
 */
@FunctionalInterface
public interface InsertListener extends Listener<InsertEvent>
{
}