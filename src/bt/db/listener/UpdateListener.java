package bt.db.listener;

import java.util.function.Consumer;

import bt.db.listener.evnt.UpdateEvent;

/**
 * {@link DatabaseListener} extension to receive {@link UpdateEvent}s.
 * 
 * @author &#8904
 */
@FunctionalInterface
public interface UpdateListener extends Consumer<UpdateEvent>
{
}
