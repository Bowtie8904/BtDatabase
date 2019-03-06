package bt.db.listener;

import java.util.function.Consumer;

import bt.db.listener.evnt.DeleteEvent;

/**
 * {@link DatabaseListener} extension to receive {@link DeleteEvent}s.
 * 
 * @author &#8904
 */
@FunctionalInterface
public interface DeleteListener extends Consumer<DeleteEvent>
{
}