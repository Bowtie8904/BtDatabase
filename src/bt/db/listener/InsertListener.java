package bt.db.listener;

import java.util.function.Consumer;

import bt.db.listener.evnt.InsertEvent;

/**
 * {@link DatabaseListener} extension to receive {@link InsertEvent}s.
 * 
 * @author &#8904
 */
@FunctionalInterface
public interface InsertListener extends Consumer<InsertEvent>
{
}