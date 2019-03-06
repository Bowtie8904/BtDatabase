package bt.db.listener.anot;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines the database table to listen on.
 * 
 * <p>
 * This annotation needs to be added to the receive methods of the {@link Listener} implementation.
 * </p>
 * 
 * <p>
 * The used value is only relevant for the method this annotation was added to.
 * </p>
 * 
 * @author &#8904
 */
@Repeatable(ListenOns.class)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ListenOn
{
    /** The name of the database table to listen on. */
    String value();
}