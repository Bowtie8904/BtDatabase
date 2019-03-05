package bt.db.listener.anot;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to enable multiple {@link ListenOn} annotations on the same method to listen on multiple tables.
 * 
 * <p>
 * This specidific annotation should never be explicitly.
 * </p>
 * 
 * @author &#8904
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ListenOns
{
    ListenOn[] value();
}