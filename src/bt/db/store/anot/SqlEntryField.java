package bt.db.store.anot;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field for persistence and indicates that the classes own persistence definitions should be used.
 * <p>
 * The identity of the declaring class will be used for automated initialization.
 * </p>
 * 
 * @author &#8904
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface SqlEntryField
{
}