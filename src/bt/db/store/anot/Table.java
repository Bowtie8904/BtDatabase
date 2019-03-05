package bt.db.store.anot;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Defines the table that should be used to persist the annotated class.
 * 
 * @author &#8904
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Table
{
    /**
     * The name of the table.
     * 
     * @return
     */
    String value();
}