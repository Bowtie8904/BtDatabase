package bt.db.store.anot;

import bt.db.constants.SqlType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for automatic persisting of objects.
 * <p>
 * This annotation is used to bind an instance field to a column name and sql type, so that it can be correctly inserted
 * into a database.
 *
 * @author &#8904
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Column
{
    /**
     * The name of the column.
     *
     * @return The name.
     */
    String name();

    /**
     * The sql type of the column.
     *
     * @return The type.
     */
    SqlType type() default SqlType.UNKNOWN;
}
