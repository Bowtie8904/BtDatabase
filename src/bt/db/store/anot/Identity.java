package bt.db.store.anot;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to mark an instance field as the identity for the class.
 * 
 * <p>
 * Identities are used to check whether an entry already exists in the database, so that no duplicates are created.
 * </p>
 * <p>
 * Every class can only have one field marked as an iddentity. That field has to be of the type long.
 * </p>
 * <p>
 * A field that has the identity annotation still needs a column anntoation to define the database column and sql type.
 * If multiple tables are used to persist a class, all of them must contain an identity column with the same name.
 * </p>
 * <p>
 * The table of the identity field should always be the 'main' table of the data structure. It has to contain all
 * current entries so that automatic initialization works properly.
 * </p>
 * 
 * @author &#8904
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Identity
{

}
