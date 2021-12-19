package bt.db.filter;

import bt.db.filter.parse.SqlPredicateColumn;
import bt.log.Log;
import bt.reflect.field.Fields;

import java.lang.reflect.Field;

/**
 * The default implementation of {@link SqlPredicate}. This class attempts to find
 * a field with the same name as the column from the condition of the object that is being checked.
 */
public class ClassFieldSqlPredicate extends SqlPredicate<Object>
{
    /**
     * Instantiates a new Class field sql predicate.
     *
     * @param field     the field
     * @param operation the operation
     * @param value     the value
     */
    public ClassFieldSqlPredicate(Object field, String operation, Object value)
    {
        super(field, operation, value);
    }

    /**
     * Instantiates a new Class field sql predicate.
     *
     * @param field     the field
     * @param operation the operation
     * @param value     the value
     * @param value2    the value 2
     */
    public ClassFieldSqlPredicate(Object field, String operation, Object value, Object value2)
    {
        super(field, operation, value, value2);
    }

    /**
     * Instantiates a new Class field sql predicate.
     *
     * @param defaultValue the default value
     */
    public ClassFieldSqlPredicate(boolean defaultValue)
    {
        super(defaultValue);
    }

    protected Field findField(Object o)
    {
        return Fields.getField(o.getClass(), f -> f.getName().equalsIgnoreCase(this.field.toString()));
    }

    /**
     * Attempts to find the first field of the given class (or its super classes) that has the same
     * case insensitive name as this instances column name.
     *
     * <p>
     * Since this method searches case insensitive it might find a field that was not intended for checking.
     * If you want to enforce case sensitivity you can override {@link ClassFieldSqlPredicate#findField(Object)}.
     * </p>
     *
     * <p>
     * If no field with a fitting name could be found this method uses the plain column name and continues. This is done
     * to ensure that 'select * from dummy where 1 = 1' can also be handled correctly.
     * </p>
     *
     * @see SqlPredicate#check(Object)
     */
    @Override
    protected boolean check(Object o)
    {
        Object val = this.field;

        if (this.field instanceof SqlPredicateColumn)
        {
            Field field = findField(o);

            if (field != null)
            {
                try
                {
                    field.setAccessible(true);
                    val = field.get(o);
                }
                catch (IllegalAccessException e)
                {
                    Log.error("Failed to get field value", e);
                }
            }
        }

        if (val instanceof String)
        {
            return checkStringValue(val.toString());
        }
        else
        {
            return checkObjectValue(val);
        }
    }
}