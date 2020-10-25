package bt.db.filter;

import java.util.Date;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author &#8904
 */
public abstract class SqlPredicate<T> implements Predicate<T>
{
    public static final String EQUALS = "=";
    public static final String NOT_EQUAL = "!=";
    public static final String IS_NOT_NULL = "IS NOT NULL";
    public static final String IS_NULL = "IS NULL";
    public static final String LIKE = "LIKE";
    public static final String NOT_LIKE = "NOT LIKE";
    public static final String IN = "IN";
    public static final String NOT_IN = "NOT IN";
    public static final String GREATER_THAN = "GREATER THAN";
    public static final String LESS_THAN = "LESS THAN";
    public static final String GREATER_EQUAL_THAN = "GREATER EQUAL THAN";
    public static final String LESS_EQUAL_THAN = "LESS EQUAL THAN";
    public static final String BETWEEN = "BETWEEN";
    public static final String NOT_BETWEEN = "NOT BETWEEN";

    protected Pattern likePattern;
    protected Object field;
    protected String operation;
    protected Object value;
    protected Object value2;
    protected Boolean defaultValue;

    public SqlPredicate(Object field, String operation, Object value)
    {
        this(field, operation, value, null);
    }

    public SqlPredicate(Object field, String operation, Object value, Object value2)
    {
        this.field = field;
        this.operation = operation;
        this.value = value;
        this.value2 = value2;

        if (this.operation.equals(SqlPredicate.LIKE) || this.operation.equals(SqlPredicate.NOT_LIKE))
        {
            compileLikePattern();
        }
    }

    public SqlPredicate(boolean defaultValue)
    {
        this.defaultValue = defaultValue;
    }

    private void compileLikePattern()
    {
        String regex = "^" + this.value.toString().toLowerCase().replace(".", "\\.").replace("%", ".*").replace("_", ".") + "$";
        this.likePattern = Pattern.compile(regex);
    }

    /**
     * Attempts to convert the given object to a numeric double value.
     *
     * <p>
     * Date types will be converted to a milliseond since the epoch value.
     * Other objects toString methods will be called and attempted to be parsed as double.
     * </p>
     *
     * @param value
     * @return A numeric representation of the given object or null if no number could be created.
     */
    protected Double getNumericValue(Object value)
    {
        Double d = null;

        if (value != null && value instanceof Date)
        {
            d = Double.valueOf(((Date)value).getTime());
        }
        else if (value != null)
        {
            try
            {
                d = Double.parseDouble(value.toString());
            }
            catch (NumberFormatException e)
            {
                d = null;
            }
        }

        return d;
    }

    /**
     * Attempts to convert the given object to a numerical Double value by using {@link SqlPredicate#getNumericValue}.
     *
     * <p>
     * If the given value can not be converted it will be returned unchanged.
     * </p>
     *
     * @param value
     * @return The converted double value or the given value.
     */
    protected Object attemptGetNumericValue(Object value)
    {
        Double numValue = getNumericValue(value);

        return numValue != null ? numValue : value;
    }

    protected boolean checkObjectValue(Object fieldValue)
    {
        return checkObjectValue(fieldValue, this.value, this.value2);
    }

    protected boolean checkObjectValue(Object fieldValue, Object value1)
    {
        return checkObjectValue(fieldValue, value1, this.value2);
    }

    protected boolean checkObjectValue(Object fieldValue, Object value1, Object value2)
    {
        boolean result = false;

        switch (this.operation)
        {
            case SqlPredicate.IS_NULL:
                result = fieldValue == null;
                break;
            case SqlPredicate.IS_NOT_NULL:
                result = fieldValue != null;
                break;
            case SqlPredicate.EQUALS:
                if (fieldValue == null)
                {
                    result = false;
                }
                else
                {
                    result = attemptGetNumericValue(fieldValue).equals(attemptGetNumericValue(value1));
                }
                break;
            case SqlPredicate.NOT_EQUAL:
                if (fieldValue == null)
                {
                    result = true;
                }
                else
                {
                    result = !attemptGetNumericValue(fieldValue).equals(attemptGetNumericValue(value1));
                }
                break;
            case SqlPredicate.GREATER_THAN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue > getNumericValue(value1);
                    }

                }
                catch (NumberFormatException e)
                {
                    result = false;
                }
                break;
            case SqlPredicate.LESS_THAN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue < getNumericValue(value1);
                    }

                }
                catch (NumberFormatException e)
                {
                    result = false;
                }
                break;
            case SqlPredicate.GREATER_EQUAL_THAN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue >= getNumericValue(value1);
                    }

                }
                catch (NumberFormatException e)
                {
                    result = false;
                }
                break;
            case SqlPredicate.LESS_EQUAL_THAN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue <= getNumericValue(value1);
                    }

                }
                catch (NumberFormatException e)
                {
                    result = false;
                }
                break;
            case SqlPredicate.LIKE:
                if (fieldValue == null)
                {
                    result = false;
                }
                else if (this.likePattern == null)
                {
                    result = false;
                }
                else
                {
                    result = this.likePattern.matcher(fieldValue.toString().toLowerCase()).matches();
                }
                break;
            case SqlPredicate.NOT_LIKE:
                if (fieldValue == null)
                {
                    result = true;
                }
                else if (this.likePattern == null)
                {
                    result = false;
                }
                else
                {
                    result = !this.likePattern.matcher(fieldValue.toString().toLowerCase()).matches();
                }
                break;
            case SqlPredicate.IN:
                if (fieldValue == null)
                {
                    result = false;
                }
                else if (value1 instanceof List)
                {
                    List list = (List)value1;

                    if (fieldValue instanceof Number)
                    {
                        list = (List)list.stream().map(this::attemptGetNumericValue).collect(Collectors.toList());
                        fieldValue = attemptGetNumericValue(fieldValue);
                    }

                    result = list.contains(fieldValue);
                }
                break;
            case SqlPredicate.NOT_IN:
                if (fieldValue == null)
                {
                    result = true;
                }
                else if (value1 instanceof List)
                {
                    List list = (List)value1;

                    if (fieldValue instanceof Number)
                    {
                        list = (List)list.stream().map(this::attemptGetNumericValue).collect(Collectors.toList());
                        fieldValue = attemptGetNumericValue(fieldValue);
                    }

                    result = !list.contains(fieldValue);
                }
                break;
            case SqlPredicate.BETWEEN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue >= getNumericValue(value1) && numericValue <= getNumericValue(value2);
                    }

                }
                catch (NumberFormatException e)
                {
                    result = false;
                }

                break;
            case SqlPredicate.NOT_BETWEEN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue >= getNumericValue(value1) && numericValue <= getNumericValue(value2);
                        result = !result;
                    }
                }
                catch (NumberFormatException e)
                {
                    result = false;
                }
                break;
        }

        return result;
    }

    protected boolean checkStringValue(String fieldValue)
    {
        return checkStringValue(fieldValue, this.value, this.value2);
    }

    protected boolean checkStringValue(String fieldValue, Object value1)
    {
        return checkStringValue(fieldValue, value1, this.value2);
    }

    protected boolean checkStringValue(String fieldValue, Object value1, Object value2)
    {
        boolean result = false;

        switch (this.operation)
        {
            case SqlPredicate.IS_NULL:
                result = fieldValue == null;
                break;
            case SqlPredicate.IS_NOT_NULL:
                result = fieldValue != null;
                break;
            case SqlPredicate.EQUALS:
                if (fieldValue == null)
                {
                    result = false;
                }
                else
                {
                    result = fieldValue.equalsIgnoreCase(value1.toString());
                }
                break;
            case SqlPredicate.NOT_EQUAL:
                if (fieldValue == null)
                {
                    result = true;
                }
                else
                {
                    result = !fieldValue.equalsIgnoreCase(value1.toString());
                }
                break;
            case SqlPredicate.GREATER_THAN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue > getNumericValue(value1);
                    }

                }
                catch (NumberFormatException e)
                {
                    result = false;
                }
                break;
            case SqlPredicate.LESS_THAN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue < getNumericValue(value1);
                    }

                }
                catch (NumberFormatException e)
                {
                    result = false;
                }
                break;
            case SqlPredicate.GREATER_EQUAL_THAN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue >= getNumericValue(value1);
                    }

                }
                catch (NumberFormatException e)
                {
                    result = false;
                }
                break;
            case SqlPredicate.LESS_EQUAL_THAN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue <= getNumericValue(value1);
                    }

                }
                catch (NumberFormatException e)
                {
                    result = false;
                }
                break;
            case SqlPredicate.LIKE:
                if (fieldValue == null)
                {
                    result = false;
                }
                else if (this.likePattern == null)
                {
                    result = false;
                }
                else
                {
                    result = this.likePattern.matcher(fieldValue.toLowerCase()).matches();
                }
                break;
            case SqlPredicate.NOT_LIKE:
                if (fieldValue == null)
                {
                    result = true;
                }
                else if (this.likePattern == null)
                {
                    result = false;
                }
                else
                {
                    result = !this.likePattern.matcher(fieldValue.toLowerCase()).matches();
                }
                break;
            case SqlPredicate.IN:
                if (fieldValue == null)
                {
                    result = false;
                }
                else if (value1 instanceof List)
                {
                    List stringList = (List)value1;
                    result = stringList.contains(fieldValue);
                }
                break;
            case SqlPredicate.NOT_IN:
                if (fieldValue == null)
                {
                    result = true;
                }
                else if (value1 instanceof List)
                {
                    List stringList = (List)value1;
                    result = !stringList.contains(fieldValue);
                }
                break;
            case SqlPredicate.BETWEEN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue >= getNumericValue(value1) && numericValue <= getNumericValue(value2);
                    }

                }
                catch (NumberFormatException e)
                {
                    result = false;
                }

                break;
            case SqlPredicate.NOT_BETWEEN:
                try
                {
                    if (fieldValue == null)
                    {
                        result = false;
                    }
                    else
                    {
                        double numericValue = getNumericValue(fieldValue);
                        result = numericValue >= getNumericValue(value1) && numericValue <= getNumericValue(value2);
                        result = !result;
                    }
                }
                catch (NumberFormatException e)
                {
                    result = false;
                }
                break;
        }

        return result;
    }

    /**
     * @see Predicate#test(Object)
     */
    @Override
    public boolean test(T t)
    {
        if (this.defaultValue != null)
        {
            return this.defaultValue;
        }

        if (t == null)
        {
            return false;
        }

        return check(t);
    }

    /**
     * Method which should contain the logic for checking the given value.
     *
     * <p>
     * The following things have been done beforehand:
     * <ul>
     *     <li>The defaultValue of this predicate has been checked. If it was not null it has been returned</li>
     *     <li>The given value has been checked for null. false was returned if it was null</li>
     * </ul>
     * </p>
     * <p>
     * This class contains methods such as {@link SqlPredicate#checkObjectValue(Object)} and {@link SqlPredicate#checkStringValue(String)}
     * which can be used by the implementations. In most cases this means that the only task for a subclass is to retrieve the values to check.
     * </p>
     *
     * @param t
     * @return
     */
    protected abstract boolean check(T t);
}