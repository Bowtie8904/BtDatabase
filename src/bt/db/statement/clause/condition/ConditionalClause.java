package bt.db.statement.clause.condition;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import bt.db.constants.SqlType;
import bt.db.func.SqlFunction;
import bt.db.statement.clause.ColumnEntry;
import bt.db.statement.impl.SelectStatement;
import bt.db.statement.value.Preparable;
import bt.db.statement.value.Value;

/**
 * @author &#8904
 *
 */
public class ConditionalClause<T> implements Preparable
{
    public static final String WHERE = "WHERE";
    public static final String OR = "OR";
    public static final String AND = "AND";
    public static final String HAVING = "HAVING";
    public static final String ON = "ON";
    protected static final String EQUALS = "=";
    protected static final String GREATER = ">";
    protected static final String LESS = "<";
    protected static final String GREATER_EQUALS = ">=";
    protected static final String LESS_EQUALS = "<=";
    protected static final String NOT_EQUAL = "!=";
    protected static final String IS_NOT_NULL = "IS NOT NULL";
    protected static final String IS_NULL = "IS NULL";
    protected static final String LIKE = "LIKE";
    protected static final String BETWEEN = "BETWEEN";
    protected static final String IN = "IN";
    protected static final String NOT_IN = "NOT IN";
    protected static final String NOT = "NOT";
    protected static final String EXISTS = "EXISTS";

    protected enum ValueType
    {
        BYTE,
        SHORT,
        INTEGER,
        LONG,
        DOUBLE,
        FLOAT,
        BOOLEAN,
        STRING,
        DATE,
        TIME,
        TIMESTAMP,
        NULL,
        NOT_NULL,
        COLUMN,
        FUNCTION,
        ARRAY,
        BETWEEN,
        SUBSELECT;

        public static ValueType getType(Object value)
        {
            if (value instanceof Byte)
            {
                return BYTE;
            }
            else if (value instanceof Short)
            {
                return SHORT;
            }
            else if (value instanceof Integer)
            {
                return INTEGER;
            }
            else if (value instanceof Long)
            {
                return LONG;
            }
            else if (value instanceof Double)
            {
                return DOUBLE;
            }
            else if (value instanceof Float)
            {
                return FLOAT;
            }
            else if (value instanceof Boolean)
            {
                return BOOLEAN;
            }
            else if (value instanceof String)
            {
                return STRING;
            }
            else if (value instanceof Date)
            {
                return DATE;
            }
            else if (value instanceof Time)
            {
                return TIME;
            }
            else if (value instanceof Timestamp)
            {
                return TIMESTAMP;
            }
            else if (value instanceof ColumnEntry)
            {
                return COLUMN;
            }
            else if (value instanceof SqlFunction)
            {
                return FUNCTION;
            }

            return null;
        }
    }

    /** The type of the value that is used. */
    protected ValueType valueType;

    protected String column;

    /** The keyword that is used. I.e. WHERE, AND, HAVING, ... */
    protected String keyword;

    /** The operator used to evaluate this condition. I.e. =, IN, BETWEEN, <=, ... */
    protected String operator;

    /** The value that is checked against (right side). */
    protected Object value;

    protected Object betweenValue1;
    protected Object betweenValue2;

    protected boolean negateExpression = false;

    protected T caller;

    protected List<Value> values;

    protected SelectStatement subSelect;

    protected String prefix = "";
    protected String postfix = "";

    public ConditionalClause(T caller, String column, String keyword)
    {
        this.caller = caller;
        this.column = column;
        this.keyword = keyword;
        this.values = new ArrayList<>();
    }

    public ConditionalClause(T caller, String prefix, String column, String keyword)
    {
        this(caller, column, keyword);
        this.prefix = prefix;
    }

    /**
     * Returns the String representing this conditional clause.
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        if (this.operator == null)
        {
            return "Operator null";
        }

        if (this.valueType.equals(ValueType.COLUMN))
        {
            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + " " + this.value.toString() + this.postfix;
        }

        if (this.operator.equals(BETWEEN))
        {
            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + getBetweenString(true) + this.postfix;
        }

        if (this.valueType.equals(ValueType.SUBSELECT))
        {
            this.subSelect.prepared();
            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + " (" + this.subSelect.toString() + ")" + this.postfix;
        }

        if (this.valueType.equals(ValueType.FUNCTION))
        {
            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + " " + ((SqlFunction)this.value).toString(true) + this.postfix;
        }

        if (this.valueType.equals(ValueType.ARRAY))
        {
            String arrayString = " (";
            Object[] array = (Object[])this.value;

            for (Object obj : array)
            {
                arrayString += "?, ";
            }

            arrayString = arrayString.substring(0, arrayString.length() - 2);
            arrayString += ")";

            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + arrayString + this.postfix;
        }

        if (this.operator.equals(IS_NOT_NULL) || this.operator.equals(IS_NULL))
        {
            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + this.postfix;
        }

        return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + " ?" + this.postfix;
    }

    /**
     * Returns the String representing this conditional clause.
     *
     * @param prepared
     *            Indicates whether values should be treated for prepared statements or inserted as plain text. true =
     *            use prepared statement, false = insert plain.
     */
    public String toString(boolean prepared)
    {
        if (this.operator == null)
        {
            return "Operator null";
        }

        if (this.operator.equals(BETWEEN))
        {
            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + getBetweenString(prepared) + this.postfix;
        }

        if (prepared)
        {
            return toString();
        }

        if (this.valueType.equals(ValueType.SUBSELECT))
        {
            this.subSelect.unprepared();
            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + " (" + this.subSelect.toString() + ")" + this.postfix;
        }

        if (this.valueType.equals(ValueType.FUNCTION))
        {
            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + " " + ((SqlFunction)this.value).toString(prepared) + this.postfix;
        }

        if (this.valueType.equals(ValueType.ARRAY))
        {
            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + " " + SqlType.arrayToString((Object[])this.value) + this.postfix;
        }

        if (this.operator.equals(IS_NOT_NULL) || this.operator.equals(IS_NULL))
        {
            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + this.postfix;
        }
        else if (this.valueType == ValueType.STRING || this.valueType == ValueType.DATE
                 || this.valueType == ValueType.TIME || this.valueType == ValueType.TIMESTAMP)
        {
            return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + " '" + this.value + "'" + this.postfix;
        }

        return this.keyword + (this.negateExpression ? " " + NOT + " " : " ") + this.prefix + this.column + " " + this.operator + " " + this.value + this.postfix;
    }

    public String getColumn()
    {
        return this.column;
    }

    public String getKeyword()
    {
        return this.keyword;
    }

    public boolean isNegated()
    {
        return this.negateExpression;
    }

    protected void setValueVariables(Object value)
    {
        ValueType type = ValueType.getType(value);
        if (value instanceof SelectStatement)
        {
            this.valueType = ValueType.SUBSELECT;
            this.subSelect = (SelectStatement)value;
            this.values.addAll(((SelectStatement)value).getValues());
        }
        else if (type.equals(ValueType.COLUMN))
        {
            this.valueType = ValueType.COLUMN;
            this.value = value;
        }
        else if (type.equals(ValueType.FUNCTION))
        {
            this.valueType = ValueType.FUNCTION;
            this.value = value;
            this.values.addAll(((SqlFunction)value).getValues());
        }
        else
        {
            this.valueType = ValueType.getType(value);
            this.value = value.toString();
            this.values.add(new Value(SqlType.convert(value.getClass()), value));
        }
    }

    protected String getBetweenString(boolean prepared)
    {
        ValueType valueType1 = ValueType.getType(this.betweenValue1);
        ValueType valueType2 = ValueType.getType(this.betweenValue2);

        if (valueType1 == null || valueType2 == null)
        {
            throw new IllegalArgumentException("One or more argument types are not suitable for a BETWEEN clause.");
        }

        String str = "";

        if (prepared)
        {
            str = " ? " + AND + " ?";
        }
        else
        {
            if (valueType1 == ValueType.DATE || valueType1 == ValueType.TIME
                || valueType1 == ValueType.TIMESTAMP)
            {
                str += " '" + this.betweenValue1 + "'";
            }
            else
            {
                str += " " + this.betweenValue1;
            }

            if (valueType2 == ValueType.DATE || valueType2 == ValueType.TIME
                || valueType2 == ValueType.TIMESTAMP)
            {
                str += " " + AND + " '" + this.betweenValue2 + "'";
            }
            else
            {
                str += " " + AND + " " + this.betweenValue2;
            }
        }

        return str;
    }

    /**
     * Adds the given between condition to this conditional statement.
     *
     * <p>
     * This conditional will then check whether the value in the set column is between (inclusive) the given values.
     * </p>
     *
     * <p>
     * Valid value types are:
     * <ul>
     * <li>int</li>
     * <li>long</li>
     * <li>byte</li>
     * <li>short</li>
     * <li>double</li>
     * <li>float</li>
     * <li>{@link Date}</li>
     * <li>{@link Time}</li>
     * <li>{@link Timestamp}</li>
     * </ul>
     * </p>
     *
     * @param value1
     *            The lower bounds (inclusive).
     * @param value2
     *            The upper bounds (inclusive).
     * @return The caller that created this conditional.
     */
    public T between(Object value1, Object value2)
    {
        this.operator = BETWEEN;
        this.valueType = ValueType.BETWEEN;
        this.betweenValue1 = value1;
        this.betweenValue2 = value2;

        ValueType valueType1 = ValueType.getType(value1);
        ValueType valueType2 = ValueType.getType(value1);

        if (valueType1 == null || valueType2 == null)
        {
            throw new IllegalArgumentException("One or more argument types are not suitable for a BETWEEN clause.");
        }

        this.values.add(new Value(SqlType.convert(value1.getClass()), value1));
        this.values.add(new Value(SqlType.convert(value2.getClass()), value2));

        return this.caller;
    }

    /**
     * Adds the given between condition to this conditional statement.
     *
     * <p>
     * This conditional will then check whether the value in the set column is between (inclusive) the given values.
     * </p>
     *
     * <p>
     * Valid value types are:
     * <ul>
     * <li>int</li>
     * <li>long</li>
     * <li>byte</li>
     * <li>short</li>
     * <li>double</li>
     * <li>float</li>
     * <li>{@link Date}</li>
     * <li>{@link Time}</li>
     * <li>{@link Timestamp}</li>
     * </ul>
     * </p>
     *
     * @param value1
     *            The lower bounds (inclusive).
     * @param value2
     *            The upper bounds (inclusive).
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T between(Object value1, Object value2, String postfix)
    {
        this.postfix = postfix;
        return between(value1, value2);
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether both sides are EQUAL.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @return The caller that created this conditional.
     */
    public T equal(Object value)
    {
        this.operator = EQUALS;
        setValueVariables(value);
        return this.caller;
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether both sides are EQUAL.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T equal(Object value, String postfix)
    {
        this.postfix = postfix;
        return equal(value);
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether both sides are NOT EQUAL.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @return The caller that created this conditional.
     */
    public T notEqual(Object value)
    {
        this.operator = NOT_EQUAL;
        setValueVariables(value);
        return this.caller;
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether both sides are NOT EQUAL.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T notEqual(Object value, String postfix)
    {
        this.postfix = postfix;
        return notEqual(value);
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether both sides are LIKE each other.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @return The caller that created this conditional.
     */
    public T like(String value)
    {
        this.valueType = ValueType.STRING;
        this.operator = LIKE;
        this.value = value;
        this.values.add(new Value(SqlType.VARCHAR, value));
        return this.caller;
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether both sides are LIKE each other.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T like(String value, String postfix)
    {
        this.postfix = postfix;
        return like(value);
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is GREATER THAN the right side.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @return The caller that created this conditional.
     */
    public T greaterThan(Object value)
    {
        this.operator = GREATER;
        setValueVariables(value);
        return this.caller;
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is GREATER THAN the right side.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T greaterThan(Object value, String postfix)
    {
        this.postfix = postfix;
        return greaterThan(value);
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is LESS THAN the right side.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @return The caller that created this conditional.
     */
    public T lessThan(Object value)
    {
        this.operator = LESS;
        setValueVariables(value);
        return this.caller;
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is LESS THAN the right side.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T lessThan(Object value, String postfix)
    {
        this.postfix = postfix;
        return lessThan(value);
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is GREATER OR EQUAL THAN the right side.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(Object value)
    {
        this.operator = GREATER_EQUALS;
        setValueVariables(value);
        return this.caller;
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is GREATER OR EQUAL THAN the right side.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(Object value, String postfix)
    {
        this.postfix = postfix;
        return greaterOrEqual(value);
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is LESS OR EQUAL THAN the right side.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(Object value)
    {
        this.operator = LESS_EQUALS;
        setValueVariables(value);
        return this.caller;
    }

    /**
     * Adds the given value to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is LESS OR EQUAL THAN the right side.
     * </p>
     *
     * @param value
     *            The value to check against.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(Object value, String postfix)
    {
        this.postfix = postfix;
        return lessOrEqual(value);
    }

    /**
     * This conditional will check whether the left side IS NULL.
     *
     * @return The caller that created this conditional.
     */
    public T isNull()
    {
        this.valueType = ValueType.NULL;
        this.operator = IS_NULL;

        return this.caller;
    }

    /**
     * This conditional will check whether the left side IS NULL.
     *
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     *
     * @return The caller that created this conditional.
     */
    public T isNull(String postfix)
    {
        this.postfix = postfix;
        return isNull();
    }

    /**
     * This conditional will check whether the left side IS NOT NULL.
     *
     * @return The caller that created this conditional.
     */
    public T notNull()
    {
        this.valueType = ValueType.NOT_NULL;
        this.operator = IS_NOT_NULL;

        return this.caller;
    }

    /**
     * This conditional will check whether the left side IS NOT NULL.
     *
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T notNull(String postfix)
    {
        this.postfix = postfix;
        return notNull();
    }

    /**
     * Adds the given select statement to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is contained in the values returned by the given select.
     * </p>
     *
     * @param select
     *            The select whichs result should be used.
     * @return The caller that created this conditional.
     */
    public T in(SelectStatement select)
    {
        this.operator = IN;
        setValueVariables(select);

        return this.caller;
    }

    /**
     * Adds the given select statement to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is contained in the values returned by the given select.
     * </p>
     *
     * @param select
     *            The select whichs result should be used.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T in(SelectStatement select, String postfix)
    {
        this.postfix = postfix;
        return in(select);
    }

    /**
     * Adds the given select statement to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is NOT contained in the values returned by the given
     * select.
     * </p>
     *
     * <p>
     * The select must be marked as {@link SelectStatement#unprepared()} and can only select a single column.
     * </p>
     *
     * @param select
     *            The select whichs result should be used.
     * @return The caller that created this conditional.
     */
    public T notIn(SelectStatement select)
    {
        this.operator = NOT_IN;
        setValueVariables(select);

        return this.caller;
    }

    /**
     * Adds the given select statement to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is NOT contained in the values returned by the given
     * select.
     * </p>
     *
     * <p>
     * The select must be marked as {@link SelectStatement#unprepared()} and can only select a single column.
     * </p>
     *
     * @param select
     *            The select whichs result should be used.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T notIn(SelectStatement select, String postfix)
    {
        this.postfix = postfix;
        return notIn(select);
    }

    /**
     * Adds the given array to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is contained in the values inside the array.
     * </p>
     *
     * @param array
     *            The array to use.
     * @return The caller that created this conditional.
     */
    public T in(Object... array)
    {
        this.valueType = ValueType.ARRAY;
        this.operator = IN;
        this.value = array;

        for (Object obj : array)
        {
            this.values.add(new Value(SqlType.convert(obj.getClass()), obj));
        }

        return this.caller;
    }

    /**
     * Adds the given array to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is contained in the values inside the array.
     * </p>
     *
     * @param array
     *            The array to use.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T in(Object[] array, String postfix)
    {
        this.postfix = postfix;
        return in(array);
    }

    /**
     * Adds the given array to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is NOT contained in the values inside the array.
     * </p>
     *
     * @param array
     *            The array to use.
     * @return The caller that created this conditional.
     */
    public T notIn(Object... array)
    {
        this.valueType = ValueType.ARRAY;
        this.operator = NOT_IN;
        this.value = array;

        for (Object obj : array)
        {
            this.values.add(new Value(SqlType.convert(obj.getClass()), obj));
        }

        return this.caller;
    }

    /**
     * Adds the given array to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is NOT contained in the values inside the array.
     * </p>
     *
     * @param array
     *            The array to use.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T notIn(Object[] array, String postfix)
    {
        this.postfix = postfix;
        return notIn(array);
    }

    /**
     * Adds the given list to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is contained in the values inside the list.
     * </p>
     *
     * @param list
     *            The list to use.
     * @return The caller that created this conditional.
     */
    public T in(List list)
    {
        return in(list.toArray(), "");
    }

    /**
     * Adds the given list to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is contained in the values inside the list.
     * </p>
     *
     * @param list
     *            The list to use.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T in(List list, String postfix)
    {
        return in(list.toArray(), postfix);
    }

    /**
     * Adds the given list to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is NOT contained in the values inside the list.
     * </p>
     *
     * @param list
     *            The list to use.
     * @return The caller that created this conditional.
     */
    public T notIn(List list)
    {
        return notIn(list.toArray(), "");
    }

    /**
     * Adds the given list to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is NOT contained in the values inside the list.
     * </p>
     *
     * @param list
     *            The list to use.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T notIn(List list, String postfix)
    {
        return notIn(list.toArray(), postfix);
    }

    public ConditionalClause<T> not()
    {
        this.negateExpression = true;
        return this;
    }

    /**
     * Adds the given select statement to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the right side select returns any data.
     * </p>
     *
     * @param select
     *            The select whichs result should be used.
     * @return The caller that created this conditional.
     */
    public T exists(SelectStatement select)
    {
        this.operator = EXISTS;
        setValueVariables(select);

        return this.caller;
    }

    /**
     * Adds the given select statement to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the right side select returns any data.
     * </p>
     *
     * @param select
     *            The select whichs result should be used.
     * @param postfix
     *            A String that will be added after the expression. Can be used for parenthesis.
     * @return The caller that created this conditional.
     */
    public T exists(SelectStatement select, String postfix)
    {
        this.operator = EXISTS;
        setValueVariables(select);

        return this.caller;
    }

    /**
     * @see bt.db.statement.value.Preparable#getValues()
     */
    @Override
    public List<Value> getValues()
    {
        return this.values;
    }
}