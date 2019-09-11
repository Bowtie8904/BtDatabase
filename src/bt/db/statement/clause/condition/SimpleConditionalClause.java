package bt.db.statement.clause.condition;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import bt.db.constants.SqlType;
import bt.db.func.SqlFunction;
import bt.db.statement.clause.ColumnEntry;
import bt.db.statement.impl.SelectStatement;

/**
 * @author &#8904
 *
 */
public class SimpleConditionalClause<T>
{
    protected static final String OR = "OR";
    protected static final String AND = "AND";
    protected static final String HAVING = "HAVING";
    protected static final String ON = "ON";
    protected static final String EQUALS = "=";
    protected static final String GREATER = ">";
    protected static final String LESS = "<";
    protected static final String GREATER_EQUALS = ">=";
    protected static final String LESS_EQUALS = "<=";
    protected static final String NOT = "!=";
    protected static final String IS_NOT_NULL = "IS NOT NULL";
    protected static final String IS_NULL = "IS NULL";
    protected static final String LIKE = "LIKE";
    protected static final String BETWEEN = "BETWEEN";
    protected static final String IN = "IN";
    protected static final String NOT_IN = "NOT IN";

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
    protected String value;

    protected T caller;

    public SimpleConditionalClause(T caller, String column, String keyword)
    {
        this.caller = caller;
        this.column = column;
        this.keyword = keyword;
    }

    public SimpleConditionalClause(String column, String keyword)
    {
        this.column = column;
        this.keyword = keyword;
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

        if (this.operator.equals(IS_NOT_NULL) || this.operator.equals(IS_NULL))
        {
            return this.keyword + " " + this.column + " " + this.operator;
        }
        else if (this.valueType == ValueType.STRING || this.valueType == ValueType.DATE
                 || this.valueType == ValueType.TIME || this.valueType == ValueType.TIMESTAMP)
        {
            return this.keyword + " " + this.column + " " + this.operator + " '" + this.value + "'";
        }

        return this.keyword + " " + this.column + " " + this.operator + " " + this.value;
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

        ValueType valueType1 = ValueType.getType(value1);
        ValueType valueType2 = ValueType.getType(value1);

        if (valueType1 == null || valueType2 == null)
        {
            throw new IllegalArgumentException("One or more argument types are not suitable for a BETWEEN clause.");
        }

        this.value = "";

        if (valueType1 == ValueType.DATE || valueType1 == ValueType.TIME
            || valueType1 == ValueType.TIMESTAMP)
        {
            this.value += "'" + value1 + "'";
        }
        else
        {
            this.value += value1;
        }

        if (valueType2 == ValueType.DATE || valueType2 == ValueType.TIME
            || valueType2 == ValueType.TIMESTAMP)
        {
            this.value += " " + AND + " '" + value2 + "'";
        }
        else
        {
            this.value += " " + AND + " " + value2;
        }

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
     * @return The caller that created this conditional.
     */
    public T equals(Date value)
    {
        this.valueType = ValueType.DATE;
        this.operator = EQUALS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T equals(Time value)
    {
        this.valueType = ValueType.TIME;
        this.operator = EQUALS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T equals(Timestamp value)
    {
        this.valueType = ValueType.TIMESTAMP;
        this.operator = EQUALS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T equals(byte value)
    {
        this.valueType = ValueType.BYTE;
        this.operator = EQUALS;
        this.value = Byte.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T equals(short value)
    {
        this.valueType = ValueType.SHORT;
        this.operator = EQUALS;
        this.value = Short.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T equals(int value)
    {
        this.valueType = ValueType.INTEGER;
        this.operator = EQUALS;
        this.value = Integer.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T equals(long value)
    {
        this.valueType = ValueType.LONG;
        this.operator = EQUALS;
        this.value = Long.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T equals(double value)
    {
        this.valueType = ValueType.DOUBLE;
        this.operator = EQUALS;
        this.value = Double.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T equals(float value)
    {
        this.valueType = ValueType.FLOAT;
        this.operator = EQUALS;
        this.value = Float.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T equals(boolean value)
    {
        this.valueType = ValueType.BOOLEAN;
        this.operator = EQUALS;
        this.value = Boolean.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T equals(String value)
    {
        this.valueType = ValueType.STRING;
        this.operator = EQUALS;
        this.value = value;
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
     * @return The caller that created this conditional.
     */
    public T like(String value)
    {
        this.valueType = ValueType.STRING;
        this.operator = LIKE;
        this.value = value;
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
     * @return The caller that created this conditional.
     */
    public T not(Date value)
    {
        this.valueType = ValueType.DATE;
        this.operator = NOT;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T not(Time value)
    {
        this.valueType = ValueType.TIME;
        this.operator = NOT;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T not(Timestamp value)
    {
        this.valueType = ValueType.TIMESTAMP;
        this.operator = NOT;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T not(byte value)
    {
        this.valueType = ValueType.BYTE;
        this.operator = NOT;
        this.value = Byte.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T not(short value)
    {
        this.valueType = ValueType.SHORT;
        this.operator = NOT;
        this.value = Short.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T not(int value)
    {
        this.valueType = ValueType.INTEGER;
        this.operator = NOT;
        this.value = Integer.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T not(long value)
    {
        this.valueType = ValueType.LONG;
        this.operator = NOT;
        this.value = Long.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T not(double value)
    {
        this.valueType = ValueType.DOUBLE;
        this.operator = NOT;
        this.value = Double.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T not(float value)
    {
        this.valueType = ValueType.FLOAT;
        this.operator = NOT;
        this.value = Float.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T not(boolean value)
    {
        this.valueType = ValueType.BOOLEAN;
        this.operator = NOT;
        this.value = Boolean.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T not(String value)
    {
        this.valueType = ValueType.STRING;
        this.operator = NOT;
        this.value = value;
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
     * @return The caller that created this conditional.
     */
    public T greaterThan(Date value)
    {
        this.valueType = ValueType.DATE;
        this.operator = GREATER;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T greaterThan(Time value)
    {
        this.valueType = ValueType.TIME;
        this.operator = GREATER;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T greaterThan(Timestamp value)
    {
        this.valueType = ValueType.TIMESTAMP;
        this.operator = GREATER;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T greaterThan(byte value)
    {
        this.valueType = ValueType.BYTE;
        this.operator = GREATER;
        this.value = Byte.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T greaterThan(short value)
    {
        this.valueType = ValueType.SHORT;
        this.operator = GREATER;
        this.value = Short.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T greaterThan(int value)
    {
        this.valueType = ValueType.INTEGER;
        this.operator = GREATER;
        this.value = Integer.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T greaterThan(long value)
    {
        this.valueType = ValueType.LONG;
        this.operator = GREATER;
        this.value = Long.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T greaterThan(double value)
    {
        this.valueType = ValueType.DOUBLE;
        this.operator = GREATER;
        this.value = Double.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T greaterThan(float value)
    {
        this.valueType = ValueType.FLOAT;
        this.operator = GREATER;
        this.value = Float.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessThan(Date value)
    {
        this.valueType = ValueType.DATE;
        this.operator = LESS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T lessThan(Time value)
    {
        this.valueType = ValueType.TIME;
        this.operator = LESS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T lessThan(Timestamp value)
    {
        this.valueType = ValueType.TIMESTAMP;
        this.operator = LESS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T lessThan(byte value)
    {
        this.valueType = ValueType.BYTE;
        this.operator = LESS;
        this.value = Byte.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessThan(short value)
    {
        this.valueType = ValueType.SHORT;
        this.operator = LESS;
        this.value = Short.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessThan(int value)
    {
        this.valueType = ValueType.INTEGER;
        this.operator = LESS;
        this.value = Integer.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessThan(long value)
    {
        this.valueType = ValueType.LONG;
        this.operator = LESS;
        this.value = Long.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessThan(double value)
    {
        this.valueType = ValueType.DOUBLE;
        this.operator = LESS;
        this.value = Double.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessThan(float value)
    {
        this.valueType = ValueType.FLOAT;
        this.operator = LESS;
        this.value = Float.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(Date value)
    {
        this.valueType = ValueType.DATE;
        this.operator = GREATER_EQUALS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(Time value)
    {
        this.valueType = ValueType.TIME;
        this.operator = GREATER_EQUALS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(Timestamp value)
    {
        this.valueType = ValueType.TIMESTAMP;
        this.operator = GREATER_EQUALS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(byte value)
    {
        this.valueType = ValueType.BYTE;
        this.operator = GREATER_EQUALS;
        this.value = Byte.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(short value)
    {
        this.valueType = ValueType.SHORT;
        this.operator = GREATER_EQUALS;
        this.value = Short.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(int value)
    {
        this.valueType = ValueType.INTEGER;
        this.operator = GREATER_EQUALS;
        this.value = Integer.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(long value)
    {
        this.valueType = ValueType.LONG;
        this.operator = GREATER_EQUALS;
        this.value = Long.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(double value)
    {
        this.valueType = ValueType.DOUBLE;
        this.operator = GREATER_EQUALS;
        this.value = Double.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(float value)
    {
        this.valueType = ValueType.FLOAT;
        this.operator = GREATER_EQUALS;
        this.value = Float.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(Date value)
    {
        this.valueType = ValueType.DATE;
        this.operator = LESS_EQUALS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(Time value)
    {
        this.valueType = ValueType.TIME;
        this.operator = LESS_EQUALS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(Timestamp value)
    {
        this.valueType = ValueType.TIMESTAMP;
        this.operator = LESS_EQUALS;
        this.value = value.toString();
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
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(byte value)
    {
        this.valueType = ValueType.BYTE;
        this.operator = LESS_EQUALS;
        this.value = Byte.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(short value)
    {
        this.valueType = ValueType.SHORT;
        this.operator = LESS_EQUALS;
        this.value = Short.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(int value)
    {
        this.valueType = ValueType.INTEGER;
        this.operator = LESS_EQUALS;
        this.value = Integer.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(long value)
    {
        this.valueType = ValueType.LONG;
        this.operator = LESS_EQUALS;
        this.value = Long.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(double value)
    {
        this.valueType = ValueType.DOUBLE;
        this.operator = LESS_EQUALS;
        this.value = Double.toString(value);
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
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(float value)
    {
        this.valueType = ValueType.FLOAT;
        this.operator = LESS_EQUALS;
        this.value = Float.toString(value);
        return this.caller;
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
     * Adds the given select statement to the right side of this conditional.
     *
     * <p>
     * This conditional will then check whether the left side is contained in the values returned by the given select.
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
    public T in(SelectStatement select)
    {
        if (select.isPrepared())
        {
            throw new IllegalArgumentException("Subselects must be marked as 'unprepared'.");
        }

        this.valueType = ValueType.SUBSELECT;
        this.operator = IN;
        this.value = "(" + select.toString() + ")";

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
     * @return The caller that created this conditional.
     */
    public T notIn(SelectStatement select)
    {
        if (select.isPrepared())
        {
            throw new IllegalArgumentException("Subselects must be marked as 'unprepared'.");
        }

        this.valueType = ValueType.SUBSELECT;
        this.operator = NOT_IN;
        this.value = "(" + select.toString() + ")";

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
     * @return The caller that created this conditional.
     */
    public T in(Object... array)
    {
        this.valueType = ValueType.ARRAY;
        this.operator = IN;
        this.value = SqlType.arrayToString(array);

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
     * @return The caller that created this conditional.
     */
    public T notIn(Object... array)
    {
        this.valueType = ValueType.ARRAY;
        this.operator = NOT_IN;
        this.value = SqlType.arrayToString(array);

        return this.caller;
    }

    /**
     * Adds the given column to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is EQUAL to the right side column value.
     * </p>
     *
     * @param value
     *            The column whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T equals(ColumnEntry value)
    {
        this.valueType = ValueType.COLUMN;
        this.operator = EQUALS;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given column to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is LIKE the right side column value.
     * </p>
     *
     * @param value
     *            The column whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T like(ColumnEntry value)
    {
        this.valueType = ValueType.COLUMN;
        this.operator = LIKE;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given column to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is NOT EQUAL to the right side column value.
     * </p>
     *
     * @param value
     *            The column whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T not(ColumnEntry value)
    {
        this.valueType = ValueType.COLUMN;
        this.operator = NOT;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given column to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is GREATER THAN the right side column value.
     * </p>
     *
     * @param value
     *            The column whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T greaterThan(ColumnEntry value)
    {
        this.valueType = ValueType.COLUMN;
        this.operator = GREATER;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given column to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is LESS THAN the right side column value.
     * </p>
     *
     * @param value
     *            The column whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T lessThan(ColumnEntry value)
    {
        this.valueType = ValueType.COLUMN;
        this.operator = LESS;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given column to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is GREATER THAN OR EQUAL TO the right side column value.
     * </p>
     *
     * @param value
     *            The column whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(ColumnEntry value)
    {
        this.valueType = ValueType.COLUMN;
        this.operator = GREATER_EQUALS;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given column to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is LESS THAN OR EQUAL TO the right side column value.
     * </p>
     *
     * @param value
     *            The column whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(ColumnEntry value)
    {
        this.valueType = ValueType.COLUMN;
        this.operator = LESS_EQUALS;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given function to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is EQUAL to the right side column value.
     * </p>
     *
     * @param value
     *            The function whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T equals(SqlFunction value)
    {
        this.valueType = ValueType.FUNCTION;
        this.operator = EQUALS;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given function to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is LIKE the right side column value.
     * </p>
     *
     * @param value
     *            The function whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T like(SqlFunction value)
    {
        this.valueType = ValueType.FUNCTION;
        this.operator = LIKE;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given function to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is NOT EQUAL to the right side column value.
     * </p>
     *
     * @param value
     *            The function whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T not(SqlFunction value)
    {
        this.valueType = ValueType.FUNCTION;
        this.operator = NOT;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given function to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is GREATER THAN the right side column value.
     * </p>
     *
     * @param value
     *            The function whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T greaterThan(SqlFunction value)
    {
        this.valueType = ValueType.FUNCTION;
        this.operator = GREATER;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given function to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is LESS THAN the right side column value.
     * </p>
     *
     * @param value
     *            The function whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T lessThan(SqlFunction value)
    {
        this.valueType = ValueType.FUNCTION;
        this.operator = LESS;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given function to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is GREATER THAN OR EQUAL TO the right side column value.
     * </p>
     *
     * @param value
     *            The function whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T greaterOrEqual(SqlFunction value)
    {
        this.valueType = ValueType.FUNCTION;
        this.operator = GREATER_EQUALS;

        this.value = value.toString();
        return this.caller;
    }

    /**
     * Adds the given function to the right side of this conditional to check against.
     *
     * <p>
     * This conditional will then check whether the left side is LESS THAN OR EQUAL TO the right side column value.
     * </p>
     *
     * @param value
     *            The function whichs value should be used to check against.
     * @return The caller that created this conditional.
     */
    public T lessOrEqual(SqlFunction value)
    {
        this.valueType = ValueType.FUNCTION;
        this.operator = LESS_EQUALS;

        this.value = value.toString();
        return this.caller;
    }
}