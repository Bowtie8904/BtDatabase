package bt.db.constants;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Defines sql types that are usable within this library.
 * 
 * @author &#8904
 */
public enum SqlType
{
    BOOLEAN("BOOLEAN", Types.BOOLEAN),
    VARCHAR("VARCHAR", Types.VARCHAR),
    INTEGER("INTEGER", Types.INTEGER),
    LONG("BIGINT", Types.BIGINT),
    DOUBLE("DOUBLE", Types.DOUBLE),
    FLOAT("FLOAT", Types.FLOAT),
    DATE("DATE", Types.DATE),
    TIME("TIME", Types.TIME),
    TIMESTAMP("TIMESTAMP", Types.TIMESTAMP),
    CLOB("CLOB", Types.CLOB),
    BLOB("BLOB", Types.BLOB),
    UNKNOWN("UNKNOWN", -1);

    private String name;
    private int intType;

    SqlType(String name, int intType)
    {
        this.name = name;
        this.intType = intType;
    }

    /**
     * Returns the name of the Apache Derby type represented by the specific {@link SqlType}.
     * 
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString()
    {
        return this.name;
    }

    /**
     * Gets the integer constant based on {@link Types} that represents this sql type.
     * 
     * @return The int constant.
     */
    public int getIntType()
    {
        return this.intType;
    }

    /**
     * Converts the int constant of {@link Types} to a known {@link SqlType}.
     * 
     * @param type
     *            The integer constant.
     * @return The sql type or null if the type is unknown.
     */
    public static SqlType convert(int type)
    {
        SqlType sqlType = null;
        switch (type)
        {
        case Types.BOOLEAN:
            sqlType = BOOLEAN;
            break;
        case Types.CHAR:
            sqlType = VARCHAR;
            break;
        case Types.VARCHAR:
            sqlType = VARCHAR;
            break;
        case Types.LONGVARCHAR:
            sqlType = VARCHAR;
            break;
        case Types.INTEGER:
            sqlType = INTEGER;
            break;
        case Types.BIGINT:
            sqlType = LONG;
            break;
        case Types.FLOAT:
            sqlType = FLOAT;
            break;
        case Types.DOUBLE:
            sqlType = DOUBLE;
            break;
        case Types.DATE:
            sqlType = DATE;
            break;
        case Types.TIME:
            sqlType = TIME;
            break;
        case Types.TIMESTAMP:
            sqlType = TIMESTAMP;
            break;
        case Types.CLOB:
            sqlType = CLOB;
            break;
        case Types.BLOB:
            sqlType = BLOB;
            break;
        default:
            sqlType = UNKNOWN;
        }
        return sqlType;
    }

    /**
     * Returns a String representation of the given array.
     * 
     * <p>
     * The returned String will have the following format: <br>
     * <code>( value1, value2, value3, ... )</code>
     * </p>
     * 
     * <p>
     * Values of the type {@link String}, {@link Date}, {@link Time} and {@link Timestamp} will have ' attached on both
     * sides to make the String representation usable for sql statements.
     * </p>
     * 
     * @param array
     *            The array to convert to a String.
     * @return The String representation.
     */
    public static String arrayToString(Object... array)
    {
        String strArray = "( ";

        for (Object obj : array)
        {
            if (obj instanceof String || obj instanceof Date || obj instanceof Time || obj instanceof Timestamp)
            {
                strArray += "'" + obj.toString() + "', ";
            }
            else
            {
                strArray += obj.toString() + ", ";
            }
        }

        if (array.length > 0)
        {
            strArray = strArray.substring(0, strArray.length() - 2);
        }

        strArray += " )";

        return strArray;
    }

    /**
     * Returns a String representation of the given array.
     * 
     * <p>
     * The returned String will have the following format: <br>
     * <code>( value1, value2, value3, ... )</code>
     * </p>
     * 
     * @param array
     *            The array to convert to a String.
     * @return The String representation.
     */
    public static String arrayToString(byte... array)
    {
        String strArray = "( ";

        for (byte value : array)
        {
            strArray += value + ", ";
        }

        if (array.length > 0)
        {
            strArray = strArray.substring(0, strArray.length() - 2);
        }

        strArray += " )";

        return strArray;
    }

    /**
     * Returns a String representation of the given array.
     * 
     * <p>
     * The returned String will have the following format: <br>
     * <code>( value1, value2, value3, ... )</code>
     * </p>
     * 
     * @param array
     *            The array to convert to a String.
     * @return The String representation.
     */
    public static String arrayToString(short... array)
    {
        String strArray = "( ";

        for (short value : array)
        {
            strArray += value + ", ";
        }

        if (array.length > 0)
        {
            strArray = strArray.substring(0, strArray.length() - 2);
        }

        strArray += " )";

        return strArray;
    }

    /**
     * Returns a String representation of the given array.
     * 
     * <p>
     * The returned String will have the following format: <br>
     * <code>( value1, value2, value3, ... )</code>
     * </p>
     * 
     * @param array
     *            The array to convert to a String.
     * @return The String representation.
     */
    public static String arrayToString(int... array)
    {
        String strArray = "( ";

        for (int value : array)
        {
            strArray += value + ", ";
        }

        if (array.length > 0)
        {
            strArray = strArray.substring(0, strArray.length() - 2);
        }

        strArray += " )";

        return strArray;
    }

    /**
     * Returns a String representation of the given array.
     * 
     * <p>
     * The returned String will have the following format: <br>
     * <code>( value1, value2, value3, ... )</code>
     * </p>
     * 
     * @param array
     *            The array to convert to a String.
     * @return The String representation.
     */
    public static String arrayToString(long... array)
    {
        String strArray = "( ";

        for (long value : array)
        {
            strArray += value + ", ";
        }

        if (array.length > 0)
        {
            strArray = strArray.substring(0, strArray.length() - 2);
        }

        strArray += " )";

        return strArray;
    }

    /**
     * Returns a String representation of the given array.
     * 
     * <p>
     * The returned String will have the following format: <br>
     * <code>( value1, value2, value3, ... )</code>
     * </p>
     * 
     * @param array
     *            The array to convert to a String.
     * @return The String representation.
     */
    public static String arrayToString(double... array)
    {
        String strArray = "( ";

        for (double value : array)
        {
            strArray += value + ", ";
        }

        if (array.length > 0)
        {
            strArray = strArray.substring(0, strArray.length() - 2);
        }

        strArray += " )";

        return strArray;
    }

    /**
     * Returns a String representation of the given array.
     * 
     * <p>
     * The returned String will have the following format: <br>
     * <code>( value1, value2, value3, ... )</code>
     * </p>
     * 
     * @param array
     *            The array to convert to a String.
     * @return The String representation.
     */
    public static String arrayToString(float... array)
    {
        String strArray = "( ";

        for (float value : array)
        {
            strArray += value + ", ";
        }

        if (array.length > 0)
        {
            strArray = strArray.substring(0, strArray.length() - 2);
        }

        strArray += " )";

        return strArray;
    }

    /**
     * Returns a String representation of the given array.
     * 
     * <p>
     * The returned String will have the following format: <br>
     * <code>( value1, value2, value3, ... )</code>
     * </p>
     * 
     * @param array
     *            The array to convert to a String.
     * @return The String representation.
     */
    public static String arrayToString(boolean... array)
    {
        String strArray = "( ";

        for (boolean value : array)
        {
            strArray += value + ", ";
        }

        if (array.length > 0)
        {
            strArray = strArray.substring(0, strArray.length() - 2);
        }

        strArray += " )";

        return strArray;
    }
}