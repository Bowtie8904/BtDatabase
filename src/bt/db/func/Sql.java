package bt.db.func;

import bt.db.func.impl.*;
import bt.db.statement.clause.ColumnEntry;

/**
 * Contains static methods to simplify the creation of sql syntax.
 *
 * @author &#8904
 */
public final class Sql
{
    /**
     * Creates a ColumnEntry instance of a table.column combination.
     *
     * @param table  The name of the table.
     * @param column The name of the column.
     * @return The ColumnEntry instance.
     */
    public static ColumnEntry column(String table, String column)
    {
        return new ColumnEntry(table,
                               column);
    }

    /**
     * Creates a ColumnEntry instance of a given column.
     *
     * @param column The name of the column.
     * @return The ColumnEntry instance.
     */
    public static ColumnEntry column(String column)
    {
        return new ColumnEntry(column);
    }

    /**
     * The IDENTITY_VAL_LOCAL function is a non-deterministic function that returns the most recently assigned value of
     * an identity column for a connection, where the assignment occurred as a result of a single row INSERT statement
     * using a VALUES clause or a single row UPDATE statement.
     *
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction lastIdentity()
    {
        return new SqlFunction("IDENTITY_VAL_LOCAL");
    }

    /**
     * Concatenates the toString() representations of the given objects.
     *
     * @param elements The objects to concatenate.
     * @return The function whichs toString method will return a valid sql representation of this concatenation.
     */
    public static ConcatenateFunction concat(Object... elements)
    {
        return new ConcatenateFunction(elements);
    }

    /**
     * AVG is an aggregate function that evaluates the average of an expression over a set of rows. AVG is allowed only
     * on expressions that evaluate to numeric data types.
     *
     * @param value The value to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction avg(Object value)
    {
        return new SqlFunction("avg", value);
    }

    /**
     * COUNT is an aggregate function that counts the number of rows accessed in an expression.
     *
     * <p>
     * This will create the COUNT function with * as parameter.
     * </p>
     *
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction count()
    {
        return new SqlFunction("count", "*");
    }

    /**
     * COUNT is an aggregate function that counts the number of rows accessed in an expression.
     *
     * <p>
     * This will create the COUNT function with the given column as parameter.
     * </p>
     *
     * @param value The value to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction count(Object value)
    {
        return new SqlFunction("count", value);
    }

    /**
     * MAX is an aggregate function that evaluates the maximum of an expression over a set of rows. MAX is allowed only
     * on expressions that evaluate to built-in data types (including CHAR, VARCHAR, DATE, TIME, CHAR FOR BIT DATA,
     * etc.).
     *
     * @param value The value to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction max(Object value)
    {
        return new SqlFunction("max", value);
    }

    /**
     * MIN is an aggregate function that evaluates the minimum of an expression over a set of rows. MIN is allowed only
     * on expressions that evaluate to built-in data types (including CHAR, VARCHAR, DATE, TIME, etc.).
     *
     * @param value The value to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction min(Object value)
    {
        return new SqlFunction("min", value);
    }

    /**
     * SUM is an aggregate function that evaluates the sum of the expression over a set of rows. SUM is allowed only on
     * expressions that evaluate to numeric data types.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction sum(Object value)
    {
        return new SqlFunction("sum", value);
    }

    /**
     * ABS returns the absolute value of a numeric expression.
     *
     * <p>
     * The given value will be used.
     * </p>
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction abs(Object value)
    {
        return new SqlFunction("abs", value);
    }

    /**
     * The RANDOM function returns a DOUBLE PRECISION number with positive sign, greater than or equal to zero (0), and
     * less than one (1.0).
     *
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction random()
    {
        return new SqlFunction("random");
    }

    /**
     * The RAND function returns a DOUBLE PRECISION number with positive sign, greater than or equal to zero (0), and
     * less than one (1.0), given an INTEGER seed number.
     *
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction random(int seed)
    {
        return new SqlFunction("rand", seed);
    }

    /**
     * The CEIL or CEILING function rounds the specified number up, and returns the smallest number that is greater than
     * or equal to the specified number.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction ceil(Object value)
    {
        return new SqlFunction("ceil", value);
    }

    /**
     * The DATE function returns a date from a value.
     *
     * <p>
     * The argument must be a date, timestamp, a positive number less than or equal to 2,932,897, a valid string
     * representation of a date or timestamp, or a string of length 7 that is not a CLOB, LONG VARCHAR, or XML value. If
     * the argument is a string of length 7, it must represent a valid date in the form yyyynnn, where yyyy are digits
     * denoting a year, and nnn are digits between 001 and 366, denoting a day of that year. The result of the function
     * is a date. If the argument can be null, the result can be null; if the argument is null, the result is the null
     * value.
     * </p>
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction date(Object value)
    {
        return new SqlFunction("date", value);
    }

    public static SqlFunction toDate(Object value)
    {
        return new SqlFunction("toDate", value);
    }

    /**
     * The TIME function returns a time from a value.
     *
     * <p>
     * The argument must be a time, timestamp, or a valid string representation of a time or timestamp that is not a
     * CLOB, LONG VARCHAR, or XML value. The result of the function is a time. If the argument can be null, the result
     * can be null; if the argument is null, the result is the null value.
     * </p>
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction time(Object value)
    {
        return new SqlFunction("time", value);
    }

    /**
     * The DAY function returns the day part of a value.
     *
     * <p>
     * The argument must be a date, timestamp, or a valid character string representation of a date or timestamp that is
     * not a CLOB, LONG VARCHAR, or XML value. The result of the function is an integer between 1 and 31. If the
     * argument can be null, the result can be null; if the argument is null, the result is the null value.
     * </p>
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction day(Object value)
    {
        return new SqlFunction("day", value);
    }

    /**
     * The HOUR function returns the hour part of a value.
     *
     * <p>
     * The argument must be a time, timestamp, or a valid character string representation of a time or timestamp that is
     * not a CLOB, LONG VARCHAR, or XML value. The result of the function is an integer between 0 and 24. If the
     * argument can be null, the result can be null; if the argument is null, the result is the null value.
     * </p>
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction hour(Object value)
    {
        return new SqlFunction("hour", value);
    }

    /**
     * The MINUTE function returns the minute part of a value.
     *
     * <p>
     * The argument must be a time, timestamp, or a valid character string representation of a time or timestamp that is
     * not a CLOB, LONG VARCHAR, or XML value. The result of the function is an integer between 0 and 59. If the
     * argument can be null, the result can be null; if the argument is null, the result is the null value.
     * </p>
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction minute(Object value)
    {
        return new SqlFunction("minute", value);
    }

    /**
     * The SECOND function returns the seconds part of a value.
     *
     * <p>
     * The argument must be a time, timestamp, or a valid character string representation of a time or timestamp that is
     * not a CLOB, LONG VARCHAR, or XML value. The result of the function is an integer between 0 and 59. If the
     * argument can be null, the result can be null. If the argument is null, the result is 0.
     * </p>
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction second(Object value)
    {
        return new SqlFunction("second", value);
    }

    /**
     * The YEAR function returns the year part of a value.
     *
     * <p>
     * The argument must be a date, timestamp, or a valid character string representation of a date or timestamp. The
     * result of the function is an integer between 1 and 9,999. If the argument can be null, the result can be null; if
     * the argument is null, the result is the null value.
     * </p>
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction year(Object value)
    {
        return new SqlFunction("year", value);
    }

    /**
     * The MONTH function returns the month part of a value.
     *
     * <p>
     * The argument must be a date, timestamp, or a valid character string representation of a date or timestamp that is
     * not a CLOB, LONG VARCHAR, or XML value. The result of the function is an integer between 1 and 12. If the
     * argument can be null, the result can be null; if the argument is null, the result is the null value.
     * </p>
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction month(Object value)
    {
        return new SqlFunction("month", value);
    }

    /**
     * The FLOOR function rounds the specified number down, and returns the largest number that is less than or equal to
     * the specified number.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction floor(Object value)
    {
        return new SqlFunction("floor", value);
    }

    /**
     * The LCASE or LOWER function takes a character expression as a parameter and returns a string in which all
     * alphabetical characters have been converted to lowercase.
     *
     * @param value The column to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction lower(Object value)
    {
        return new SqlFunction("lower", value);
    }

    /**
     * The UCASE or UPPER function takes a character expression as a parameter and returns a string in which all
     * alphabetical characters have been converted to uppercase.
     *
     * @param value The column to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction upper(Object value)
    {
        return new SqlFunction("upper", value);
    }

    /**
     * The LENGTH function is applied to either a character string expression or a bit string expression and returns the
     * number of characters in the result.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction length(Object value)
    {
        return new SqlFunction("length", value);
    }

    /**
     * The LTRIM function removes blanks from the beginning of a character string expression.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction leftTrim(Object value)
    {
        return new SqlFunction("ltrim", value);
    }

    /**
     * The RTRIM function removes blanks from the end of a character string expression.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction rightTrim(Object value)
    {
        return new SqlFunction("rtrim", value);
    }

    /**
     * The ROW_NUMBER function returns the row number over a named or unnamed window specification.
     *
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction rowNumber()
    {
        return new SqlFunction("row_number() over");
    }

    /**
     * The MOD function returns the remainder (modulus) of argument 1 divided by argument 2. The result is negative only
     * if argument 1 is negative.
     *
     * @param value1 The first value to use.
     * @param value2 The second value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static ModFunction mod(Object value1, Object value2)
    {
        return new ModFunction(value1, value2);
    }

    /**
     * Converts the given decimal value to hexadecimal.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction decimalToHex(Object value)
    {
        return new SqlFunction("decimalToHex", value);
    }

    /**
     * Converts the given decimal value to octal.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction decimalToOctal(Object value)
    {
        return new SqlFunction("decimalToOctal", value);
    }

    /**
     * Converts the given decimal value to binary.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction decimalToBinary(Object value)
    {
        return new SqlFunction("decimalToBinary", value);
    }

    /**
     * Converts the given hexadecimal value to decimal.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction hexToDecimal(Object value)
    {
        return new SqlFunction("hexToDecimal", value);
    }

    /**
     * Converts the given hexadecimal value to octal.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction hexToOctal(Object value)
    {
        return new SqlFunction("hexToOctal", value);
    }

    /**
     * Converts the given hexadecimal value to binary.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction hexToBinary(Object value)
    {
        return new SqlFunction("hexToBinary", value);
    }

    /**
     * Converts the given binary value to decimal.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction binaryToDecimal(Object value)
    {
        return new SqlFunction("binaryToDecimal", value);
    }

    /**
     * Converts the given binary value to octal.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction binaryToOctal(Object value)
    {
        return new SqlFunction("binaryToOctal", value);
    }

    /**
     * Converts the given binary value to hexadecimal.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction binaryToHex(Object value)
    {
        return new SqlFunction("binaryToHex", value);
    }

    /**
     * Converts the given octal value to decimal.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction octalToDecimal(Object value)
    {
        return new SqlFunction("octalToDecimal", value);
    }

    /**
     * Converts the given octal value to binary.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction octalToBinary(Object value)
    {
        return new SqlFunction("octalToBinary", value);
    }

    /**
     * Converts the given octal value to hexadecimal.
     *
     * @param value The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction octalToHex(Object value)
    {
        return new SqlFunction("octalToHex", value);
    }

    /**
     * Pads the given String value on the left side with the given padding String until the given length is reached.
     *
     * @param value  The value to use.
     * @param length The length of the resulting String.
     * @param The    character that is used for padding.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static LeftPadFunction lpad(Object value, int length, String pad)
    {
        return new LeftPadFunction(value, length, pad);
    }

    /**
     * Pads the given String value on the right side with the given padding String until the given length is reached.
     *
     * @param value  The value to use.
     * @param length The length of the resulting String.
     * @param The    character that is used for padding.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static RightPadFunction rpad(Object value, int length, String pad)
    {
        return new RightPadFunction(value, length, pad);
    }

    /**
     * The NVL (COALESCE) function takes two or more compatible arguments and returns the first argument that is not
     * null.
     *
     * <p>
     * The result is null only if all the arguments are null.
     * </p>
     *
     * @param value1   The first value to check.
     * @param value2   The second value to check.
     * @param elements Additional values.
     * @return The function whichs toString method will return a valid sql representation of this action.
     */
    public static NullValueFunction nvl(Object value1, Object value2, Object... elements)
    {
        return new NullValueFunction(value1, value2, elements);
    }

    /**
     * Adds the given number of days to the value.
     *
     * @param value  The value to use.
     * @param length The number of days to add.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction addDays(Object value, int days)
    {
        var func = new SqlFunction("addDays", value);
        func.add(days);
        return func;
    }

    /**
     * Adds the given number of hours to the value.
     *
     * @param value  The value to use.
     * @param length The number of hours to add.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction addHours(Object value, int hours)
    {
        var func = new SqlFunction("addHours", value);
        func.add(hours);
        return func;
    }

    /**
     * Adds the given number of minutes to the value.
     *
     * @param value  The value to use.
     * @param length The number of minutes to add.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction addMinutes(Object value, int mins)
    {
        var func = new SqlFunction("addMinutes", value);
        func.add(mins);
        return func;
    }

    /**
     * Adds the given number of days to the value.
     *
     * @param value  The value to use.
     * @param length The number of days to add.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SqlFunction addSeconds(Object value, int secs)
    {
        var func = new SqlFunction("addSeconds", value);
        func.add(secs);
        return func;
    }
}