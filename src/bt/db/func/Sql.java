package bt.db.func;

import bt.db.func.impl.AbsoluteFunction;
import bt.db.func.impl.AverageFunction;
import bt.db.func.impl.CeilFunction;
import bt.db.func.impl.ConcatenateFunction;
import bt.db.func.impl.CountFunction;
import bt.db.func.impl.DateFunction;
import bt.db.func.impl.DayFunction;
import bt.db.func.impl.FloorFunction;
import bt.db.func.impl.HourFunction;
import bt.db.func.impl.LeftTrimFunction;
import bt.db.func.impl.LengthFunction;
import bt.db.func.impl.LowerFunction;
import bt.db.func.impl.MaxFunction;
import bt.db.func.impl.MinFunction;
import bt.db.func.impl.MinuteFunction;
import bt.db.func.impl.ModFunction;
import bt.db.func.impl.MonthFunction;
import bt.db.func.impl.RandomFunction;
import bt.db.func.impl.RightTrimFunction;
import bt.db.func.impl.RowNumberFunction;
import bt.db.func.impl.SecondFunction;
import bt.db.func.impl.SumFunction;
import bt.db.func.impl.TimeFunction;
import bt.db.func.impl.UpperFunction;
import bt.db.func.impl.YearFunction;
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
     * @param table
     *            The name of the table.
     * @param column
     *            The name of the column.
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
     * @param column
     *            The name of the column.
     * @return The ColumnEntry instance.
     */
    public static ColumnEntry column(String column)
    {
        return new ColumnEntry(column);
    }

    /**
     * Concatenates the toString() representations of the given objects.
     *
     * @param elements
     *            The objects to concatenate.
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
     * @param value
     *            The value to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static AverageFunction avg(Object value)
    {
        return new AverageFunction(value);
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
    public static CountFunction count()
    {
        return new CountFunction(new ColumnEntry("*"));
    }

    /**
     * COUNT is an aggregate function that counts the number of rows accessed in an expression.
     *
     * <p>
     * This will create the COUNT function with the given column as parameter.
     * </p>
     *
     * @param value
     *            The value to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static CountFunction count(Object value)
    {
        return new CountFunction(value);
    }

    /**
     * MAX is an aggregate function that evaluates the maximum of an expression over a set of rows. MAX is allowed only
     * on expressions that evaluate to built-in data types (including CHAR, VARCHAR, DATE, TIME, CHAR FOR BIT DATA,
     * etc.).
     *
     * @param value
     *            The value to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static MaxFunction max(Object value)
    {
        return new MaxFunction(value);
    }

    /**
     * MIN is an aggregate function that evaluates the minimum of an expression over a set of rows. MIN is allowed only
     * on expressions that evaluate to built-in data types (including CHAR, VARCHAR, DATE, TIME, etc.).
     *
     * @param value
     *            The value to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static MinFunction min(Object value)
    {
        return new MinFunction(value);
    }

    /**
     * SUM is an aggregate function that evaluates the sum of the expression over a set of rows. SUM is allowed only on
     * expressions that evaluate to numeric data types.
     *
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SumFunction sum(Object value)
    {
        return new SumFunction(value);
    }

    /**
     * ABS returns the absolute value of a numeric expression.
     *
     * <p>
     * The given value will be used.
     * </p>
     *
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static AbsoluteFunction abs(Object value)
    {
        return new AbsoluteFunction(value);
    }

    /**
     * The RANDOM function returns a DOUBLE PRECISION number with positive sign, greater than or equal to zero (0), and
     * less than one (1.0).
     *
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static RandomFunction random()
    {
        return new RandomFunction();
    }

    /**
     * The RAND function returns a DOUBLE PRECISION number with positive sign, greater than or equal to zero (0), and
     * less than one (1.0), given an INTEGER seed number.
     *
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static RandomFunction random(int seed)
    {
        return new RandomFunction(seed);
    }

    /**
     * The CEIL or CEILING function rounds the specified number up, and returns the smallest number that is greater than
     * or equal to the specified number.
     *
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public CeilFunction ceil(Object value)
    {
        return new CeilFunction(value);
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
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public DateFunction date(Object value)
    {
        return new DateFunction(value);
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
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public TimeFunction time(Object value)
    {
        return new TimeFunction(value);
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
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public DayFunction day(Object value)
    {
        return new DayFunction(value);
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
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public HourFunction hour(Object value)
    {
        return new HourFunction(value);
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
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public MinuteFunction minute(Object value)
    {
        return new MinuteFunction(value);
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
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public SecondFunction second(Object value)
    {
        return new SecondFunction(value);
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
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public YearFunction year(Object value)
    {
        return new YearFunction(value);
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
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public MonthFunction month(Object value)
    {
        return new MonthFunction(value);
    }

    /**
     * The FLOOR function rounds the specified number down, and returns the largest number that is less than or equal to
     * the specified number.
     *
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public FloorFunction floor(Object value)
    {
        return new FloorFunction(value);
    }

    /**
     * The LCASE or LOWER function takes a character expression as a parameter and returns a string in which all
     * alphabetical characters have been converted to lowercase.
     *
     * @param value
     *            The column to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public LowerFunction lower(Object value)
    {
        return new LowerFunction(value);
    }

    /**
     * The UCASE or UPPER function takes a character expression as a parameter and returns a string in which all
     * alphabetical characters have been converted to uppercase.
     *
     * @param value
     *            The column to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public UpperFunction upper(Object value)
    {
        return new UpperFunction(value);
    }

    /**
     * The LENGTH function is applied to either a character string expression or a bit string expression and returns the
     * number of characters in the result.
     *
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public LengthFunction length(Object value)
    {
        return new LengthFunction(value);
    }

    /**
     * The LTRIM function removes blanks from the beginning of a character string expression.
     *
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public LeftTrimFunction leftTrim(Object value)
    {
        return new LeftTrimFunction(value);
    }

    /**
     * The RTRIM function removes blanks from the end of a character string expression.
     *
     * @param value
     *            The value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public RightTrimFunction rightTrim(Object value)
    {
        return new RightTrimFunction(value);
    }

    /**
     * The ROW_NUMBER function returns the row number over a named or unnamed window specification.
     *
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public RowNumberFunction rowNumber()
    {
        return new RowNumberFunction();
    }

    /**
     * The MOD function returns the remainder (modulus) of argument 1 divided by argument 2. The result is negative only
     * if argument 1 is negative.
     *
     * @param value1
     *            The first value to use.
     * @param value2
     *            The second value to use.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public ModFunction mod(Object value1, Object value2)
    {
        return new ModFunction(value1, value2);
    }
}