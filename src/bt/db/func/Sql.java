package bt.db.func;

import bt.db.func.impl.AbsoluteFunction;
import bt.db.func.impl.AverageFunction;
import bt.db.func.impl.ConcatenateFunction;
import bt.db.func.impl.CountFunction;
import bt.db.func.impl.MaxFunction;
import bt.db.func.impl.MinFunction;
import bt.db.func.impl.RandomFunction;
import bt.db.func.impl.SumFunction;
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
     * @param column
     *            The column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static AverageFunction avg(ColumnEntry column)
    {
        return new AverageFunction(column);
    }

    /**
     * AVG is an aggregate function that evaluates the average of an expression over a set of rows. AVG is allowed only
     * on expressions that evaluate to numeric data types.
     * 
     * @param column
     *            The name of the column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static AverageFunction avg(String column)
    {
        return new AverageFunction(new ColumnEntry(column));
    }

    /**
     * AVG is an aggregate function that evaluates the average of an expression over a set of rows. AVG is allowed only
     * on expressions that evaluate to numeric data types.
     * 
     * @param table
     *            The name of the table to use as a parameter for this function.
     * @param column
     *            The name of the column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static AverageFunction avg(String table, String column)
    {
        return new AverageFunction(new ColumnEntry(table,
                                                   column));
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
     * @param column
     *            The column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static CountFunction count(ColumnEntry column)
    {
        return new CountFunction(column);
    }

    /**
     * COUNT is an aggregate function that counts the number of rows accessed in an expression.
     * 
     * <p>
     * This will create the COUNT function with the given column as parameter.
     * </p>
     * 
     * @param column
     *            The name of the column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static CountFunction count(String column)
    {
        return new CountFunction(new ColumnEntry(column));
    }

    /**
     * COUNT is an aggregate function that counts the number of rows accessed in an expression.
     * 
     * <p>
     * This will create the COUNT function with the given table.column combination as parameter.
     * </p>
     * 
     * @param table
     *            The name of the table to use as a parameter for this function.
     * @param column
     *            The name of the column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static CountFunction count(String table, String column)
    {
        return new CountFunction(new ColumnEntry(table,
                                                 column));
    }

    /**
     * MAX is an aggregate function that evaluates the maximum of an expression over a set of rows. MAX is allowed only
     * on expressions that evaluate to built-in data types (including CHAR, VARCHAR, DATE, TIME, CHAR FOR BIT DATA,
     * etc.).
     * 
     * @param column
     *            The column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static MaxFunction max(ColumnEntry column)
    {
        return new MaxFunction(column);
    }

    /**
     * MAX is an aggregate function that evaluates the maximum of an expression over a set of rows. MAX is allowed only
     * on expressions that evaluate to built-in data types (including CHAR, VARCHAR, DATE, TIME, CHAR FOR BIT DATA,
     * etc.).
     * 
     * @param column
     *            The name of the column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static MaxFunction max(String column)
    {
        return new MaxFunction(new ColumnEntry(column));
    }

    /**
     * MAX is an aggregate function that evaluates the maximum of an expression over a set of rows. MAX is allowed only
     * on expressions that evaluate to built-in data types (including CHAR, VARCHAR, DATE, TIME, CHAR FOR BIT DATA,
     * etc.).
     * 
     * @param table
     *            The name of the table to use as a parameter for this function.
     * @param column
     *            The name of the column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static MaxFunction max(String table, String column)
    {
        return new MaxFunction(new ColumnEntry(table,
                                               column));
    }

    /**
     * MIN is an aggregate function that evaluates the minimum of an expression over a set of rows. MIN is allowed only
     * on expressions that evaluate to built-in data types (including CHAR, VARCHAR, DATE, TIME, etc.).
     * 
     * @param column
     *            The column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static MinFunction min(ColumnEntry column)
    {
        return new MinFunction(column);
    }

    /**
     * MIN is an aggregate function that evaluates the minimum of an expression over a set of rows. MIN is allowed only
     * on expressions that evaluate to built-in data types (including CHAR, VARCHAR, DATE, TIME, etc.).
     * 
     * @param column
     *            The name of the column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static MinFunction min(String column)
    {
        return new MinFunction(new ColumnEntry(column));
    }

    /**
     * MIN is an aggregate function that evaluates the minimum of an expression over a set of rows. MIN is allowed only
     * on expressions that evaluate to built-in data types (including CHAR, VARCHAR, DATE, TIME, etc.).
     * 
     * @param table
     *            The name of the table to use as a parameter for this function.
     * @param column
     *            The name of the column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static MinFunction min(String table, String column)
    {
        return new MinFunction(new ColumnEntry(table,
                                               column));
    }

    /**
     * SUM is an aggregate function that evaluates the sum of the expression over a set of rows. SUM is allowed only on
     * expressions that evaluate to numeric data types.
     * 
     * @param column
     *            The column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SumFunction sum(ColumnEntry column)
    {
        return new SumFunction(column);
    }

    /**
     * SUM is an aggregate function that evaluates the sum of the expression over a set of rows. SUM is allowed only on
     * expressions that evaluate to numeric data types.
     * 
     * @param column
     *            The name of the column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SumFunction sum(String column)
    {
        return new SumFunction(new ColumnEntry(column));
    }

    /**
     * SUM is an aggregate function that evaluates the sum of the expression over a set of rows. SUM is allowed only on
     * expressions that evaluate to numeric data types.
     * 
     * @param table
     *            The name of the table to use as a parameter for this function.
     * @param column
     *            The name of the column to use as parameter for this function.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static SumFunction sum(String table, String column)
    {
        return new SumFunction(new ColumnEntry(table,
                                               column));
    }

    /**
     * ABS returns the absolute value of a numeric expression.
     * 
     * <p>
     * The value of the given table.column combination will be used.
     * </p>
     * 
     * @param table
     *            The name of the table.
     * @param column
     *            The name of the column.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static AbsoluteFunction abs(String table, String column)
    {
        return new AbsoluteFunction(new ColumnEntry(table,
                                                    column));
    }

    /**
     * ABS returns the absolute value of a numeric expression.
     * 
     * <p>
     * The value of the given column will be used.
     * </p>
     * 
     * @param column
     *            The name of the column.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static AbsoluteFunction abs(String column)
    {
        return new AbsoluteFunction(new ColumnEntry(column));
    }

    /**
     * ABS returns the absolute value of a numeric expression.
     * 
     * <p>
     * The value of the given column will be used.
     * </p>
     * 
     * @param value
     *            The column which value should be used in the statement.
     * @return The function whichs toString will return a valid sql representation of this action.
     */
    public static AbsoluteFunction abs(ColumnEntry value)
    {
        return new AbsoluteFunction(value);
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
    public static AbsoluteFunction abs(int value)
    {
        return new AbsoluteFunction(value);
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
    public static AbsoluteFunction abs(long value)
    {
        return new AbsoluteFunction(value);
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
    public static AbsoluteFunction abs(double value)
    {
        return new AbsoluteFunction(value);
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
    public static AbsoluteFunction abs(float value)
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
}