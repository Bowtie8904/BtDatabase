package bt.db.statement.result;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import bt.console.ConsoleTable;
import bt.db.DatabaseAccess;
import bt.db.constants.SqlType;
import bt.log.Logger;

/**
 * Wraps the values from the given ResultSet to make them more accessible and serializable.
 *
 * @author &#8904
 */
public class SqlResultSet implements Iterable<SqlResult>
{
    private List<SqlResult> results;
    private List<String> colOrder;
    private String sql;
    private List<String> values;
    private Map<String, String> valueTypes;
    private int[] defaultFormat;

    /**
     * Creates a new instance.
     *
     * <p>
     * {@link #parse(ResultSet)} needs to be called before this instance is usable.
     * </p>
     *
     * @param columnOrder
     *            A list containing the names of the columns in correct order.
     */
    public SqlResultSet(List<String> columnOrder)
    {
        this.colOrder = columnOrder;
        this.results = new ArrayList<>();
        this.values = new ArrayList<>();
        this.valueTypes = new HashMap<>();

        this.defaultFormat = new int[this.colOrder.size()];

        for (int i = 0; i < this.defaultFormat.length; i ++ )
        {
            this.defaultFormat[i] = 25;
        }
    }

    /**
     * Creates a new instance.
     *
     * <p>
     * {@link #parse(ResultSet)} needs to be called before this instance is usable.
     * </p>
     *
     * @param columnOrder
     *            An array containing the names of the columns in correct order.
     */
    public SqlResultSet(String... columnOrder)
    {
        this(List.of(columnOrder));
    }

    /**
     * Creates a new instance and parses the given ResultSet.
     *
     * @param set
     *            The ResultSet to use.
     */
    public SqlResultSet(ResultSet set)
    {
        this.colOrder = new ArrayList<>();
        this.results = new ArrayList<>();
        this.values = new ArrayList<>();
        this.valueTypes = new HashMap<>();

        try
        {
            parse(set);
        }
        catch (SQLException e)
        {
            DatabaseAccess.log.print(this,
                                     e);
        }
    }

    /**
     * Sets the {@link SqlResult results} that should be wrapped by this instance.
     *
     * @param results
     *            A list containing all results that this instance should wrap.
     */
    public void setResults(List<SqlResult> results)
    {
        for (SqlResult result : results)
        {
            if (this.colOrder.size() != result.getColumnNames().size())
            {
                throw new IllegalArgumentException("All SqlResults must have the same number of columns.");
            }
        }

        this.results = results;
    }

    /**
     * Returns a list containing pairs of column name : column type.
     *
     * <p>
     * The type will be the sql type String representation.
     * </p>
     *
     * @return The list.
     */
    public List<Entry<String, String>> getColumnTypes()
    {
        List<Entry<String, String>> columns = new ArrayList<>();

        for (String col : this.colOrder)
        {
            columns.add(new SimpleEntry(col,
                                        this.valueTypes.get(col)));
        }

        return columns;
    }

    /**
     * Sets the sql that was used to get this result.
     *
     * @param sql
     *            The sql statement.
     */
    public void setSql(String sql)
    {
        this.sql = sql;
    }

    /**
     * Gets the sql that was used to get this result.
     *
     * @return The sql statement.
     */
    public String getSql()
    {
        return this.sql;
    }

    /**
     * Sets a list of used values for the prepared statement that resulted in this set.
     *
     * @param values
     *            The list of values.
     */
    public void setValues(List<String> values)
    {
        this.values = values;
    }

    /**
     * Gets a list of used values for the prepared statement that resulted in this set.
     */
    public List<String> getValues()
    {
        return this.values;
    }

    /**
     * Gets the size of this set. This is equal to the amount of results contained.
     *
     * @return The size of the set.
     */
    public int size()
    {
        return this.results.size();
    }

    /**
     * Gets the result at the given index.
     *
     * @param index
     *            The index of the desired result.
     * @return The requested result.
     */
    public SqlResult get(int index)
    {
        return this.results.get(index);
    }

    /**
     * Gets a list of Strings representing the name of the columns in left to right order.
     *
     * @return
     */
    public List<String> getColumnOrder()
    {
        return this.colOrder;
    }

    /**
     * Parses the given ResultSet.
     *
     * @param set
     *            The ResultSet whichs values should be
     * @return
     * @throws SQLException
     */
    public SqlResultSet parse(ResultSet set) throws SQLException
    {
        List<SqlResult> results = new ArrayList<>();

        List<String> stringResults = new ArrayList<>();
        List<String> byteResults = new ArrayList<>();
        List<String> shortResults = new ArrayList<>();
        List<String> intResults = new ArrayList<>();
        List<String> longResults = new ArrayList<>();
        List<String> doubleResults = new ArrayList<>();
        List<String> floatResults = new ArrayList<>();
        List<String> dateResults = new ArrayList<>();
        List<String> timeResults = new ArrayList<>();
        List<String> timestampResults = new ArrayList<>();
        List<String> booleanResults = new ArrayList<>();
        List<String> clobResults = new ArrayList<>();
        List<String> blobResults = new ArrayList<>();
        List<String> objectResults = new ArrayList<>();

        ResultSetMetaData meta = set.getMetaData();
        int count = meta.getColumnCount();
        this.defaultFormat = new int[count];

        for (int i = 1; i <= count; i ++ )
        {
            String columnName = meta.getColumnName(i).toUpperCase();
            int valueType = meta.getColumnType(i);
            this.colOrder.add(columnName);
            this.valueTypes.put(columnName,
                                SqlType.convert(valueType).toString());
            int width = meta.getColumnDisplaySize(i) + 4 > 50 ? 50 : meta.getColumnDisplaySize(i) + 4;

            if (width < columnName.length() + 2)
            {
                width = columnName.length() + 2;
            }

            if (width < 10)
            {
                width = 10;
            }

            this.defaultFormat[i - 1] = width;

            switch (valueType)
            {
                case Types.BOOLEAN:
                    booleanResults.add(columnName);
                    break;
                case Types.CHAR:
                    stringResults.add(columnName);
                    break;
                case Types.VARCHAR:
                    stringResults.add(columnName);
                    break;
                case Types.BIT:
                    booleanResults.add(columnName);
                    break;
                case Types.TINYINT:
                    byteResults.add(columnName);
                    break;
                case Types.SMALLINT:
                    shortResults.add(columnName);
                    break;
                case Types.INTEGER:
                    intResults.add(columnName);
                    break;
                case Types.BIGINT:
                    longResults.add(columnName);
                    break;
                case Types.REAL:
                    floatResults.add(columnName);
                    break;
                case Types.FLOAT:
                    doubleResults.add(columnName);
                    break;
                case Types.DOUBLE:
                    doubleResults.add(columnName);
                    break;
                case Types.DATE:
                    dateResults.add(columnName);
                    break;
                case Types.TIME:
                    timeResults.add(columnName);
                    break;
                case Types.TIMESTAMP:
                    timestampResults.add(columnName);
                    break;
                case Types.CLOB:
                    clobResults.add(columnName);
                    break;
                case Types.BLOB:
                    blobResults.add(columnName);
                    break;
                case Types.JAVA_OBJECT:
                    objectResults.add(columnName);
                    break;
                default:
                    objectResults.add(columnName);
            }
        }

        while (set.next())
        {
            SqlResult result = new SqlResult(this.colOrder);

            for (String name : stringResults)
            {
                result.put(name,
                           set.getString(name));
            }

            for (String name : byteResults)
            {
                result.put(name,
                           set.getByte(name));
            }

            for (String name : shortResults)
            {
                result.put(name,
                           set.getShort(name));
            }

            for (String name : intResults)
            {
                result.put(name,
                           set.getInt(name));
            }

            for (String name : longResults)
            {
                result.put(name,
                           set.getLong(name));
            }

            for (String name : floatResults)
            {
                result.put(name,
                           set.getFloat(name));
            }

            for (String name : doubleResults)
            {
                result.put(name,
                           set.getDouble(name));
            }

            for (String name : dateResults)
            {
                result.put(name,
                           set.getDate(name));
            }

            for (String name : timeResults)
            {
                result.put(name,
                           set.getTime(name));
            }

            for (String name : timestampResults)
            {
                result.put(name,
                           set.getTimestamp(name));
            }

            for (String name : booleanResults)
            {
                result.put(name,
                           set.getBoolean(name));
            }

            for (String name : clobResults)
            {
                try
                {
                    result.put(name,
                               set.getClob(name));
                }
                catch (Exception e)
                {

                }
            }

            for (String name : blobResults)
            {
                try
                {
                    result.put(name,
                               set.getBlob(name));
                }
                catch (Exception e)
                {

                }
            }

            for (String name : objectResults)
            {
                result.putObject(name,
                                 set.getObject(name));
            }

            result.setColumnTypes(this.valueTypes);

            results.add(result);
        }

        this.results = results;

        return this;
    }

    public int[] getColumnSizes()
    {
        return this.defaultFormat;
    }

    /**
     * Prints a formatted table of the result.
     *
     * <p>
     * The given values define the widths of the columns.
     * </p>
     *
     * @param columnFormat
     *            If only one number is given, all columns will have the same width. If more than one value is given,
     *            there needs to be the same amount of numbers as there is columns.
     */
    public SqlResultSet print(int... columnFormat)
    {
        if (columnFormat == null)
        {
            System.out.println(toString(this.defaultFormat));
        }
        else
        {
            System.out.println(toString(columnFormat));
        }
        return this;
    }

    /**
     * Prints a formatted table of the result.
     */
    public SqlResultSet print()
    {
        System.out.println(toString(this.defaultFormat));
        return this;
    }

    public SqlResultSet print(Logger log)
    {
        log.print("\n" + toString(this.defaultFormat));
        return this;
    }

    /**
     * Formats a table of the results.
     *
     * <p>
     * The given values define the widths of the columns.
     * </p>
     *
     * @param columnFormat
     *            If only one number is given, all columns will have the same width. If more than one value is given,
     *            there needs to be the same amount of numbers as there is columns.
     * @return The formatted table.
     */
    public String toString(int... columnFormat)
    {
        ConsoleTable consoleRows;

        if (columnFormat.length == 1)
        {
            int[] format = new int[this.colOrder.size()];

            for (int i = 0; i < format.length; i ++ )
            {
                format[i] = columnFormat[0];
            }

            consoleRows = new ConsoleTable(format);
        }
        else
        {
            consoleRows = new ConsoleTable(columnFormat);
        }

        consoleRows.setCenteredTitle(this.colOrder.toArray(new Object[] {}));

        for (SqlResult result : this.results)
        {
            consoleRows.addCenteredRow(result.getValueArray());
        }

        return consoleRows.toString();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return toString(this.defaultFormat);
    }

    /**
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<SqlResult> iterator()
    {
        return this.results.iterator();
    }
}