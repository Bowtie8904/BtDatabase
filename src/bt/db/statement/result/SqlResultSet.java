package bt.db.statement.result;

import bt.console.output.table.ConsoleTable;
import bt.console.output.table.render.Alignment;
import bt.db.constants.SqlType;
import bt.log.Log;
import bt.reflect.classes.Classes;
import bt.utils.Exceptions;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;

/**
 * Wraps the values from the given ResultSet to make them more accessible and serializable.
 *
 * @author &#8904
 */
public class SqlResultSet implements Iterable<SqlResult>, Serializable
{
    private List<SqlResult> results;
    private List<String> colOrder;
    private String sql;
    private List<String> values;
    private Map<String, String> valueTypes;

    /**
     * Creates a new instance.
     *
     * <p>
     * {@link #parse(ResultSet)} needs to be called before this instance is usable.
     * </p>
     *
     * @param columnOrder A list containing the names of the columns in correct order.
     */
    public SqlResultSet(List<String> columnOrder)
    {
        this.colOrder = columnOrder;
        this.results = new ArrayList<>();
        this.values = new ArrayList<>();
        this.valueTypes = new HashMap<>();
    }

    /**
     * Creates a new instance.
     *
     * <p>
     * {@link #parse(ResultSet)} needs to be called before this instance is usable.
     * </p>
     *
     * @param columnOrder An array containing the names of the columns in correct order.
     */
    public SqlResultSet(String... columnOrder)
    {
        this(List.of(columnOrder));
    }

    /**
     * Creates a new instance and parses the given ResultSet.
     *
     * @param set The ResultSet to use.
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
            Log.error("Failed to parse ResultSet", e);
        }
    }

    /**
     * Sets the {@link SqlResult results} that should be wrapped by this instance.
     *
     * @param results A list containing all results that this instance should wrap.
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
     * @param sql The sql statement.
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
     * @param values The list of values.
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
     * @param index The index of the desired result.
     *
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
     * @param set The ResultSet whichs values should be
     *
     * @return
     *
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

        for (int i = 1; i <= count; i++)
        {
            String columnName = meta.getColumnName(i).toUpperCase();
            int valueType = meta.getColumnType(i);
            this.colOrder.add(columnName);
            this.valueTypes.put(columnName,
                                SqlType.convert(valueType).toString());

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
                result.put(name, set.getString(name));
            }

            for (String name : byteResults)
            {
                Byte val = set.getByte(name);

                if (set.wasNull())
                {
                    val = null;
                }

                result.put(name, val);
            }

            for (String name : shortResults)
            {
                Short val = set.getShort(name);

                if (set.wasNull())
                {
                    val = null;
                }

                result.put(name, val);
            }

            for (String name : intResults)
            {
                Integer val = set.getInt(name);

                if (set.wasNull())
                {
                    val = null;
                }

                result.put(name, val);
            }

            for (String name : longResults)
            {
                Long val = set.getLong(name);

                if (set.wasNull())
                {
                    val = null;
                }

                result.put(name, val);
            }

            for (String name : floatResults)
            {
                Float val = set.getFloat(name);

                if (set.wasNull())
                {
                    val = null;
                }

                result.put(name, val);
            }

            for (String name : doubleResults)
            {
                Double val = set.getDouble(name);

                if (set.wasNull())
                {
                    val = null;
                }

                result.put(name, val);
            }

            for (String name : dateResults)
            {
                result.put(name, set.getDate(name));
            }

            for (String name : timeResults)
            {
                result.put(name, set.getTime(name));
            }

            for (String name : timestampResults)
            {
                result.put(name, set.getTimestamp(name));
            }

            for (String name : booleanResults)
            {
                Boolean val = set.getBoolean(name);

                if (set.wasNull())
                {
                    val = null;
                }

                result.put(name, val);
            }

            for (String name : clobResults)
            {
                Exceptions.ignoreThrow(() -> result.put(name, set.getClob(name)));
            }

            for (String name : blobResults)
            {
                Exceptions.ignoreThrow(() -> result.put(name, set.getBlob(name)));
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
        set.close();

        return this;
    }

    /**
     * This method applies each row of this resultset to the given function.
     *
     * <p>
     * The return values of each function call will be added to a list and returned as such.
     * </p>
     *
     * @param mapFunction
     * @param <T>
     *
     * @return The list of function results.
     */
    public <T> List<T> map(Function<SqlResult, T> mapFunction)
    {
        List<T> list = new ArrayList<>(size());

        for (var result : this.results)
        {
            list.add(mapFunction.apply(result));
        }

        return list;
    }

    /**
     * Attempts to create a new instance of the given class for each row in this resultset.
     * This method will then use the {@link SqlResult#applyValues(Object)} method to apply
     * the values from the resultset to the instances fields.
     *
     * <p>
     * Note that only works with
     * <ul>
     *     <li>normal classes</li>
     *     <li>anonymous classes in a static context</li>
     *     <li>static nested classes</li>
     * </ul>
     * </p>
     *
     * @param mappingClass
     * @param <T>
     *
     * @return A list containing all created instances.
     */
    public <T> List<T> map(Class<T> mappingClass)
    {
        List<T> list = new ArrayList<>(size());

        for (var result : this.results)
        {
            T obj = Classes.newInstance(mappingClass);

            if (obj != null)
            {
                list.add(obj);

                try
                {
                    result.applyValues(obj);
                }
                catch (IllegalAccessException e)
                {
                    Log.error("Failed to apply value to mapping object", e);
                }
            }
            else
            {
                throw new IllegalArgumentException("Given class offers no valid no argument constructor.");
            }
        }

        return list;
    }

    /**
     * Prints a formatted table of the result.
     */
    public SqlResultSet print()
    {
        Log.info(toString());
        return this;
    }

    public String toString(String[] separatorStyles, String[] dataStyles)
    {
        ConsoleTable table = new ConsoleTable();
        table.setMultiline(true);
        table.setDefaultHeaderStyles(dataStyles);
        table.setDefaultValueStyles(dataStyles);
        table.setSeparatorStyles(separatorStyles);

        for (String colName : this.colOrder)
        {
            var col = table.addColumn(colName);

            SqlType type = SqlType.convert(this.valueTypes.get(colName));

            if (type == SqlType.BOOLEAN)
            {
                col.setValueAlignment(Alignment.CENTER);
            }
            else if (List.of(SqlType.INTEGER, SqlType.LONG,
                             SqlType.FLOAT, SqlType.DOUBLE).contains(type))
            {
                col.setValueAlignment(Alignment.RIGHT);
            }
            else
            {
                col.setValueAlignment(Alignment.LEFT);
            }
        }

        for (SqlResult result : this.results)
        {
            table.addRow(result.getValueArray());
        }

        return table.toString();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return toString(new String[0], new String[0]);
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