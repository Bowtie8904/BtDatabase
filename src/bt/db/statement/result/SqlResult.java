package bt.db.statement.result;

import java.lang.reflect.Field;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import bt.db.constants.SqlType;
import bt.db.statement.impl.InsertStatement;
import bt.utils.refl.field.Fields;

/**
 * Represents a single result (row) of data.
 *
 * @author &#8904
 */
public class SqlResult implements Iterable<Object>
{
    private Map<String, String> stringResults = new HashMap<>();
    private Map<String, Integer> intResults = new HashMap<>();
    private Map<String, Short> shortResults = new HashMap<>();
    private Map<String, Byte> byteResults = new HashMap<>();
    private Map<String, Long> longResults = new HashMap<>();
    private Map<String, Double> doubleResults = new HashMap<>();
    private Map<String, Float> floatResults = new HashMap<>();
    private Map<String, Date> dateResults = new HashMap<>();
    private Map<String, Time> timeResults = new HashMap<>();
    private Map<String, Timestamp> timestampResults = new HashMap<>();
    private Map<String, Boolean> booleanResults = new HashMap<>();
    private Map<String, Object> objectResults = new HashMap<>();
    private Map<String, Clob> clobResults = new HashMap<>();
    private Map<String, Blob> blobResults = new HashMap<>();

    private List<String> columnOrder;
    private Map<String, String> columnTypes;

    /**
     * Creates a new instance.
     *
     * @param columnOrder
     *            A list containing the names of the columns in correct order.
     */
    public SqlResult(List<String> columnOrder)
    {
        this.columnOrder = columnOrder;
    }

    /**
     * Creates a new instance.
     *
     * @param columnOrder
     *            An array containing the names of the columns in correct order.
     */
    public SqlResult(String... columnOrder)
    {
        this.columnOrder = List.of(columnOrder);
    }

    /**
     * Applies the values of this row to the fields of the given object. The fields need to have the same
     * (case-insensitive) names as the selected columns.
     *
     * @param obj
     *            The object to apply the values to.
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public void applyValues(Object obj) throws IllegalArgumentException, IllegalAccessException
    {
        for (Field field : Fields.getAllFields(obj.getClass()))
        {
            Object value = this.objectResults.get(field.getName().toUpperCase());

            if (value != null || this.objectResults.containsKey(field.getName().toUpperCase()))
            {
                field.setAccessible(true);
                field.set(obj, value);
            }
        }
    }

    /**
     * Sets the values and links them to the columns.
     *
     * <p>
     * Values are added to maps based on their type. Every value will also be added to the {@link Object} map,
     * regardless of type. This makes every value accessible via {@link #get(String)}.
     * </p>
     *
     * @param values
     *            An Object array containing the values in the correct order according to the column order.
     */
    public void useValues(Object... values)
    {
        if (this.columnOrder.size() != values.length)
        {
            throw new IllegalArgumentException(
                                               "Must pass the same number of values as there is columns in the result.");
        }

        for (int i = 0; i < this.columnOrder.size(); i ++ )
        {
            putObject(this.columnOrder.get(i),
                      values[i]);

            if (values[i] instanceof String)
            {
                put(this.columnOrder.get(i),
                    (String)values[i]);
            }
            else if (values[i] instanceof Integer)
            {
                put(this.columnOrder.get(i),
                    (Integer)values[i]);
            }
            else if (values[i] instanceof Short)
            {
                put(this.columnOrder.get(i),
                    (Short)values[i]);
            }
            else if (values[i] instanceof Byte)
            {
                put(this.columnOrder.get(i),
                    (Byte)values[i]);
            }
            else if (values[i] instanceof Long)
            {
                put(this.columnOrder.get(i),
                    (Long)values[i]);
            }
            else if (values[i] instanceof Double)
            {
                put(this.columnOrder.get(i),
                    (Double)values[i]);
            }
            else if (values[i] instanceof Float)
            {
                put(this.columnOrder.get(i),
                    (Float)values[i]);
            }
            else if (values[i] instanceof Date)
            {
                put(this.columnOrder.get(i),
                    (Date)values[i]);
            }
            else if (values[i] instanceof Time)
            {
                put(this.columnOrder.get(i),
                    (Time)values[i]);
            }
            else if (values[i] instanceof Timestamp)
            {
                put(this.columnOrder.get(i),
                    (Timestamp)values[i]);
            }
            else if (values[i] instanceof Boolean)
            {
                put(this.columnOrder.get(i),
                    (Boolean)values[i]);
            }
            else if (values[i] instanceof Clob)
            {
                put(this.columnOrder.get(i),
                    (Clob)values[i]);
            }
            else if (values[i] instanceof Blob)
            {
                put(this.columnOrder.get(i),
                    (Blob)values[i]);
            }
        }
    }

    /**
     * Maps the given value to the given name.
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void putObject(String name, Object value)
    {
        name = name.toUpperCase();
        this.objectResults.put(name,
                          value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, String value)
    {
        name = name.toUpperCase();
        this.stringResults.put(name,
                          value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, Time value)
    {
        name = name.toUpperCase();
        this.timeResults.put(name,
                        value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, Timestamp value)
    {
        name = name.toUpperCase();
        this.timestampResults.put(name,
                             value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, Clob value)
    {
        name = name.toUpperCase();
        this.clobResults.put(name,
                        value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, Blob value)
    {
        name = name.toUpperCase();
        this.blobResults.put(name,
                        value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, byte value)
    {
        name = name.toUpperCase();
        this.byteResults.put(name,
                        value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, short value)
    {
        name = name.toUpperCase();
        this.shortResults.put(name,
                         value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, int value)
    {
        name = name.toUpperCase();
        this.intResults.put(name,
                       value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, long value)
    {
        name = name.toUpperCase();
        this.longResults.put(name,
                        value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, double value)
    {
        name = name.toUpperCase();
        this.doubleResults.put(name,
                          value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, float value)
    {
        name = name.toUpperCase();
        this.floatResults.put(name,
                         value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, Date value)
    {
        name = name.toUpperCase();
        this.dateResults.put(name,
                        value);
        putObject(name,
                  value);
    }

    /**
     * Maps the given value to the given name.
     *
     * <p>
     * The value will also be added implicitely via {@link #putObject(String, Object)}.
     * </p>
     *
     * @param name
     *            The mapped name of the value.
     * @param value
     *            The value to be mapped.
     */
    public void put(String name, boolean value)
    {
        name = name.toUpperCase();
        this.booleanResults.put(name,
                           value);
        putObject(name,
                  value);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or null if no value of this type was mapped to the given name.
     */
    public String getString(String name)
    {
        name = name.toUpperCase();
        return this.stringResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or 0 if no value of this type was mapped to the given name.
     */
    public byte getByte(String name)
    {
        name = name.toUpperCase();
        return this.byteResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or 0 if no value of this type was mapped to the given name.
     */
    public short getShort(String name)
    {
        name = name.toUpperCase();
        return this.shortResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or 0 if no value of this type was mapped to the given name.
     */
    public int getInt(String name)
    {
        name = name.toUpperCase();
        return this.intResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or 0 if no value of this type was mapped to the given name.
     */
    public long getLong(String name)
    {
        name = name.toUpperCase();
        return this.longResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or 0 if no value of this type was mapped to the given name.
     */
    public double getDouble(String name)
    {
        name = name.toUpperCase();
        return this.doubleResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or 0 if no value of this type was mapped to the given name.
     */
    public float getFloat(String name)
    {
        name = name.toUpperCase();
        return this.floatResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or null if no value of this type was mapped to the given name.
     */
    public Date getDate(String name)
    {
        name = name.toUpperCase();
        return this.dateResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or null if no value of this type was mapped to the given name.
     */
    public Time getTime(String name)
    {
        name = name.toUpperCase();
        return this.timeResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or null if no value of this type was mapped to the given name.
     */
    public Timestamp getTimestamp(String name)
    {
        name = name.toUpperCase();
        return this.timestampResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or null if no value of this type was mapped to the given name.
     */
    public Clob getClob(String name)
    {
        name = name.toUpperCase();
        return this.clobResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or null if no value of this type was mapped to the given name.
     */
    public Blob getBlob(String name)
    {
        name = name.toUpperCase();
        return this.blobResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or false if no value of this type was mapped to the given name.
     */
    public boolean getBoolean(String name)
    {
        name = name.toUpperCase();
        return this.booleanResults.get(name);
    }

    /**
     * Gets the value mapped to the given name.
     *
     * @param name
     *            The name of the value to get.
     * @return The value or null if no value was mapped to the given name.
     */
    public Object get(String name)
    {
        name = name.toUpperCase();
        return this.objectResults.get(name);
    }

    /**
     * Returns an array of String representations for each value of this result.
     *
     * <p>
     * The order is the same as the one in {@link #getColumnNames()}.
     * </p>
     *
     * <p>
     * null values will be formatted as <i>"< null >"</i>
     * </p>
     *
     * @return A String array containing the values of this result.
     */
    public String[] getValueArray()
    {
        String[] values = new String[this.columnOrder.size()];

        for (int i = 0; i < this.columnOrder.size(); i ++ )
        {
            String name = this.columnOrder.get(i);
            Object value = get(name);
            values[i] = value != null ? value.toString() : "< null >";
        }

        return values;
    }

    /**
     * Sets the column types of the contained column order.
     *
     * @param types
     *            A map containing the column names as a key and the string representation of {@link SqlType}s as
     *            values.
     */
    protected void setColumnTypes(Map<String, String> types)
    {
        this.columnTypes = types;
    }

    /**
     * Gets a list containing the column names in the correct order.
     *
     * @return The list of names.
     */
    public List<String> getColumnNames()
    {
        return this.columnOrder;
    }

    /**
     * Creates an insert statement string that can be used to reproduce this rows values.
     *
     * @param table
     *            The table to insert into.
     * @param excludeColumns
     *            An array containing the names of columns that should not be added via the created insert string.
     * @return The created insert statement string.
     */
    public String export(String table, String[] excludeColumns)
    {
        var statement = new InsertStatement(null);
        statement.into(table);
        statement.unprepared();
        String col;
        boolean add = true;

        for (int i = 0; i < this.columnOrder.size(); i ++ )
        {
            col = this.columnOrder.get(i);

            for (String exCol : excludeColumns)
            {
                if (col.equalsIgnoreCase(exCol))
                {
                    add = false;
                    break;
                }
            }

            if (!add)
            {
                add = true;
                continue;
            }

            statement.set(col,
                          this.objectResults.get(col),
                          SqlType.convert(this.columnTypes.get(col)));
        }

        return statement.toString();
    }

    /**
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<Object> iterator()
    {
        return this.objectResults.values().iterator();
    }
}