package bt.db.statement.result;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author &#8904
 *
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

    public SqlResult(List<String> columnOrder)
    {
        this.columnOrder = columnOrder;
    }

    public SqlResult(String... columnOrder)
    {
        this.columnOrder = List.of(columnOrder);
    }

    public void useValues(Object... values)
    {
        if (this.columnOrder.size() != values.length)
        {
            throw new IllegalArgumentException(
                    "Must pass the same number of values as there is columns in the result.");
        }

        for (int i = 0; i < this.columnOrder.size(); i ++ )
        {
            putObject(this.columnOrder.get(i), values[i]);

            if (values[i] instanceof String)
            {
                put(this.columnOrder.get(i), (String)values[i]);
            }
            else if (values[i] instanceof Integer)
            {
                put(this.columnOrder.get(i), (Integer)values[i]);
            }
            else if (values[i] instanceof Short)
            {
                put(this.columnOrder.get(i), (Short)values[i]);
            }
            else if (values[i] instanceof Byte)
            {
                put(this.columnOrder.get(i), (Byte)values[i]);
            }
            else if (values[i] instanceof Long)
            {
                put(this.columnOrder.get(i), (Long)values[i]);
            }
            else if (values[i] instanceof Double)
            {
                put(this.columnOrder.get(i), (Double)values[i]);
            }
            else if (values[i] instanceof Float)
            {
                put(this.columnOrder.get(i), (Float)values[i]);
            }
            else if (values[i] instanceof Date)
            {
                put(this.columnOrder.get(i), (Date)values[i]);
            }
            else if (values[i] instanceof Time)
            {
                put(this.columnOrder.get(i), (Time)values[i]);
            }
            else if (values[i] instanceof Timestamp)
            {
                put(this.columnOrder.get(i), (Timestamp)values[i]);
            }
            else if (values[i] instanceof Boolean)
            {
                put(this.columnOrder.get(i), (Boolean)values[i]);
            }
            else if (values[i] instanceof Clob)
            {
                put(this.columnOrder.get(i), (Clob)values[i]);
            }
            else if (values[i] instanceof Blob)
            {
                put(this.columnOrder.get(i), (Blob)values[i]);
            }
        }
    }

    public void putObject(String name, Object value)
    {
        name = name.toUpperCase();
        objectResults.put(name, value);
    }

    public void put(String name, String value)
    {
        name = name.toUpperCase();
        stringResults.put(name, value);
        putObject(name, value);
    }

    public void put(String name, Time value)
    {
        name = name.toUpperCase();
        timeResults.put(name, value);
        putObject(name, value);
    }

    public void put(String name, Timestamp value)
    {
        name = name.toUpperCase();
        timestampResults.put(name, value);
        putObject(name, value);
    }

    public void put(String name, Clob value)
    {
        name = name.toUpperCase();
        clobResults.put(name, value);
        putObject(name, value);
    }

    public void put(String name, Blob value)
    {
        name = name.toUpperCase();
        blobResults.put(name, value);
        putObject(name, value);
    }

    public void put(String name, byte value)
    {
        name = name.toUpperCase();
        byteResults.put(name, value);
        putObject(name, value);
    }

    public void put(String name, short value)
    {
        name = name.toUpperCase();
        shortResults.put(name, value);
        putObject(name, value);
    }

    public void put(String name, int value)
    {
        name = name.toUpperCase();
        intResults.put(name, value);
        putObject(name, value);
    }
    
    public void put(String name, long value)
    {
        name = name.toUpperCase();
        longResults.put(name, value);
        putObject(name, value);
    }
    
    public void put(String name, double value)
    {
        name = name.toUpperCase();
        doubleResults.put(name, value);
        putObject(name, value);
    }
    
    public void put(String name, float value)
    {
        name = name.toUpperCase();
        floatResults.put(name, value);
        putObject(name, value);
    }
    
    public void put(String name, Date value)
    {
        name = name.toUpperCase();
        dateResults.put(name, value);
        putObject(name, value);
    }
    
    public void put(String name, boolean value)
    {
        name = name.toUpperCase();
        booleanResults.put(name, value);
        putObject(name, value);
    }
    
    public String getString(String name)
    {
        name = name.toUpperCase();
        return stringResults.get(name);
    }
    
    public byte getByte(String name)
    {
        name = name.toUpperCase();
        return byteResults.get(name);
    }

    public short getShort(String name)
    {
        name = name.toUpperCase();
        return shortResults.get(name);
    }

    public int getInt(String name)
    {
        name = name.toUpperCase();
        return intResults.get(name);
    }
    
    public long getLong(String name)
    {
        name = name.toUpperCase();
        return longResults.get(name);
    }
    
    public double getDouble(String name)
    {
        name = name.toUpperCase();
        return doubleResults.get(name);
    }
    
    public float getFloat(String name)
    {
        name = name.toUpperCase();
        return floatResults.get(name);
    }
    
    public Date getDate(String name)
    {
        name = name.toUpperCase();
        return dateResults.get(name);
    }
    
    public Time getTime(String name)
    {
        name = name.toUpperCase();
        return timeResults.get(name);
    }

    public Timestamp getTimestamp(String name)
    {
        name = name.toUpperCase();
        return timestampResults.get(name);
    }

    public Clob getClob(String name)
    {
        name = name.toUpperCase();
        return clobResults.get(name);
    }

    public Blob getBlob(String name)
    {
        name = name.toUpperCase();
        return blobResults.get(name);
    }

    public boolean getBoolean(String name)
    {
        name = name.toUpperCase();
        return booleanResults.get(name);
    }

    public Object get(String name)
    {
        name = name.toUpperCase();
        return objectResults.get(name);
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

    public List<String> getColumnNames()
    {
        return this.columnOrder;
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