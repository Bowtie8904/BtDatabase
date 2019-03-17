package bt.db.statement.clause;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.function.Supplier;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlType;
import bt.db.statement.SqlModifyStatement;
import bt.db.statement.impl.InsertStatement;
import bt.db.statement.impl.UpdateStatement;

/**
 * Holds data for SET caluses used in insert and update statements.
 * 
 * @author &#8904
 */
public class SetClause<T extends SqlModifyStatement>
{
    /** The sql type of the used value. */
    private SqlType sqlValueType;

    /** The name of the column that is used in this clause. */
    private String column;

    /** The value for the column. */
    private Object value;

    /** A suplier that is used to retrieve the long id when this clause is prepared for execution. */
    private Supplier<Long> idSupplier;

    /** The statement that created this clause. */
    private T statement;

    public SetClause(T statement, String column, Object value, SqlType type)
    {
        this.statement = statement;
        this.column = column;
        this.sqlValueType = type;
        this.value = value;
    }

    public SetClause(T statement, String column, Supplier<Long> idSupplier)
    {
        this.statement = statement;
        this.column = column;
        this.sqlValueType = SqlType.LONG;
        this.idSupplier = idSupplier;
    }

    /**
     * Prepares the values for execution.
     * 
     * @param statement
     *            The statement which should be prepared with this set clause.
     * @param parameterIndex
     *            The index of the parameter in the given statement that should be prepared.
     * @return A String representation of the prepared value.
     */
    public String prepareValue(PreparedStatement statement, int parameterIndex)
    {
        String strValue = null;

        try
        {
            if (this.idSupplier != null)
            {
                this.value = this.idSupplier.get();
            }

            if (this.value == null)
            {
                statement.setNull(parameterIndex, this.sqlValueType.getIntType());
                strValue = "null";
            }
            else
            {
                switch (this.sqlValueType)
                {
                case BOOLEAN:
                    statement.setBoolean(parameterIndex, (boolean)this.value);
                    strValue = Boolean.toString((boolean)this.value);
                    break;
                case VARCHAR:
                    statement.setString(parameterIndex, (String)this.value);
                    strValue = (String)this.value;
                    break;
                case INTEGER:
                    statement.setInt(parameterIndex, (int)this.value);
                    strValue = Integer.toString((int)this.value);
                    break;
                case LONG:
                    statement.setLong(parameterIndex, (long)this.value);
                    strValue = Long.toString((long)this.value);
                    break;
                case FLOAT:
                    statement.setFloat(parameterIndex, (float)this.value);
                    strValue = Float.toString((float)this.value);
                    break;
                case DOUBLE:
                    statement.setDouble(parameterIndex, (double)this.value);
                    strValue = Double.toString((double)this.value);
                    break;
                case DATE:
                    if (this.value instanceof String)
                    {
                        statement.setDate(parameterIndex, Date.valueOf((String)this.value));
                        strValue = (String)this.value;
                    }
                    else if (this.value instanceof Date)
                    {
                        statement.setDate(parameterIndex, (Date)this.value);
                        strValue = ((Date)this.value).toString();
                    }
                    break;
                case TIME:
                    if (this.value instanceof String)
                    {
                        statement.setTime(parameterIndex, Time.valueOf((String)this.value));
                        strValue = (String)this.value;
                    }
                    else if (this.value instanceof Time)
                    {
                        statement.setTime(parameterIndex, (Time)this.value);
                        strValue = ((Time)this.value).toString();
                    }
                    break;
                case TIMESTAMP:
                    if (this.value instanceof String)
                    {
                        statement.setTimestamp(parameterIndex, Timestamp.valueOf((String)this.value));
                        strValue = (String)this.value;
                    }
                    else if (this.value instanceof Timestamp)
                    {
                        statement.setTimestamp(parameterIndex, (Timestamp)this.value);
                        strValue = ((Timestamp)this.value).toString();
                    }
                    break;
                case CLOB:
                    statement.setClob(parameterIndex, (Clob)this.value);
                    strValue = ((Clob)this.value).toString();
                    break;
                case BLOB:
                    statement.setBlob(parameterIndex, (Blob)this.value);
                    strValue = ((Blob)this.value).toString();
                    break;
                default:
                    break;
                }
            }
        }
        catch (Exception e)
        {
            DatabaseAccess.log.print(this, e);
        }

        return strValue;
    }

    /**
     * Returns the String representing this set clause.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        if (this.statement instanceof InsertStatement)
        {
            return this.column;
        }
        else if (this.statement instanceof UpdateStatement)
        {
            return this.column + " = ?";
        }

        return "Invalid statement type";
    }

    public String toString(boolean prepared)
    {
        if (prepared)
        {
            return toString();
        }

        if (this.statement instanceof InsertStatement)
        {
            return this.column;
        }
        else if (this.statement instanceof UpdateStatement)
        {
            return this.column + " = " + getStringValue();
        }

        return "Invalid statement type";
    }

    public String getStringValue()
    {
        String strValue = null;

        try
        {
            if (this.idSupplier != null)
            {
                this.value = this.idSupplier.get();
            }

            if (this.value == null)
            {
                strValue = "null";
            }
            else
            {
                switch (this.sqlValueType)
                {
                case BOOLEAN:
                    strValue = Boolean.toString((boolean)this.value);
                    break;
                case VARCHAR:
                    strValue = "'" + (String)this.value + "'";
                    break;
                case INTEGER:
                    strValue = Integer.toString((int)this.value);
                    break;
                case LONG:
                    strValue = Long.toString((long)this.value);
                    break;
                case FLOAT:
                    strValue = Float.toString((float)this.value);
                    break;
                case DOUBLE:
                    strValue = Double.toString((double)this.value);
                    break;
                case DATE:
                    if (this.value instanceof String)
                    {
                        strValue = "'" + (String)this.value + "'";
                    }
                    else if (this.value instanceof Date)
                    {
                        strValue = "'" + ((Date)this.value).toString() + "'";
                    }
                    break;
                case TIME:
                    if (this.value instanceof String)
                    {
                        strValue = "'" + (String)this.value + "'";
                    }
                    else if (this.value instanceof Time)
                    {
                        strValue = "'" + ((Time)this.value).toString() + "'";
                    }
                    break;
                case TIMESTAMP:
                    if (this.value instanceof String)
                    {
                        strValue = "'" + (String)this.value + "'";
                    }
                    else if (this.value instanceof Timestamp)
                    {
                        strValue = "'" + ((Timestamp)this.value).toString() + "'";
                    }
                    break;
                case CLOB:
                    strValue = ((Clob)this.value).toString();
                    break;
                case BLOB:
                    strValue = ((Blob)this.value).toString();
                    break;
                default:
                    break;
                }
            }
        }
        catch (Exception e)
        {
            DatabaseAccess.log.print(this, e);
        }

        return strValue;
    }
}