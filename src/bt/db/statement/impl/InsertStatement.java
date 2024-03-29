package bt.db.statement.impl;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlType;
import bt.db.exc.SqlExecutionException;
import bt.db.func.Sql;
import bt.db.func.SqlFunction;
import bt.db.statement.SqlModifyStatement;
import bt.db.statement.clause.SetClause;
import bt.log.Log;

import java.math.BigDecimal;
import java.sql.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Represents an SQL insert statement which can be extended through method chaining.
 *
 * @author &#8904
 */
public class InsertStatement extends SqlModifyStatement<InsertStatement, InsertStatement>
{
    private int repeats = 1;

    private SelectStatement dataSelect;

    private Consumer<Long> identityReceiver;

    /**
     * Creates a new instance. On duplicate key this will log an error message and return -1.
     *
     * @param db The database that should be used for this statement.
     */
    public InsertStatement(DatabaseAccess db)
    {
        super(db);
        this.statementKeyword = "INSERT INTO";
    }

    /**
     * Gets the name of the table that data is inserted into.
     *
     * @return
     */
    public String getTable()
    {
        return this.tables.length > 0 ? this.tables[0] : null;
    }

    /**
     * Defines the table to insert into.
     *
     * @param table The name of the table.
     *
     * @return This instance for chaining.
     */
    public InsertStatement into(String table)
    {
        this.tables = new String[]
                {
                        table.toUpperCase()
                };

        return this;
    }

    /**
     * Defines a select statement whichs result set contains the data that will be inserted.
     *
     * <p>
     * Note that the select will be executed unprepared.
     * </p>
     *
     * @param select
     *
     * @return
     */
    public InsertStatement from(SelectStatement select)
    {
        this.dataSelect = select.unprepared();
        return this;
    }

    /**
     * Defines the columns used in a 'select into from' statement.
     *
     * @param columns
     *
     * @return
     */
    public InsertStatement columns(Object... columns)
    {
        this.columns = columns;
        return this;
    }

    /**
     * Defines how often the statement should be executed. (default=1)
     *
     * <p>
     * This might distort returned error codes of the execute method if executions of some (or all) statements fails.
     * </p>
     *
     * @param repeats The number of executions including the first one. repeats=0 would result in no executions and
     *                repeats=1 has the same effect as not calling this method.
     *
     * @return This instance for chaining.
     */
    public InsertStatement repeat(int repeats)
    {
        this.repeats = repeats;
        return this;
    }

    /**
     * Sets the value of the given column to <i>null</i>.
     *
     * @param column  The column whichs value should be set to null.
     * @param sqlType The sql value type of the column. This uses standard {@link Types}.
     *
     * @return This instance for chaining.
     */
    public InsertStatement setNull(String column, SqlType sqlType)
    {
        return set(column,
                   null,
                   sqlType);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column  The column whichs value should be set.
     * @param value   The value to use.
     * @param sqlType The type of the column.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, Object value, SqlType sqlType)
    {
        SetClause<InsertStatement> set = new SetClause<>(this,
                                                         column,
                                                         value,
                                                         sqlType);
        addSetClause(set);
        return this;
    }

    /**
     * Uses the given supplier to retrieve a value for the given column when this statement is prepared for execution.
     *
     * @param column        The column whichs value should be set.
     * @param valueSupplier The supplier that offers a value for the given column.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, SqlType sqlType, Supplier<?> valueSupplier)
    {
        SetClause<InsertStatement> set = new SetClause<>(this,
                                                         column,
                                                         sqlType,
                                                         valueSupplier);
        addSetClause(set);
        return this;
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, SqlFunction value)
    {
        return set(column,
                   value,
                   SqlType.UNKNOWN);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, Date value)
    {
        return set(column,
                   value,
                   SqlType.DATE);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, Time value)
    {
        return set(column,
                   value,
                   SqlType.TIME);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, Timestamp value)
    {
        return set(column,
                   value,
                   SqlType.TIMESTAMP);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, int value)
    {
        return set(column,
                   value,
                   SqlType.INTEGER);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, long value)
    {
        return set(column,
                   value,
                   SqlType.LONG);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, double value)
    {
        return set(column,
                   value,
                   SqlType.DOUBLE);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, float value)
    {
        return set(column,
                   value,
                   SqlType.FLOAT);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, boolean value)
    {
        return set(column,
                   value,
                   SqlType.BOOLEAN);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, String value)
    {
        return set(column,
                   value,
                   SqlType.VARCHAR);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, Blob value)
    {
        return set(column,
                   value,
                   SqlType.BLOB);
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column The column whichs value should be set.
     * @param value  The value to use.
     *
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, Clob value)
    {
        return set(column,
                   value,
                   SqlType.CLOB);
    }

    /**
     * Defines a consumer that receives the used identity for this insert statement.
     *
     * @param identityConsumer
     *
     * @return This instance for chaining.
     */
    public InsertStatement usedIdentity(Consumer<Long> identityConsumer)
    {
        this.identityReceiver = identityConsumer;
        return this;
    }

    /**
     * @return The number of affected rows or an error code (usually -1, can be customized for different errors via the
     * fail methods).
     *
     * @see bt.db.statement.SqlModifyStatement#execute()
     */
    @Override
    public int execute()
    {
        startExecutionTime();
        int result = 0;

        for (int i = 0; i < this.repeats; i++)
        {
            result += executeStatement();
        }

        result = handleThreshholds(result);

        endExecutionTime();

        return result;
    }

    @Override
    protected int executeStatement()
    {
        String sql = toString();

        if (this.setClauses.isEmpty() && this.dataSelect == null)
        {
            Log.error("Can't execute insert statement without any values. Please define at least one column value.");
            return -1;
        }

        int result = Integer.MIN_VALUE;

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
        {
            Log.debug("Executing: " + sql);

            if (this.dataSelect == null && this.prepared)
            {
                var reducedList = this.setClauses.stream().filter(set -> !(set.getValue() instanceof SqlFunction)).collect(Collectors.toList());

                if (!reducedList.isEmpty())
                {
                    Log.debug("With values:");
                }

                for (int i = 0; i < reducedList.size(); i++)
                {
                    SetClause<InsertStatement> set = reducedList.get(i);
                    Log.debug("p" + (i + 1) + " = " + set.prepareValue(statement, i + 1));
                }
            }

            result = statement.executeUpdate();
            endExecutionTime();
            Log.debug("Affected rows: " + result);

            if (this.shouldCommit)
            {
                this.db.commit();
            }

            if (this.identityReceiver != null)
            {
                var set = this.db.select(Sql.column(Sql.lastIdentity().toString()).as("id")).from(this.tables[0]).first().execute();
                long usedIdentity = -1;

                for (var row : set)
                {
                    usedIdentity = ((BigDecimal)row.get("id")).longValue();
                    break;
                }

                this.identityReceiver.accept(usedIdentity);
            }

            handleSuccess(result);
        }
        catch (SQLException e)
        {
            result = handleFail(new SqlExecutionException(e.getMessage(), sql, e));
        }

        return result;
    }

    /**
     * @see bt.db.statement.SqlStatement#toString()
     */
    @Override
    public String toString()
    {
        String sql = this.statementKeyword + " " + System.lineSeparator();

        sql += this.tables[0];

        if (this.dataSelect == null)
        {
            if (!this.setClauses.isEmpty())
            {
                sql += " (" + System.lineSeparator();

                for (SetClause<InsertStatement> set : this.setClauses)
                {
                    sql += set.toString(this.prepared) + ", " + System.lineSeparator();
                }

                sql = sql.substring(0,
                                    sql.length() - (System.lineSeparator().length() + 2));

                sql += ") " + System.lineSeparator() + "VALUES " + System.lineSeparator() + "(";

                if (this.prepared)
                {
                    for (SetClause<InsertStatement> set : this.setClauses)
                    {
                        if (set.getValue() instanceof SqlFunction)
                        {
                            sql += set.getStringValue() + ", " + System.lineSeparator();
                        }
                        else
                        {
                            sql += "?, " + System.lineSeparator();
                        }
                    }
                }
                else
                {
                    for (SetClause<InsertStatement> set : this.setClauses)
                    {
                        sql += set.getStringValue() + ", " + System.lineSeparator();
                    }
                }

                sql = sql.substring(0,
                                    sql.length() - (System.lineSeparator().length() + 2));

                sql += ")";
            }
        }
        else
        {
            if (this.columns != null && this.columns.length > 0)
            {
                sql += "(" + System.lineSeparator();

                for (Object col : this.columns)
                {
                    sql += col.toString() + ", " + System.lineSeparator();
                }

                sql = sql.substring(0, sql.length() - (System.lineSeparator().length() + 2));

                sql += ")" + System.lineSeparator();
            }
            sql += " " + this.dataSelect.toString();
        }

        return sql;
    }
}