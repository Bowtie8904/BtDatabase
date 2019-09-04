package bt.db.statement.impl;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlType;
import bt.db.statement.SqlModifyStatement;
import bt.db.statement.clause.SetClause;

/**
 * Represents an SQL insert statement which can be extended through method chaining.
 *
 * @author &#8904
 */
public class InsertStatement extends SqlModifyStatement<InsertStatement, InsertStatement>
{
    private int repeats = 1;

    private SelectStatement dataSelect;

    /**
     * Creates a new instance. On duplicate key this will log an error message and return -1.
     *
     * @param db
     *            The database that should be used for this statement.
     */
    public InsertStatement(DatabaseAccess db)
    {
        super(db);
        this.statementKeyword = "INSERT INTO";
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#commit()
     */
    @Override
    public InsertStatement commit()
    {
        return (InsertStatement)super.commit();
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#unprepared()
     */
    @Override
    public InsertStatement unprepared()
    {
        return (InsertStatement)super.unprepared();
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
     * @param table
     *            The name of the table.
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
     * @return
     */
    public InsertStatement from(SelectStatement select)
    {
        this.dataSelect = select.unprepared();
        return this;
    }

    /**
     * Defines how often the statement should be executed. (default=1)
     *
     * <p>
     * This might distort returned error codes of the execute method if executions of some (or all) statements fails.
     * </p>
     *
     * @param repeats
     *            The number of executions including the first one. repeats=0 would result in no executions and
     *            repeats=1 has the same effect as not calling this method.
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
     * @param column
     *            The column whichs value should be set to null.
     * @param sqlType
     *            The sql value type of the column. This uses standard {@link Types}.
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
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
     * @param sqlType
     *            The type of the column.
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
     * Uses the given supplier to retrieve a long value for the given column when this statement is prepared for
     * execution.
     *
     * @param column
     *            The column whichs value should be set.
     * @param idSupplier
     *            The supplier that offers a long value for the given column.
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, Supplier<Long> longSupplier)
    {
        SetClause<InsertStatement> set = new SetClause<>(this,
                                                                        column,
                                                                        longSupplier);
        addSetClause(set);
        return this;
    }

    /**
     * Sets the given column to the given value.
     *
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
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
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
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
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
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
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
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
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
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
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
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
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
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
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
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
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
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
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
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
     * @param column
     *            The column whichs value should be set.
     * @param value
     *            The value to use.
     * @return This instance for chaining.
     */
    public InsertStatement set(String column, Clob value)
    {
        return set(column,
                   value,
                   SqlType.CLOB);
    }

    /**
     * Defines a data modifying statement (insert, update, delete) to execute if a check constraint is violated by the
     * statement.
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public InsertStatement onCheckViolation(SqlModifyStatement statement)
    {
        this.onCheckFail = (s, e) ->
        {
            return statement.execute();
        };
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) to execute if a check constraint is violated by the
     * statement.
     *
     * <p>
     * The first parameter (InsertStatement) will be this statement instance, the second one is the SQLException that
     * caused the fail. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param onDuplicate
     *            The function to execute.
     * @return This instance for chaining.
     */
    public InsertStatement onCheckViolation(BiFunction<InsertStatement, SQLException, Integer> onDuplicate)
    {
        this.onCheckFail = onDuplicate;
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) to execute if a foreign key constraint is violated by
     * the statement.
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public InsertStatement onForeignKeyViolation(SqlModifyStatement statement)
    {
        this.onForeignKeyFail = (s, e) ->
        {
            return statement.execute();
        };
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) to execute if a foreign key constraint is violated by
     * the statement.
     *
     * <p>
     * The first parameter (InsertStatement) will be this statement instance, the second one is the SQLException that
     * caused the fail. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param onDuplicate
     *            The function to execute.
     * @return This instance for chaining.
     */
    public InsertStatement onForeignKeyViolation(BiFunction<InsertStatement, SQLException, Integer> onDuplicate)
    {
        this.onForeignKeyFail = onDuplicate;
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) to execute if the primary key used in the original
     * insert is already contained in the table or if a unique constraint is violated.
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public InsertStatement onDuplicateKey(SqlModifyStatement statement)
    {
        this.onDuplicateKey = (s, e) ->
        {
            return statement.execute();
        };
        return this;
    }

    /**
     * Defines a function that will be executed if the primary key used in the original insert is already contained in
     * the table or if a unique constraint is violated.
     *
     * <p>
     * The first parameter (InsertStatement) will be this statement instance, the second one is the SQLException that
     * caused the fail. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param onDuplicate
     *            The function to execute.
     * @return This instance for chaining.
     */
    public InsertStatement onDuplicateKey(BiFunction<InsertStatement, SQLException, Integer> onDuplicate)
    {
        this.onDuplicateKey = onDuplicate;
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) which will be executed if there was an error during
     * the execution of the original insert statement.
     *
     * <p>
     * The given statement is only executed if the error was not handled by one of the other exception functions
     * (onDuplicateKey, onCheckViolation, ...).
     * </p>
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param onFail
     *            The SqlModifyStatement to execute instead.
     * @return This instance for chaining.
     */
    public InsertStatement onFail(SqlModifyStatement onFail)
    {
        this.onFail = (statement, e) ->
        {
            return onFail.execute();
        };

        return this;
    }

    /**
     * Defines a BiFunction that will be executed if there was an error during the execution of this statement.
     *
     * <p>
     * The first parameter (InsertStatement) will be this statement instance, the second one is the SQLException that
     * caused the fail. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     *
     * <p>
     * The given function is only executed if the error was not handled by one of the other exception functions
     * (onDuplicateKey, onCheckViolation, ...).
     * </p>
     *
     * @param onFail
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
    public InsertStatement onFail(BiFunction<InsertStatement, SQLException, Integer> onFail)
    {
        this.onFail = onFail;
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) to execute if the original insert affected less rows
     * than the given lower threshhold.
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param lowerThreshhold
     *            The threshhold to check.
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public InsertStatement onLessThan(int lowerThreshhold, SqlModifyStatement statement)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = (i, set) ->
        {
            return statement.execute();
        };
        return this;
    }

    /**
     * Defines a BiFunction that will be executed if the original insert affected less rows than the given lower
     * threshhold.
     *
     * <p>
     * The first parameter (int) will be the number of rows affected, the second one is the InsertStatement from the
     * original insert. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param lowerThreshhold
     *            The threshhold to check.
     * @param onLessThan
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
    public InsertStatement onLessThan(int lowerThreshhold, BiFunction<Integer, InsertStatement, Integer> onLessThan)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = onLessThan;
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) to execute if the original insert affected more rows
     * than the given higher threshhold.
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param higherThreshhold
     *            The threshhold to check.
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public InsertStatement onMoreThan(int higherThreshhold, SqlModifyStatement statement)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = (i, set) ->
        {
            return statement.execute();
        };
        return this;
    }

    /**
     * Defines a BiFunction that will be executed if the original insert affected more rows than the given higher
     * threshhold.
     *
     * <p>
     * The first parameter (int) will be the number of rows affected, the second one is the InsertStatement from the
     * original insert. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param higherThreshhold
     *            The threshhold to check.
     * @param onLessThan
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
    public InsertStatement onMoreThan(int higherThreshhold,
                                      BiFunction<Integer, InsertStatement, Integer> onMoreThan)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = onMoreThan;
        return this;
    }

    /**
     * @return The number of affected rows or an error code (usually -1, can be customized for different errors via the
     *         fail methods).
     *
     * @see bt.db.statement.SqlModifyStatement#execute()
     */
    @Override
    public int execute()
    {
        return execute(false);
    }

    /**
     * @return The number of affected rows or an error code (usually -1, can be customized for different errors via the
     *         fail methods).
     *
     * @see bt.db.statement.SqlModifyStatement#execute(boolean)
     */
    @Override
    public int execute(boolean printLogs)
    {
        int result = 0;

        for (int i = 0; i < this.repeats; i ++ )
        {
            result += executeStatement(printLogs);
        }

        if (result < this.lowerThreshhold && this.onLessThan != null)
        {
            return this.onLessThan.apply(result,
                                         this);
        }
        else if (result > this.higherThreshhold && this.onMoreThan != null)
        {
            return this.onMoreThan.apply(result,
                                         this);
        }

        return result;
    }

    private int executeStatement(boolean printLogs)
    {
        String sql = toString();

        if (this.setClauses.isEmpty() && this.dataSelect == null)
        {
            DatabaseAccess.log.print(
                                     "Can't execute insert statement without any values. Please define at least one column value.");
            return -1;
        }

        int result = Integer.MIN_VALUE;

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
        {
            log("Executing: " + sql,
                printLogs);

            if (this.dataSelect == null && this.prepared)
            {
                if (!this.setClauses.isEmpty())
                {
                    log("With values:",
                        printLogs);
                }

                for (int i = 0; i < this.setClauses.size(); i ++ )
                {
                    SetClause<InsertStatement> set = this.setClauses.get(i);
                    log("p" + (i + 1) + " = " + set.prepareValue(statement,
                                                                 i + 1),
                        printLogs);
                }
            }

            try
            {
                result = statement.executeUpdate();
                log("Affected rows: " + result,
                    printLogs);

                if (this.shouldCommit)
                {
                    this.db.commit();
                }

                if (result < this.lowerThreshhold && this.onLessThan != null)
                {
                    return this.onLessThan.apply(result,
                                                 this);
                }
                else if (result > this.higherThreshhold && this.onMoreThan != null)
                {
                    return this.onMoreThan.apply(result,
                                                 this);
                }
            }
            catch (SQLException updateFail)
            {
                if (this.onDuplicateKey != null && updateFail.getSQLState().equals(DUPLICATE_KEY_ERROR))
                {
                    result = this.onDuplicateKey.apply(this, updateFail);
                }
                else if (this.onCheckFail != null && updateFail.getSQLState().equals(CHECK_CONSTRAINT_VIOLATION_ERROR))
                {
                    result = this.onCheckFail.apply(this, updateFail);
                }
                else if (this.onForeignKeyFail != null && updateFail.getSQLState().equals(FOREIGN_KEY_VIOLATION_ERROR))
                {
                    result = this.onForeignKeyFail.apply(this, updateFail);
                }
                else if (this.onFail != null)
                {
                    result = this.onFail.apply(this, updateFail);
                }
                else
                {
                    DatabaseAccess.log.print(updateFail);
                    result = -1;
                }
            }
        }
        catch (SQLException e)
        {
            if (this.onFail != null)
            {
                result = this.onFail.apply(this, e);
            }
            else
            {
                DatabaseAccess.log.print(sql);
                DatabaseAccess.log.print(e);
                result = -1;
            }
        }

        return result;
    }

    /**
     * @see bt.db.statement.SqlStatement#toString()
     */
    @Override
    public String toString()
    {
        String sql = this.statementKeyword + " ";

        sql += this.tables[0];

        if (this.dataSelect == null)
        {
            if (!this.setClauses.isEmpty())
            {
                sql += " (";

                for (SetClause<InsertStatement> set : this.setClauses)
                {
                    sql += set.toString(this.prepared) + ", ";
                }

                sql = sql.substring(0,
                                    sql.length() - 2);

                sql += ") VALUES (";

                if (this.prepared)
                {
                    for (SetClause<InsertStatement> set : this.setClauses)
                    {
                        sql += "?, ";
                    }
                }
                else
                {
                    for (SetClause<InsertStatement> set : this.setClauses)
                    {
                        sql += set.getStringValue() + ", ";
                    }
                }

                sql = sql.substring(0,
                                    sql.length() - 2);

                sql += ")";
            }
        }
        else
        {
            sql += " " + this.dataSelect.toString();
        }

        return sql;
    }
}