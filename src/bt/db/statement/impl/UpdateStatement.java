package bt.db.statement.impl;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlType;
import bt.db.exc.SqlExecutionException;
import bt.db.func.SqlFunction;
import bt.db.statement.SqlModifyStatement;
import bt.db.statement.clause.SetClause;
import bt.db.statement.clause.condition.ConditionalClause;
import bt.db.statement.value.Preparable;
import bt.db.statement.value.Value;
import bt.log.Log;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Represents an SQL update statement which can be extended through method chaining.
 *
 * @author &#8904
 */
public class UpdateStatement extends SqlModifyStatement<UpdateStatement, UpdateStatement> implements Preparable
{
    /**
     * Creates a new instance.
     *
     * @param db    The database that should be used for the statement.
     * @param table The table that should be updated.
     */
    public UpdateStatement(DatabaseAccess db, String table)
    {
        super(db);

        this.tables = new String[]
                {
                        table.toUpperCase()
                };
        this.statementKeyword = "UPDATE";
    }

    /**
     * Gets the table in which this statement will update.
     *
     * @return The name of the table.
     */
    public String getTable()
    {
        return this.tables.length > 0 ? this.tables[0] : null;
    }

    /**
     * Creates a new where conditional clause using the given column for this statement.
     *
     * @param column The column to use in this condition.
     *
     * @return The created ConditionalClause.
     */
    public ConditionalClause<UpdateStatement> where(String column)
    {
        ConditionalClause<UpdateStatement> where = new ConditionalClause<>(this,
                                                                           column,
                                                                           ConditionalClause.WHERE);
        addWhereClause(where);
        return where;
    }

    /**
     * Creates a new where conditional clause using the given column for this statement.
     *
     * @param column The column to use in this condition.
     * @param prefix A String that will be put in front of the expression. Can be used for parenthesis.
     *
     * @return The created ConditionalClause.
     */
    public ConditionalClause<UpdateStatement> where(String prefix, String column)
    {
        ConditionalClause<UpdateStatement> where = new ConditionalClause<>(this,
                                                                           prefix,
                                                                           column,
                                                                           ConditionalClause.WHERE);
        addWhereClause(where);
        return where;
    }

    /**
     * Creates a new conditional clause to chain with an existing where or having clause using the given column for this
     * statement.
     *
     * @param column The column to use in this condition.
     *
     * @return The created ConditionalClause.
     */
    public ConditionalClause<UpdateStatement> and(String column)
    {
        ConditionalClause<UpdateStatement> where = new ConditionalClause<>(this,
                                                                           column,
                                                                           ConditionalClause.AND);
        addWhereClause(where);
        return where;
    }

    /**
     * Creates a new conditional clause to chain with an existing where or having clause using the given column for this
     * statement.
     *
     * @param column The column to use in this condition.
     * @param prefix A String that will be put in front of the expression. Can be used for parenthesis.
     *
     * @return The created ConditionalClause.
     */
    public ConditionalClause<UpdateStatement> and(String prefix, String column)
    {
        ConditionalClause<UpdateStatement> clause = new ConditionalClause<>(this,
                                                                            prefix,
                                                                            column,
                                                                            ConditionalClause.AND);

        addWhereClause(clause);

        return clause;
    }

    /**
     * Creates a new conditional clause to chain with an existing where or having clause using the given column for this
     * statement.
     *
     * @param column The column to use in this condition.
     *
     * @return The created ConditionalClause.
     */
    public ConditionalClause<UpdateStatement> or(String column)
    {
        ConditionalClause<UpdateStatement> where = new ConditionalClause<>(this,
                                                                           column,
                                                                           ConditionalClause.OR);
        addWhereClause(where);
        return where;
    }

    /**
     * Creates a new conditional clause to chain with an existing where or having clause using the given column for this
     * statement.
     *
     * @param column The column to use in this condition.
     * @param prefix A String that will be put in front of the expression. Can be used for parenthesis.
     *
     * @return The created ConditionalClause.
     */
    public ConditionalClause<UpdateStatement> or(String prefix, String column)
    {
        ConditionalClause<UpdateStatement> clause = new ConditionalClause<>(this,
                                                                            prefix,
                                                                            column,
                                                                            ConditionalClause.OR);

        addWhereClause(clause);

        return clause;
    }

    /**
     * Sets the value of the given column to <i>null</i>.
     *
     * @param column  The column whichs value should be updated.
     * @param sqlType The sql value type of the column. This uses standard {@link Types}.
     *
     * @return This instance for chaining.
     */
    public UpdateStatement setNull(String column, SqlType sqlType)
    {
        return set(column,
                   null,
                   sqlType);
    }

    /**
     * Updates the given column with the given value.
     *
     * @param column  The column whichs value should be updated.
     * @param value   The value to update with
     * @param sqlType The type of the column.
     *
     * @return This instance for chaining.
     */
    public UpdateStatement set(String column, Object value, SqlType sqlType)
    {
        SetClause<UpdateStatement> set = new SetClause<>(this,
                                                         column,
                                                         value,
                                                         sqlType);
        addSetClause(set);
        return this;
    }

    /**
     * Updates the given column with the given value.
     *
     * @param column The column whichs value should be updated.
     * @param value  The value to update with.
     *
     * @return This instance for chaining.
     */
    public UpdateStatement set(String column, SqlFunction value)
    {
        return set(column,
                   value,
                   SqlType.UNKNOWN);
    }

    /**
     * Uses the given supplier to retrieve a value for the given column when this statement is prepared for execution.
     *
     * @param column        The column whichs value should be set.
     * @param valueSupplier The supplier that offers a value for the given column.
     *
     * @return This instance for chaining.
     */
    public UpdateStatement set(String column, SqlType sqlType, Supplier<?> valueSupplier)
    {
        SetClause<UpdateStatement> set = new SetClause<>(this,
                                                         column,
                                                         sqlType,
                                                         valueSupplier);
        addSetClause(set);
        return this;
    }

    /**
     * Updates the given column with the given value.
     *
     * @param column The column whichs value should be updated.
     * @param value  The value to update with
     *
     * @return This instance for chaining.
     */
    public UpdateStatement set(String column, int value)
    {
        return set(column,
                   value,
                   SqlType.INTEGER);
    }

    /**
     * Updates the given column with the given value.
     *
     * @param column The column whichs value should be updated.
     * @param value  The value to update with
     *
     * @return This instance for chaining.
     */
    public UpdateStatement set(String column, long value)
    {
        return set(column,
                   value,
                   SqlType.LONG);
    }

    /**
     * Updates the given column with the given value.
     *
     * @param column The column whichs value should be updated.
     * @param value  The value to update with
     *
     * @return This instance for chaining.
     */
    public UpdateStatement set(String column, double value)
    {
        return set(column,
                   value,
                   SqlType.DOUBLE);
    }

    /**
     * Updates the given column with the given value.
     *
     * @param column The column whichs value should be updated.
     * @param value  The value to update with
     *
     * @return This instance for chaining.
     */
    public UpdateStatement set(String column, float value)
    {
        return set(column,
                   value,
                   SqlType.FLOAT);
    }

    /**
     * Updates the given column with the given value.
     *
     * @param column The column whichs value should be updated.
     * @param value  The value to update with
     *
     * @return This instance for chaining.
     */
    public UpdateStatement set(String column, boolean value)
    {
        return set(column,
                   value,
                   SqlType.BOOLEAN);
    }

    /**
     * Updates the given column with the given value.
     *
     * @param column The column whichs value should be updated.
     * @param value  The value to update with
     *
     * @return This instance for chaining.
     */
    public UpdateStatement set(String column, String value)
    {
        return set(column,
                   value,
                   SqlType.VARCHAR);
    }

    /**
     * Executes the built statement. Depending on the number of rows affected, the defined onLessThan or onMoreThan
     * might be executed. If there is an error during this execution, the onFail function is called.
     *
     * @see bt.db.statement.SqlModifyStatement#execute()
     */
    @Override
    protected int executeStatement()
    {
        String sql = toString();

        if (this.setClauses.isEmpty())
        {
            Log.error("Can't execute update statement without any values. Please define at least one column value.");
            return -1;
        }

        int result = Integer.MIN_VALUE;

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
        {
            Log.debug("Executing: " + sql);

            if (this.prepared)
            {
                var reducedList = this.setClauses.stream().filter(set -> !(set.getValue() instanceof SqlFunction)).collect(Collectors.toList());

                if (!reducedList.isEmpty())
                {
                    Log.debug("With values:");
                }
                int i = 0;

                for (; i < reducedList.size(); i++)
                {
                    SetClause<UpdateStatement> set = reducedList.get(i);
                    Log.debug("p" + (i + 1) + " = " + set.prepareValue(statement, i + 1));
                }

                List<Value> values = getValues();
                Preparable.prepareStatement(statement, values, i);

                for (Value val : values)
                {
                    Log.debug("p" + (i + 1) + " = " + val.getValue());
                }
            }

            result = statement.executeUpdate();
            endExecutionTime();
            Log.debug("Affected rows: " + result);

            if (this.shouldCommit)
            {
                this.db.commit();
            }

            handleSuccess(result);
            result = handleThreshholds(result);
        }
        catch (SQLException e)
        {
            result = handleFail(new SqlExecutionException(e.getMessage(), sql, e));
        }

        return result;
    }

    /**
     * Formats the full select statement.
     *
     * <p>
     * Depending on {@link #isPrepared()} values will either be inserted into the raw sql or replaced by ? placeholders.
     * </p>
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        String sql = this.statementKeyword + " " + System.lineSeparator();

        sql += this.tables[0];

        if (!this.setClauses.isEmpty())
        {
            sql += " SET " + System.lineSeparator();

            for (SetClause<UpdateStatement> set : this.setClauses)
            {
                sql += set.toString(this.prepared) + ", " + System.lineSeparator();
            }

            sql = sql.substring(0,
                                sql.length() - (System.lineSeparator().length() + 2));
        }

        for (ConditionalClause<UpdateStatement> where : this.whereClauses)
        {
            sql += " " + where.toString(this.prepared) + System.lineSeparator();
        }

        return sql;
    }

    /**
     * @see bt.db.statement.value.Preparable#getValues()
     */
    @Override
    public List<Value> getValues()
    {
        List<Value> values = new ArrayList<>();

        for (ConditionalClause c : this.whereClauses)
        {
            values.addAll(c.getValues());
        }

        return values;
    }
}