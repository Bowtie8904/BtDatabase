package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlType;
import bt.db.statement.SqlModifyStatement;
import bt.db.statement.clause.BetweenConditionalClause;
import bt.db.statement.clause.ConditionalClause;
import bt.db.statement.clause.SetClause;

/**
 * Represents an SQL update statement which can be extended through method chaining.
 *
 * @author &#8904
 */
public class UpdateStatement extends SqlModifyStatement<UpdateStatement, UpdateStatement>
{
    /**
     * Creates a new instance.
     *
     * @param db
     *            The database that should be used for the statement.
     * @param table
     *            The table that should be updated.
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
     * @param column
     *            The column to use in this condition.
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
     * Creates a new conditional clause to chain with an existing where or having clause using the given column for this
     * statement.
     *
     * @param column
     *            The column to use in this condition.
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
     * @param column
     *            The column to use in this condition.
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
     * Sets the value of the given column to <i>null</i>.
     *
     * @param column
     *            The column whichs value should be updated.
     * @param sqlType
     *            The sql value type of the column. This uses standard {@link Types}.
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
     * @param column
     *            The column whichs value should be updated.
     * @param value
     *            The value to update with
     * @param sqlType
     *            The type of the column.
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
     * Uses the given supplier to retrieve a value for the given column when this statement is prepared for execution.
     *
     * @param column
     *            The column whichs value should be set.
     * @param valueSupplier
     *            The supplier that offers a value for the given column.
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
     * @param column
     *            The column whichs value should be updated.
     * @param value
     *            The value to update with
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
     * @param column
     *            The column whichs value should be updated.
     * @param value
     *            The value to update with
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
     * @param column
     *            The column whichs value should be updated.
     * @param value
     *            The value to update with
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
     * @param column
     *            The column whichs value should be updated.
     * @param value
     *            The value to update with
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
     * @param column
     *            The column whichs value should be updated.
     * @param value
     *            The value to update with
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
     * @param column
     *            The column whichs value should be updated.
     * @param value
     *            The value to update with
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
    public int execute()
    {
        return execute(false);
    }

    /**
     * Executes the built statement. Depending on the number of rows affected, the defined onLessThan or onMoreThan
     * might be executed. If there is an error during this execution, the onFail function is called.
     *
     * @see bt.db.statement.SqlModifyStatement#execute(boolean)
     */
    @Override
    public int execute(boolean printLogs)
    {
        String sql = toString();

        if (this.setClauses.isEmpty())
        {
            DatabaseAccess.log.print("Can't execute update statement without any values. Please define at least one column value.");
            return -1;
        }

        int result = Integer.MIN_VALUE;

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql);)
        {
            List<ConditionalClause<UpdateStatement>> valueWhere = this.whereClauses
                                                                                   .stream()
                                                                                   .filter(w -> w.usesValue())
                                                                                   .collect(Collectors.toList());

            log("Executing: " + sql,
                printLogs);

            if (this.prepared)
            {
                if (!this.setClauses.isEmpty())
                {
                    log("With values:",
                        printLogs);
                }
                int i = 0;

                for (; i < this.setClauses.size(); i ++ )
                {
                    SetClause<UpdateStatement> set = this.setClauses.get(i);
                    log("p" + (i + 1) + " = " + set.prepareValue(statement,
                                                                 i + 1),
                        printLogs);
                }

                for (; i < valueWhere.size() + this.setClauses.size(); i ++ )
                {
                    ConditionalClause<UpdateStatement> where = valueWhere.get(i - this.setClauses.size());
                    log("p" + (i + 1) + " = " + where.prepareValue(statement,
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

                if (this.onSuccess != null)
                {
                    this.onSuccess.accept(this, result);
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
                result = this.onFail.apply(this,
                                           e);
            }
            else
            {
                DatabaseAccess.log.print(e);
                result = -1;
            }
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
        String sql = this.statementKeyword + " ";

        sql += this.tables[0];

        if (!this.setClauses.isEmpty())
        {
            sql += " SET ";

            for (SetClause<UpdateStatement> set : this.setClauses)
            {
                sql += set.toString(this.prepared) + ", ";
            }

            sql = sql.substring(0,
                                sql.length() - 2);
        }

        // indicates whether the klast clause was a between clause, to skip the duplicate
        boolean lastBetween = false;

        for (ConditionalClause<UpdateStatement> where : this.whereClauses)
        {
            if (!lastBetween)
            {
                sql += " " + where.toString(this.prepared);

                if (where instanceof BetweenConditionalClause)
                {
                    lastBetween = true;
                }
            }
            else
            {
                lastBetween = false;
            }
        }

        return sql;
    }
}