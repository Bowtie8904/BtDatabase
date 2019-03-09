package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlType;
import bt.db.statement.SqlModifyStatement;
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

    @Override
    public UpdateStatement commit()
    {
        return (UpdateStatement)super.commit();
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#unprepared()
     */
    @Override
    public UpdateStatement unprepared()
    {
        return (UpdateStatement)super.unprepared();
    }

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
        ConditionalClause<UpdateStatement> where = new ConditionalClause<UpdateStatement>(this, column, ConditionalClause.WHERE);
        addWhereClause(where);
        return where;
    }

    /**
     * Creates a new conditional clause to chain with an existing where or having clause using the given column for
     * this statement.
     * 
     * @param column
     *            The column to use in this condition.
     * @return The created ConditionalClause.
     */
    public ConditionalClause<UpdateStatement> and(String column)
    {
        ConditionalClause<UpdateStatement> where = new ConditionalClause<UpdateStatement>(this, column, ConditionalClause.AND);
        addWhereClause(where);
        return where;
    }

    /**
     * Creates a new conditional clause to chain with an existing where or having clause using the given column for
     * this statement.
     * 
     * @param column
     *            The column to use in this condition.
     * @return The created ConditionalClause.
     */
    public ConditionalClause<UpdateStatement> or(String column)
    {
        ConditionalClause<UpdateStatement> where = new ConditionalClause<UpdateStatement>(this, column, ConditionalClause.OR);
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
        return set(column, null, sqlType);
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
        SetClause<UpdateStatement> set = new SetClause<UpdateStatement>(this, column, value, sqlType);
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
        return set(column, value, SqlType.INTEGER);
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
        return set(column, value, SqlType.LONG);
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
        return set(column, value, SqlType.DOUBLE);
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
        return set(column, value, SqlType.FLOAT);
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
        return set(column, value, SqlType.BOOLEAN);
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
        return set(column, value, SqlType.VARCHAR);
    }

    public UpdateStatement onFail(SqlModifyStatement onFail)
    {
        this.onFail = (statement, e) ->
        {
            return onFail.execute();
        };

        return this;
    }

    public UpdateStatement onFail(BiFunction<UpdateStatement, SQLException, Integer> onFail)
    {
        this.onFail = onFail;
        return this;
    }

    public UpdateStatement onLessThan(int lowerThreshhold, SqlModifyStatement statement)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = (i, set) ->
        {
            return statement.execute();
        };
        return this;
    }

    public UpdateStatement onLessThan(int lowerThreshhold, BiFunction<Integer, UpdateStatement, Integer> onLessThan)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = onLessThan;
        return this;
    }

    public UpdateStatement onMoreThan(int higherThreshhold, SqlModifyStatement statement)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = (i, set) ->
        {
            return statement.execute();
        };
        return this;
    }

    public UpdateStatement onMoreThan(int higherThreshhold,
            BiFunction<Integer, UpdateStatement, Integer> onMoreThan)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = onMoreThan;
        return this;
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
            DatabaseAccess.log.print(
                    "Can't execute update statement without any values. Please define at least one column value.");
            return -1;
        }

        int result = Integer.MIN_VALUE;

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql);)
        {
            List<ConditionalClause<UpdateStatement>> valueWhere = this.whereClauses
                    .stream()
                    .filter(w -> w.usesValue())
                    .collect(Collectors.toList());

            log("Executing: " + sql, printLogs);

            if (this.prepared)
            {
                if (!this.setClauses.isEmpty())
                {
                    log("With values:", printLogs);
                }
                int i = 0;

                for (; i < this.setClauses.size(); i ++ )
                {
                    SetClause<UpdateStatement> set = this.setClauses.get(i);
                    log("p" + (i + 1) + " = " + set.prepareValue(statement, i + 1), printLogs);
                }

                for (; i < valueWhere.size() + this.setClauses.size(); i ++ )
                {
                    ConditionalClause<UpdateStatement> where = valueWhere.get(i - this.setClauses.size());
                    log("p" + (i + 1) + " = " + where.prepareValue(statement, i + 1), printLogs);
                }
            }

            result = statement.executeUpdate();
            log("Affected rows: " + result, printLogs);

            if (this.shouldCommit)
            {
                this.db.commit();
            }

            if (result < this.lowerThreshhold && this.onLessThan != null)
            {
                return this.onLessThan.apply(result, this);
            }
            else if (result > this.higherThreshhold && this.onMoreThan != null)
            {
                return this.onMoreThan.apply(result, this);
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

            sql = sql.substring(0, sql.length() - 2);
        }

        for (ConditionalClause<UpdateStatement> where : this.whereClauses)
        {
            sql += " " + where.toString(this.prepared);
        }

        return sql;
    }
}