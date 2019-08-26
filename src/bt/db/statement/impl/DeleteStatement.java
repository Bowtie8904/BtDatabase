package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import bt.db.DatabaseAccess;
import bt.db.statement.SqlModifyStatement;
import bt.db.statement.clause.BetweenConditionalClause;
import bt.db.statement.clause.ConditionalClause;

/**
 * Represents an SQL delete statement which can be extended through method chaining.
 * 
 * @author &#8904
 */
public class DeleteStatement extends SqlModifyStatement<DeleteStatement, DeleteStatement>
{
    /**
     * Creates a new instance.
     * 
     * @param db
     *            The database that should be used for the statement.
     */
    public DeleteStatement(DatabaseAccess db)
    {
        super(db);
        this.statementKeyword = "DELETE";
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#commit()
     */
    @Override
    public DeleteStatement commit()
    {
        return (DeleteStatement)super.commit();
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#unprepared()
     */
    @Override
    public DeleteStatement unprepared()
    {
        return (DeleteStatement)super.unprepared();
    }

    /**
     * Defines the table to delete from.
     * 
     * @param table
     *            The table name.
     * @return This instance for chaining.
     */
    public DeleteStatement from(String table)
    {
        this.tables = new String[]
            {
              table.toUpperCase()
            };
        return this;
    }

    /**
     * Gets the table from which this statement will delete.
     * 
     * @return The name of the table or null if none has been set yet via {@link #from(String)}.
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
    public ConditionalClause<DeleteStatement> where(String column)
    {
        ConditionalClause<DeleteStatement> where = new ConditionalClause<DeleteStatement>(this,
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
    public ConditionalClause<DeleteStatement> and(String column)
    {
        ConditionalClause<DeleteStatement> where = new ConditionalClause<DeleteStatement>(this,
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
    public ConditionalClause<DeleteStatement> or(String column)
    {
        ConditionalClause<DeleteStatement> where = new ConditionalClause<DeleteStatement>(this,
                                                                                          column,
                                                                                          ConditionalClause.OR);
        addWhereClause(where);
        return where;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) which will be executed if there was an error during
     * the execution of the original delete statement.
     * 
     * @param onFail
     *            The SqlModifyStatement to execute instead.
     * @return This instance for chaining.
     */
    public DeleteStatement onFail(SqlModifyStatement onFail)
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
     * The first parameter (DeleteStatement) will be this statement instance, the second one is the SQLException that
     * caused the fail. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     * 
     * @param onFail
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
    public DeleteStatement onFail(BiFunction<DeleteStatement, SQLException, Integer> onFail)
    {
        this.onFail = onFail;
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) to execute if the original delete affected less rows
     * than the given lower threshhold.
     * 
     * @param lowerThreshhold
     *            The threshhold to check.
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public DeleteStatement onLessThan(int lowerThreshhold, SqlModifyStatement statement)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = (i, set) ->
            {
                return statement.execute();
            };
        return this;
    }

    /**
     * Defines a BiFunction that will be executed if the original delete affected less rows than the given lower
     * threshhold.
     * 
     * <p>
     * The first parameter (int) will be the number of rows affected, the second one is the DeleteStatement from the
     * original delete. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     * 
     * @param lowerThreshhold
     *            The threshhold to check.
     * @param onLessThan
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
    public DeleteStatement onLessThan(int lowerThreshhold, BiFunction<Integer, DeleteStatement, Integer> onLessThan)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = onLessThan;
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) to execute if the original delete affected more rows
     * than the given higher threshhold.
     * 
     * @param higherThreshhold
     *            The threshhold to check.
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public DeleteStatement onMoreThan(int higherThreshhold, SqlModifyStatement statement)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = (i, set) ->
            {
                return statement.execute();
            };
        return this;
    }

    /**
     * Defines a BiFunction that will be executed if the original delete affected more rows than the given higher
     * threshhold.
     * 
     * <p>
     * The first parameter (int) will be the number of rows affected, the second one is the DeleteStatement from the
     * original insert. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     * 
     * @param higherThreshhold
     *            The threshhold to check.
     * @param onLessThan
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
    public DeleteStatement onMoreThan(int higherThreshhold,
                                      BiFunction<Integer, DeleteStatement, Integer> onMoreThan)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = onMoreThan;
        return this;
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#execute()
     */
    @Override
    public int execute()
    {
        return execute(false);
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#execute(boolean)
     */
    @Override
    public int execute(boolean printLogs)
    {
        String sql = toString();

        int result = Integer.MIN_VALUE;

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
        {
            List<ConditionalClause<DeleteStatement>> valueWhere = this.whereClauses
                                                                                   .stream()
                                                                                   .filter(w -> w.usesValue())
                                                                                   .collect(Collectors.toList());

            log("Executing: " + sql,
                printLogs);

            if (this.prepared)
            {
                if (!valueWhere.isEmpty())
                {
                    log("With values:",
                        printLogs);
                }

                for (int i = 0; i < valueWhere.size(); i ++ )
                {
                    ConditionalClause<DeleteStatement> where = valueWhere.get(i);
                    log("p" + (i + 1) + " = " + where.prepareValue(statement,
                                                                   i + 1),
                        printLogs);
                }
            }

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
     * @see bt.db.statement.SqlStatement#toString()
     */
    @Override
    public String toString()
    {
        String sql = this.statementKeyword;

        sql += " FROM ";

        for (String table : this.tables)
        {
            sql += table;
        }

        // indicates whether the last clause was a between clause, to skip the duplicate
        boolean lastBetween = false;

        for (ConditionalClause<DeleteStatement> where : this.whereClauses)
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