package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import bt.db.DatabaseAccess;
import bt.db.exc.SqlExecutionException;
import bt.db.statement.SqlModifyStatement;
import bt.db.statement.clause.BetweenConditionalClause;
import bt.db.statement.clause.condition.ConditionalClause;

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
        ConditionalClause<DeleteStatement> where = new ConditionalClause<>(this,
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
        ConditionalClause<DeleteStatement> where = new ConditionalClause<>(this,
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
        ConditionalClause<DeleteStatement> where = new ConditionalClause<>(this,
                                                                                          column,
                                                                                          ConditionalClause.OR);
        addWhereClause(where);
        return where;
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#execute(boolean)
     */
    @Override
    protected int executeStatement(boolean printLogs)
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
            endExecutionTime();
            log("Affected rows: " + result,
                printLogs);

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