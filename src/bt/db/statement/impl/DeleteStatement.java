package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import bt.db.DatabaseAccess;
import bt.db.statement.SqlModifyStatement;
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

    @Override
    public DeleteStatement commit()
    {
        return (DeleteStatement)super.commit();
    }

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
        ConditionalClause<DeleteStatement> where = new ConditionalClause<DeleteStatement>(this, column, ConditionalClause.WHERE);
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
    public ConditionalClause<DeleteStatement> and(String column)
    {
        ConditionalClause<DeleteStatement> where = new ConditionalClause<DeleteStatement>(this, column, ConditionalClause.AND);
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
    public ConditionalClause<DeleteStatement> or(String column)
    {
        ConditionalClause<DeleteStatement> where = new ConditionalClause<DeleteStatement>(this, column, ConditionalClause.OR);
        addWhereClause(where);
        return where;
    }

    public DeleteStatement onFail(SqlModifyStatement onFail)
    {
        this.onFail = (statement, e) ->
        {
            return onFail.execute();
        };

        return this;
    }

    public DeleteStatement onFail(BiFunction<DeleteStatement, SQLException, Integer> onFail)
    {
        this.onFail = onFail;
        return this;
    }

    public DeleteStatement onLessThan(int lowerThreshhold, SqlModifyStatement statement)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = (i, set) ->
        {
            return statement.execute();
        };
        return this;
    }

    public DeleteStatement onLessThan(int lowerThreshhold, BiFunction<Integer, DeleteStatement, Integer> onLessThan)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = onLessThan;
        return this;
    }

    public DeleteStatement onMoreThan(int higherThreshhold, SqlModifyStatement statement)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = (i, set) ->
        {
            return statement.execute();
        };
        return this;
    }

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

            log("Executing: " + sql, printLogs);

            if (this.prepared)
            {
                if (!valueWhere.isEmpty())
                {
                    log("With values:", printLogs);
                }

                for (int i = 0; i < valueWhere.size(); i ++ )
                {
                    ConditionalClause<DeleteStatement> where = valueWhere.get(i);
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
     * Formats the full delete statement without instering values.
     * 
     * @see java.lang.Object#toString()
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

        for (ConditionalClause<DeleteStatement> where : this.whereClauses)
        {
            sql += " " + where.toString(this.prepared);
        }

        return sql;
    }
}