package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import bt.db.DatabaseAccess;
import bt.db.exc.SqlExecutionException;
import bt.db.statement.SqlModifyStatement;
import bt.db.statement.clause.condition.ConditionalClause;
import bt.db.statement.value.Preparable;
import bt.db.statement.value.Value;

/**
 * Represents an SQL delete statement which can be extended through method chaining.
 *
 * @author &#8904
 */
public class DeleteStatement extends SqlModifyStatement<DeleteStatement, DeleteStatement> implements Preparable
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
            log("Executing: " + sql,
                printLogs);


            if (this.prepared)
            {
                List<Value> values = getValues();
                Preparable.prepareStatement(statement, values);

                if (!values.isEmpty())
                {
                    log("With values:",
                        printLogs);
                }

                Value val = null;

                for (int i = 0; i < values.size(); i ++ )
                {
                    val = values.get(i);
                    log("p" + (i + 1) + " = " + val.getValue() + " [" + val.getType().toString() + "]", printLogs);
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

        for (ConditionalClause<DeleteStatement> where : this.whereClauses)
        {
            sql += " " + where.toString(this.prepared);
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