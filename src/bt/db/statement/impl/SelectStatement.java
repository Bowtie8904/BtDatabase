package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import bt.db.DatabaseAccess;
import bt.db.statement.SqlModifyStatement;
import bt.db.statement.SqlStatement;
import bt.db.statement.clause.ConditionalClause;
import bt.db.statement.clause.JoinConditionalClause;
import bt.db.statement.clause.OrderByClause;
import bt.db.statement.clause.join.InnerJoin;
import bt.db.statement.clause.join.JoinClause;
import bt.db.statement.clause.join.OuterJoin;
import bt.db.statement.result.SqlResultSet;

/**
 * Represents an SQL select statement which can be extended through method chaining.
 * 
 * @author &#8904
 */
public class SelectStatement extends SqlStatement<SelectStatement>
{
    /** Executed if the select returned less rows than specified. */
    private BiFunction<Integer, SqlResultSet, SqlResultSet> onLessThan;

    /** Executed if the select returned more rows than specified. */
    private BiFunction<Integer, SqlResultSet, SqlResultSet> onMoreThan;

    /** Threshhold for {@link #onLessThan} */
    private int lowerThreshhold;

    /** Threshhold for {@link #onMoreThan} */
    private int higherThreshhold;

    /**
     * Indicates which kind of conditional statement (where, having, (join) on, ...) was used last to correctly
     * distribute further conditionals (and, or, ...)
     */
    private String lastConditionalType = ConditionalClause.WHERE;

    /** The order by clause used by this select. */
    private OrderByClause orderBy;

    /** The column names by which this selects result should be grouped. */
    private String[] groupBy;

    /** Indicates how many rows from the first should be returned. -1 = all rows. */
    private int first = -1;

    /** Contains all joins used in this statement. */
    private List<JoinClause> joins;

    /** Indictaes that * is selected. */
    private boolean selectAll;

    /**
     * Creates a new instance which selects all columns (*) and will log an error message if no rows are returned.
     * 
     * @param db
     *            The database that should be used for the statement.
     */
    public SelectStatement(DatabaseAccess db)
    {
        super(db);
        this.joins = new ArrayList<>();
        this.columns = new String[]
        {
                "*"
        };
        this.selectAll = true;
        this.statementKeyword = "SELECT";
        
        this.lowerThreshhold = 1;
        this.onLessThan = (i, set) ->
        {
            DatabaseAccess.log.print("< " + set.getSql() + " > did not return any data.");

            if (set.getValues().size() > 0)
            {
                String values = "Used values:\n";

                for (String value : set.getValues())
                {
                    values += value + "\n";
                }

                DatabaseAccess.log.print(values);
            }

            return set;
        };
    }

    /**
     * Creates a new instance which selects the given columns and will log an error message if no rows are returned.
     * 
     * @param db
     *            The database that should be used for the statement.
     * @param columns
     *            All columns that should be selected.
     */
    public SelectStatement(DatabaseAccess db, String... columns)
    {
        super(db);
        this.joins = new ArrayList<>();
        this.columns = columns;
        
        for (String col : this.columns)
        {
            if (col.trim().equals("*"))
            {
                this.selectAll = true;
            }
        }
        
        this.statementKeyword = "SELECT";

        this.lowerThreshhold = 1;
        this.onLessThan = (i, set) ->
        {
            DatabaseAccess.log.print("< " + set.getSql() + " > did not return any data.");

            if (set.getValues().size() > 0)
            {
                String values = "Used values:\n";

                for (String value : set.getValues())
                {
                    values += value + "\n";
                }

                DatabaseAccess.log.print(values);
            }

            return set;
        };
    }

    public String[] getTables()
    {
        return this.tables;
    }

    /**
     * Changes this statement to select distinct rows.
     * 
     * @return This instance for chaining.
     */
    public SelectStatement distinct()
    {
        if (this.selectAll)
        {
            try
            {
                throw new SQLSyntaxErrorException("Can't use distinct keyword when all columns are selected.");
            }
            catch (SQLSyntaxErrorException e)
            {
                DatabaseAccess.log.print(e);
                return this;
            }
        }

        this.statementKeyword = "SELECT DISTINCT";
        return this;
    }

    /**
     * Defines the tables to select from.
     * 
     * @param tables
     *            The tables to select from
     * @return This instance for chaining.
     */
    public SelectStatement from(String... tables)
    {
        this.tables = tables;
        return this;
    }

    /**
     * Creates an inner join with the given table.
     * 
     * @param table
     *            The table to join with.
     * @return The created InnerJoin.
     */
    public InnerJoin join(String table)
    {
        InnerJoin join = new InnerJoin(this, this.tables[this.tables.length - 1], table);
        this.joins.add(join);
        return join;
    }

    /**
     * Creates a left outer join with the given table.
     * 
     * @param table
     *            The table to join with.
     * @return The created OuterJoin.
     */
    public OuterJoin leftJoin(String table)
    {
        OuterJoin join = new OuterJoin(this, this.tables[this.tables.length - 1], table, true);
        this.joins.add(join);
        return join;
    }

    /**
     * Creates a right outer join with the given table.
     * 
     * @param table
     *            The table to join with.
     * @return The created OuterJoin.
     */
    public OuterJoin rightJoin(String table)
    {
        OuterJoin join = new OuterJoin(this, this.tables[this.tables.length - 1], table, false);
        this.joins.add(join);
        return join;
    }

    /**
     * Creates a new where conditional clause using the given column for this statement.
     * 
     * @param column
     *            The column to use in this condition.
     * @return The created ConditionalClause.
     */
    public ConditionalClause<SelectStatement> where(String column)
    {
        ConditionalClause<SelectStatement> where = new ConditionalClause<SelectStatement>(this, column, ConditionalClause.WHERE);
        addWhereClause(where);
        this.lastConditionalType = ConditionalClause.WHERE;
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
    public ConditionalClause<SelectStatement> and(String column)
    {
        ConditionalClause<SelectStatement> clause = new ConditionalClause<SelectStatement>(this, column,
                ConditionalClause.AND);

        if (this.lastConditionalType.equals(ConditionalClause.WHERE))
        {
            addWhereClause(clause);
        }
        else if (this.lastConditionalType.equals(ConditionalClause.HAVING))
        {
            addHavingClause(clause);
        }

        return clause;
    }

    /**
     * Creates a new conditional clause to chain with an existing where or having clause using the given column for
     * this statement.
     * 
     * @param column
     *            The column to use in this condition.
     * @return The created ConditionalClause.
     */
    public ConditionalClause<SelectStatement> or(String column)
    {
        ConditionalClause<SelectStatement> clause = new ConditionalClause<SelectStatement>(this, column,
                ConditionalClause.OR);

        if (this.lastConditionalType.equals(ConditionalClause.WHERE))
        {
            addWhereClause(clause);
        }
        else if (this.lastConditionalType.equals(ConditionalClause.HAVING))
        {
            addHavingClause(clause);
        }

        return clause;
    }

    /**
     * Creates a new join conditional clause to chain with an existing (join) on clause using the given column for this
     * statement.
     * 
     * @param column
     *            The column to use in this condition.
     * @return The created JoinConditionalClause.
     */
    public JoinConditionalClause andOn(String column)
    {
        JoinClause join = this.joins.get(this.joins.size() - 1);

        JoinConditionalClause clause = new JoinConditionalClause(this, column, join.getFirstTable(),
                join.getSecondTable(), ConditionalClause.AND);
        join.addConditionalClause(clause);

        return clause;
    }

    /**
     * Creates a new join conditional clause to chain with an existing (join) on clause using the given column for this
     * statement.
     * 
     * @param column
     *            The column to use in this condition.
     * @return The created JoinConditionalClause.
     */
    public JoinConditionalClause orOn(String column)
    {
        JoinClause join = this.joins.get(this.joins.size() - 1);
        JoinConditionalClause clause = new JoinConditionalClause(this, column, join.getFirstTable(),
                join.getSecondTable(), ConditionalClause.OR);
        join.addConditionalClause(clause);

        return clause;
    }

    /**
     * Creates a new having conditional clause using the given column for this statement.
     * 
     * A group by expression must be before this call.
     * 
     * @param column
     *            The column to use in this condition.
     * @return The created ConditionalClause.
     */
    public ConditionalClause<SelectStatement> having(String column)
    {
        if (this.groupBy == null)
        {
            try
            {
                throw new SQLSyntaxErrorException("Must define a group by expression before using a having clause.");
            }
            catch (SQLSyntaxErrorException e)
            {
                DatabaseAccess.log.print(e);
                return null;
            }
        }

        ConditionalClause<SelectStatement> having = new ConditionalClause<SelectStatement>(this, column,
                ConditionalClause.HAVING);
        addHavingClause(having);
        this.lastConditionalType = ConditionalClause.HAVING;
        return having;
    }

    /**
     * Defines the columns to order the result by.
     * 
     * @param columns
     *            The columns to order by.
     * @return The created OrderByClause.
     */
    public OrderByClause orderBy(String... columns)
    {
        OrderByClause clause = new OrderByClause(this, columns);
        this.orderBy = clause;
        return clause;
    }

    /**
     * Makes this statement only return the very first row.
     * 
     * @return This instance for chaining.
     */
    public SelectStatement first()
    {
        this.first = 1;
        return this;
    }

    /**
     * Makes this statement return the first <i>n</i> rows.
     * 
     * <p>
     * <i> n <= 0 </i> means all rows will be returned.
     * </p>
     * 
     * @param n
     *            The number of rows to return.
     * @return This instance for chaining.
     */
    public SelectStatement first(int n)
    {
        this.first = n;
        return this;
    }

    /**
     * Defines the columns to group by.
     * 
     * @param columns
     *            The columns to group by.
     * @return This instance for chaining.
     */
    public SelectStatement groupBy(String... columns)
    {
        this.groupBy = columns;
        return this;
    }

    /**
     * Defines a select statement which will be executed (and whichs resultset will be returned by {@link #execute()})
     * if the original select returned less rows than the given lower threshhold.
     * 
     * @param lowerThreshhold
     *            The threshhold to check.
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public SelectStatement onLessThan(int lowerThreshhold, SelectStatement statement)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = (i, set) ->
        {
            return statement.execute();
        };
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) to execute if the original select returned less rows
     * than the given lower threshhold.
     * 
     * @param lowerThreshhold
     *            The threshhold to check.
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public SelectStatement onLessThan(int lowerThreshhold, SqlModifyStatement statement)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = (i, set) ->
        {
            statement.execute();
            return set;
        };
        return this;
    }

    /**
     * Defines a BiFunction that will be executed if the original select returned less rows than the given lower
     * threshhold.
     * 
     * <p>
     * The first parameter (int) will be the number of rows returned, the second one is the SqlResultSet from the
     * original select. The return value (SqlResultSet) will be returned by this instances {@link #execute()}.
     * </p>
     * 
     * @param lowerThreshhold
     *            The threshhold to check.
     * @param onLessThan
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
    public SelectStatement onLessThan(int lowerThreshhold, BiFunction<Integer, SqlResultSet, SqlResultSet> onLessThan)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = onLessThan;
        return this;
    }

    /**
     * Defines a select statement which will be executed (and whichs resultset will be returned by {@link #execute()})
     * if the original select returned more rows than the given higher threshhold.
     * 
     * @param higherThreshhold
     *            The threshhold to check.
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public SelectStatement onMoreThan(int higherThreshhold, SelectStatement statement)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = (i, set) ->
        {
            return statement.execute();
        };
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete) to execute if the original select returned more rows
     * than the given higher threshhold.
     * 
     * @param higherThreshhold
     *            The threshhold to check.
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public SelectStatement onMoreThan(int higherThreshhold, SqlModifyStatement statement)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = (i, set) ->
        {
            statement.execute();
            return set;
        };
        return this;
    }

    /**
     * Defines a BiFunction that will be executed if the original select returned more rows than the given higher
     * threshhold.
     * 
     * <p>
     * The first parameter (int) will be the number of rows returned, the second one is the SqlResultSet from the
     * original select. The return value (SqlResultSet) will be returned by this instances {@link #execute()}.
     * </p>
     * 
     * @param higherThreshhold
     *            The threshhold to check.
     * @param onLessThan
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
    public SelectStatement onMoreThan(int higherThreshhold, BiFunction<Integer, SqlResultSet, SqlResultSet> onMoreThan)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = onMoreThan;
        return this;
    }

    /**
     * Sets the last used conditional type to correctly distribute chaining conditionals (and, or, ...).
     * 
     * <p>
     * This method is called automatically and should not be used during the actual creation of a select statement.
     * </p>
     * 
     * @param type
     *            The conditional type (where, having, on, ...).
     */
    public void setLastConditionalType(String type)
    {
        this.lastConditionalType = type;
    }

    /**
     * Defines a SelectStatement which will be executed and whichs SqlResultSet will be returned if there was an error
     * during the execution of the original select.
     * 
     * @param onFail
     *            The SelectStatement to execute instead.
     * @return This instance for chaining.
     */
    public SelectStatement onFail(SelectStatement onFail)
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
     * The first parameter (SelectStatement) will be this statement instance, the second one is the SqlReSQLExceptiont
     * that caused the fail. The return value (SqlResultSet) will be returned by this instances {@link #execute()}.
     * </p>
     * 
     * @param onFail
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
    public SelectStatement onFail(BiFunction<SelectStatement, SQLException, SqlResultSet> onFail)
    {
        this.onFail = onFail;
        return this;
    }

    /**
     * Indicates that this statement should not be executed as a prepared statement. Instead all set values will be
     * directly inserted into the raw sql string.
     * 
     * <p>
     * <b>Note that using this method makes the statement vulnerable for sql injections.</b>
     * </p>
     * 
     * @return This instance for chaining.
     */
    public SelectStatement unprepared()
    {
        this.prepared = false;
        return this;
    }

    /**
     * Executes the select and returns the resultset. Depending on the number of rows returned, the defined onLessThan
     * or onMoreThan might be executed.
     * 
     * @return The result.
     */
    public SqlResultSet execute()
    {
        return execute(false);
    }

    /**
     * Executes the select and returns the resultset. Depending on the number of rows returned, the defined onLessThan
     * or onMoreThan might be executed. If there is an error during this execution, the onFail function is called.
     * 
     * @param printLogs
     *            true if information such as the full statement and paramaters should be printed.
     * @return The result.
     */
    public SqlResultSet execute(boolean printLogs)
    {
        String sql = toString();
        SqlResultSet result = null;

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY))
        {
            log("Executing: " + sql, printLogs);

            List<String> valueList = new ArrayList<>();

            if (this.prepared)
            {
                String values = "";

                int i = 0;

                List<JoinConditionalClause> valueJoin = new ArrayList<>();

                for (JoinClause join : this.joins)
                {
                    valueJoin.addAll(join.getConditionalClauses());
                }

                valueJoin = valueJoin
                        .stream()
                        .filter(j -> j.usesValue())
                        .collect(Collectors.toList());

                for (; i < valueJoin.size(); i ++ )
                {
                    JoinConditionalClause clause = valueJoin.get(i);
                    String value = "p" + (i + 1) + " = " + clause.prepareValue(statement, i + 1);
                    values += value + "\n";
                    valueList.add(value);
                }

                int offset = valueJoin.size();

                List<ConditionalClause<SelectStatement>> valueWhere = this.whereClauses
                        .stream()
                        .filter(w -> w.usesValue())
                        .collect(Collectors.toList());

                for (; i < valueWhere.size() + offset; i ++ )
                {
                    ConditionalClause<SelectStatement> where = valueWhere.get(i - offset);
                    String value = "p" + (i + 1) + " = " + where.prepareValue(statement, i + 1);
                    values += value + "\n";
                    valueList.add(value);
                }

                offset += valueWhere.size();

                List<ConditionalClause<SelectStatement>> valueHaving = this.havingClauses
                        .stream()
                        .filter(h -> h.usesValue())
                        .collect(Collectors.toList());

                for (; i < valueHaving.size() + offset; i ++ )
                {
                    ConditionalClause<SelectStatement> having = valueHaving.get(i - offset);
                    String value = "p" + (i + 1) + " = " + having.prepareValue(statement, i + 1);
                    values += value + "\n";
                    valueList.add(value);
                }

                if (!values.isEmpty())
                {
                    log("With values:", printLogs);
                    log(values, printLogs);
                }

            }

            result = new SqlResultSet(statement.executeQuery());
            result.setSql(sql);
            result.setValues(valueList);

            log("Returned rows: " + result.size(), printLogs);

            if (result.size() < this.lowerThreshhold && this.onLessThan != null)
            {
                return this.onLessThan.apply(result.size(), result);
            }
            else if (result.size() > this.higherThreshhold && this.onMoreThan != null)
            {
                return this.onMoreThan.apply(result.size(), result);
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

        for (String column : this.columns)
        {
            sql += column + ", ";
        }

        sql = sql.substring(0, sql.length() - 2);

        sql += " FROM ";

        for (String table : this.tables)
        {
            sql += table + ", ";
        }

        sql = sql.substring(0, sql.length() - 2);

        for (JoinClause join : this.joins)
        {
            sql += " " + join.toString(this.prepared);
        }

        for (ConditionalClause<SelectStatement> where : this.whereClauses)
        {
            sql += " " + where.toString(this.prepared);
        }

        if (this.orderBy != null)
        {
            sql += " " + this.orderBy.toString();
        }

        if (this.groupBy != null)
        {
            sql += " GROUP BY ";

            for (String column : this.groupBy)
            {
                sql += column + ", ";
            }

            sql = sql.substring(0, sql.length() - 2);

            for (ConditionalClause<SelectStatement> having : this.havingClauses)
            {
                sql += " " + having.toString(this.prepared);
            }
        }

        if (this.first > 0)
        {
            if (this.first == 1)
            {
                sql += " FETCH FIRST ROW ONLY";
            }
            else
            {
                sql += " FETCH FIRST " + this.first + " ROWS ONLY";
            }
        }

        return sql;
    }
}