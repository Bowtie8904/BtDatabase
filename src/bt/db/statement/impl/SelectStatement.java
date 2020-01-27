package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import bt.db.DatabaseAccess;
import bt.db.exc.SqlExecutionException;
import bt.db.statement.SqlModifyStatement;
import bt.db.statement.SqlStatement;
import bt.db.statement.clause.ChainClause;
import bt.db.statement.clause.FromClause;
import bt.db.statement.clause.OrderByClause;
import bt.db.statement.clause.condition.ConditionalClause;
import bt.db.statement.clause.join.JoinClause;
import bt.db.statement.clause.join.JoinConditionalClause;
import bt.db.statement.result.SqlResultSet;
import bt.db.statement.result.StreamableResultSet;
import bt.db.statement.value.Preparable;
import bt.db.statement.value.Value;
import bt.utils.nulls.Null;

/**
 * Represents an SQL select statement which can be extended through method chaining.
 *
 * @author &#8904
 */
public class SelectStatement extends SqlStatement<SelectStatement> implements Preparable
{
    /** Executed if the select returned less rows than specified. */
    private BiFunction<Integer, SqlResultSet, SqlResultSet> onLessThan;

    /** Executed if the select returned more rows than specified. */
    private BiFunction<Integer, SqlResultSet, SqlResultSet> onMoreThan;

    /** A defined method that is called if the statement execution finishes successfully. */
    protected BiConsumer<SelectStatement, SqlResultSet> onSuccess;

    /** A defined function that is called if the statement execution fails for any reason. */
    protected BiFunction<SelectStatement, SqlExecutionException, SqlResultSet> onFail;

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

    /** Indicates how many rows should be skipped from the top of the result set. */
    private int offset = -1;

    /** Contains all joins used in this statement. */
    private List<JoinClause> joins;

    /** Indictaes that * is selected. */
    private boolean selectAll;

    /** Holds all used chain clauses. */
    private List<ChainClause> chains;

    /** An alias used if this select is used as a subselect. */
    private String alias;

    private List<SelectStatement> subSelects;

    private List<FromClause> fromClauses;

    /**
     * Creates a new instance which selects all columns (*) and will log an error message if no rows are returned.
     *
     * @param db
     *            The database that should be used for the statement.
     */
    public SelectStatement(DatabaseAccess db)
    {
        this(db, "*");
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
        this.subSelects = new ArrayList<>();
        this.fromClauses = new ArrayList<>();
        this.columns = columns;

        for (Object col : this.columns)
        {
            if (col.toString().trim().equals("*"))
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

    /**
     * Sets an alias used if this select is a subselect.
     */
    public SelectStatement alias(String alias)
    {
        this.alias = alias;
        return this;
    }

    /**
     * Gets the set alias or null.
     *
     * @return
     */
    public String getAlias()
    {
        return this.alias;
    }

    /**
     * Gets the tables that were specified via {@link #from(String...)}.
     *
     * @return An array of the table names.
     */
    public Object[] getTables()
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
     *            The tables to select from.
     * @return This instance for chaining.
     */
    public SelectStatement from(Object... tables)
    {
        for (Object table : tables)
        {
            if (table instanceof SelectStatement)
            {
                SelectStatement select = (SelectStatement)table;

                if (select.getAlias() == null)
                {
                    DatabaseAccess.log.print(new SQLSyntaxErrorException("Subselects must have an alias assigned."));
                }
            }

            this.fromClauses.add(new FromClause(table));
        }

        return this;
    }

    /**
     * Creates a join with the given table.
     *
     * @param table
     *            The table to join with.
     * @return The created JoinClause.
     */
    public JoinClause join(String table)
    {
        JoinClause join = null;
        try
        {
            FromClause from = this.fromClauses.get(this.fromClauses.size() - 1);
            join = new JoinClause(this,
                                  from.getTableName(),
                                  table);
            this.joins.add(join);

            // add join to last from clause
            from.addJoin(join);
        }
        catch (IndexOutOfBoundsException e)
        {
            DatabaseAccess.log.print("Must define at least one from clause before a join can be performed.");
        }

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
        ConditionalClause<SelectStatement> where = new ConditionalClause<>(this,
                                                                           column,
                                                                           ConditionalClause.WHERE);
        addWhereClause(where);
        this.lastConditionalType = ConditionalClause.WHERE;
        return where;
    }

    /**
     * Creates a new where conditional clause using the given column for this statement.
     *
     * @param column
     *            The column to use in this condition.
     * @param prefix
     *            A String that will be put in front of the expression. Can be used for parenthesis.
     * @return The created ConditionalClause.
     */
    public ConditionalClause<SelectStatement> where(String prefix, String column)
    {
        ConditionalClause<SelectStatement> where = new ConditionalClause<>(this,
                                                                           prefix,
                                                                           column,
                                                                           ConditionalClause.WHERE);
        addWhereClause(where);
        this.lastConditionalType = ConditionalClause.WHERE;
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
    public ConditionalClause<SelectStatement> and(String column)
    {
        ConditionalClause<SelectStatement> clause = new ConditionalClause<>(this,
                                                                            column,
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
     * Creates a new conditional clause to chain with an existing where or having clause using the given column for this
     * statement.
     *
     * @param column
     *            The column to use in this condition.
     * @param prefix
     *            A String that will be put in front of the expression. Can be used for parenthesis.
     * @return The created ConditionalClause.
     */
    public ConditionalClause<SelectStatement> and(String prefix, String column)
    {
        ConditionalClause<SelectStatement> clause = new ConditionalClause<>(this,
                                                                            prefix,
                                                                            column,
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
     * Creates a new conditional clause to chain with an existing where or having clause using the given column for this
     * statement.
     *
     * @param column
     *            The column to use in this condition.
     * @return The created ConditionalClause.
     */
    public ConditionalClause<SelectStatement> or(String column)
    {
        ConditionalClause<SelectStatement> clause = new ConditionalClause<>(this,
                                                                            column,
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
     * Creates a new conditional clause to chain with an existing where or having clause using the given column for this
     * statement.
     *
     * @param column
     *            The column to use in this condition.
     * @param prefix
     *            A String that will be put in front of the expression. Can be used for parenthesis.
     * @return The created ConditionalClause.
     */
    public ConditionalClause<SelectStatement> or(String prefix, String column)
    {
        ConditionalClause<SelectStatement> clause = new ConditionalClause<>(this,
                                                                            prefix,
                                                                            column,
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

        JoinConditionalClause clause = new JoinConditionalClause(this,
                                                                 column,
                                                                 join.getFirstTable(),
                                                                 join.getSecondTable(),
                                                                 ConditionalClause.AND);
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
        JoinConditionalClause clause = new JoinConditionalClause(this,
                                                                 column,
                                                                 join.getFirstTable(),
                                                                 join.getSecondTable(),
                                                                 ConditionalClause.OR);
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

        ConditionalClause<SelectStatement> having = new ConditionalClause<>(this,
                                                                            column,
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
        OrderByClause clause = new OrderByClause(this,
                                                 columns);
        this.orderBy = clause;
        return clause;
    }

    /**
     * Defines the column to order the result by.
     *
     * @param column
     *            The number of the column.
     * @return The created OrderByClause.
     */
    public OrderByClause orderBy(int column)
    {
        return orderBy(Integer.toString(column));
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
     * <p>
     * If an {@link #offset(int) offset} was defined then the first <i>n</i> rows starting from the offset are returned.
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
     * Makes this statement return the rows starting from the offset
     *
     * <p>
     * <i> n <= 0 </i> means all rows will be returned.
     * </p>
     *
     * @param n
     *            The offest, number of rows to skip.
     * @return This instance for chaining.
     */
    public SelectStatement offset(int n)
    {
        this.offset = n;
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
     * Defines a consumer to execute if the statement executes successfully.
     *
     * <p>
     * The first parameter (SelectStatement) will be this statement instance, the second one is the resultset.
     * </p>
     *
     * @param onSuccess
     *            The function to execute.
     * @return This instance for chaining.
     */
    public SelectStatement onSuccess(BiConsumer<SelectStatement, SqlResultSet> onSuccess)
    {
        this.onSuccess = onSuccess;
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
     * The first parameter (SelectStatement) will be this statement instance, the second one is the
     * SqlExecutionException that caused the fail. The return value (SqlResultSet) will be returned by this instances
     * {@link #execute()}.
     * </p>
     *
     * @param onFail
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
    public SelectStatement onFail(BiFunction<SelectStatement, SqlExecutionException, SqlResultSet> onFail)
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
     * Indicates that this statement should be executed as a prepared statement.
     *
     * @return This instance for chaining.
     */
    public SelectStatement prepared()
    {
        this.prepared = true;
        return this;
    }

    /**
     * Combines this select with the given one.
     *
     * <p>
     * The given select will be executed unprepared.
     * </p>
     *
     * <p>
     * The SqlResultSet of this select will contain all rows out of all unioned selects.
     * </p>
     *
     * @param select
     *            The select to append to this one.
     * @return This instance for chaining.
     */
    public SelectStatement unionAll(SelectStatement select)
    {
        createChain(ChainClause.UNION_ALL,
                    select);
        return this;
    }

    /**
     * Combines this select with the given one.
     *
     * <p>
     * The given select will be executed unprepared.
     * </p>
     *
     * <p>
     * The SqlResultSet of this select will only contain unique rows out of all unioned selects.
     * </p>
     *
     * @param select
     *            The select to append to this one.
     * @return This instance for chaining.
     */
    public SelectStatement union(SelectStatement select)
    {
        createChain(ChainClause.UNION,
                    select);
        return this;
    }

    /**
     * Combines this select with the given one.
     *
     * <p>
     * The given select will be executed unprepared.
     * </p>
     *
     * <p>
     * The SqlResultSet of this select will contain all rows that occur in both selects.
     * </p>
     *
     * @param select
     *            The select to append to this one.
     * @return This instance for chaining.
     */
    public SelectStatement intersectAll(SelectStatement select)
    {
        createChain(ChainClause.INTERSECT_ALL,
                    select);
        return this;
    }

    /**
     * Combines this select with the given one.
     *
     * <p>
     * The given select will be executed unprepared.
     * </p>
     *
     * <p>
     * The SqlResultSet of this select will only contain unique rows that occur in both selects.
     * </p>
     *
     * @param select
     *            The select to append to this one.
     * @return This instance for chaining.
     */
    public SelectStatement intersect(SelectStatement select)
    {
        createChain(ChainClause.INTERSECT,
                    select);
        return this;
    }

    /**
     * Combines this select with the given one.
     *
     * <p>
     * The given select will be executed unprepared.
     * </p>
     *
     * <p>
     * The SqlResultSet of this select will contain all rows that occur only in this select and not in the given one.
     * </p>
     *
     * @param select
     *            The select to append to this one.
     * @return This instance for chaining.
     */
    public SelectStatement exceptAll(SelectStatement select)
    {
        createChain(ChainClause.EXCEPT_ALL,
                    select);
        return this;
    }

    /**
     * Combines this select with the given one.
     *
     * <p>
     * The given select will be executed unprepared.
     * </p>
     *
     * <p>
     * The SqlResultSet of this select will only contain unique rows that occur only in this select and not in the given
     * one.
     * </p>
     *
     * @param select
     *            The select to append to this one.
     * @return This instance for chaining.
     */
    public SelectStatement except(SelectStatement select)
    {
        createChain(ChainClause.EXCEPT,
                    select);
        return this;
    }

    /**
     * Creates a new UnionClause and adds it to the list.
     *
     * @param unionType
     *            {@link ChainClause#UNION} or {@link ChainClause#UNION_ALL}.
     * @param select
     *            The select to append.
     */
    private void createChain(String chainType, SelectStatement select)
    {
        if (this.chains == null)
        {
            this.chains = new ArrayList<>();
        }

        this.chains.add(new ChainClause(chainType,
                                        select));
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
        startExecutionTime();
        String sql = toString();
        SqlResultSet result = null;

        try (PreparedStatement statement = this.db.getConnection()
                                                  .prepareStatement(sql,
                                                                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                                    ResultSet.CONCUR_READ_ONLY))
        {
            log("Executing: " + sql,
                printLogs);

            List<String> valueList = new ArrayList<>();

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

                for (int j = 0; j < values.size(); j ++ )
                {
                    val = values.get(j);
                    log("p" + (j + 1) + " [" + val.getType().toString() + "] = " + val.getValue(), printLogs);
                }
            }

            result = new SqlResultSet(statement.executeQuery());
            endExecutionTime();
            result.setSql(sql);
            result.setValues(valueList);

            log("Returned rows: " + result.size(),
                printLogs);

            Null.checkConsume(this.onSuccess, result, (r) -> this.onSuccess.accept(this, r));

            if (result.size() < this.lowerThreshhold && this.onLessThan != null)
            {
                return this.onLessThan.apply(result.size(),
                                             result);
            }
            else if (result.size() > this.higherThreshhold && this.onMoreThan != null)
            {
                return this.onMoreThan.apply(result.size(),
                                             result);
            }
        }
        catch (SQLException e)
        {
            endExecutionTime();
            if (this.onFail != null)
            {
                result = this.onFail.apply(this, new SqlExecutionException(e.getMessage(), sql, e));
            }
            else
            {
                this.db.dispatchException(new SqlExecutionException(e.getMessage(), sql, e));
            }
        }
        endExecutionTime();
        return result;
    }

    public StreamableResultSet executeAsStream()
    {
        return executeAsStream(false);
    }

    public StreamableResultSet executeAsStream(boolean printLogs)
    {
        startExecutionTime();
        String sql = toString();
        StreamableResultSet result = null;

        try
        {
            PreparedStatement statement = this.db.getConnection()
                                                 .prepareStatement(sql,
                                                                   ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                                   ResultSet.CONCUR_READ_ONLY);
            log("Executing: " + sql, printLogs);

            List<String> valueList = new ArrayList<>();

            if (this.prepared)
            {
                List<Value> values = getValues();

                Preparable.prepareStatement(statement, values);

                if (!values.isEmpty())
                {
                    log("With values:", printLogs);
                }

                Value val = null;

                for (int j = 0; j < values.size(); j ++ )
                {
                    val = values.get(j);
                    log("p" + (j + 1) + " [" + val.getType().toString() + "] = " + val.getValue(), printLogs);
                }
            }

            result = new StreamableResultSet(statement.executeQuery(), statement);
            endExecutionTime();

            Null.checkRun(this.onSuccess, () -> this.onSuccess.accept(this, null));
        }
        catch (SQLException e)
        {
            endExecutionTime();
            if (this.onFail != null)
            {
                this.onFail.apply(this, new SqlExecutionException(e.getMessage(), sql, e));
            }
            else
            {
                this.db.dispatchException(new SqlExecutionException(e.getMessage(), sql, e));
            }
        }

        endExecutionTime();
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

        for (Object column : this.columns)
        {
            sql += column + ", ";
        }

        sql = sql.substring(0, sql.length() - 2);

        sql += " FROM ";

        for (FromClause table : this.fromClauses)
        {
            sql += table.toString(this.prepared) + ", ";
        }

        sql = sql.substring(0, sql.length() - 2);

        for (ConditionalClause<SelectStatement> where : this.whereClauses)
        {
            sql += " " + where.toString(this.prepared);
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

        if (this.chains != null)
        {
            for (ChainClause union : this.chains)
            {
                sql += " " + union.toString(this.prepared);
            }
        }

        if (this.orderBy != null)
        {
            sql += " " + this.orderBy.toString();
        }

        if (this.offset > 0)
        {
            if (this.offset == 1)
            {
                sql += " OFFSET 1 ROW";
            }
            else
            {
                sql += " OFFSET " + this.offset + " ROWS";
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

    /**
     * @see bt.db.statement.value.Preparable#getValues()
     */
    @Override
    public List<Value> getValues()
    {
        List<Value> values = new ArrayList<>();

        for (FromClause f : this.fromClauses)
        {
            values.addAll(f.getValues());
        }

        for (ConditionalClause c : this.whereClauses)
        {
            values.addAll(c.getValues());
        }

        for (ConditionalClause h : this.havingClauses)
        {
            values.addAll(h.getValues());
        }

        if (this.chains != null)
        {
            for (ChainClause u : this.chains)
            {
                values.addAll(u.getValues());
            }
        }

        return values;
    }
}