package bt.db.statement;

import java.util.ArrayList;
import java.util.List;

import bt.db.DatabaseAccess;
import bt.db.statement.clause.condition.ConditionalClause;

/**
 * Base class for all SQL statements.
 *
 * @author &#8904
 */
public class SqlStatement<T extends SqlStatement>
{
    /** The database used for the statement. */
    protected DatabaseAccess db;

    /** The used keyword, i.e. insert. */
    protected String statementKeyword;

    /** Used table names. */
    protected String[] tables;

    /** Used column names. */
    protected String[] columns;

    /** All used where conditionals for this statement. */
    protected List<ConditionalClause<T>> whereClauses;

    /** All used having conditionals for this statement. */
    protected List<ConditionalClause<T>> havingClauses;

    /** Indicates whether this statement is treated like a prepared statement or not. */
    protected boolean prepared = true;

    protected long executionTime = -1;

    private long startTime;

    /**
     * Creates a new instance.
     *
     * @param db
     *            The database that should be used for the statement.
     */
    public SqlStatement(DatabaseAccess db)
    {
        this.db = db;
        this.whereClauses = new ArrayList<>();
        this.havingClauses = new ArrayList<>();
        this.tables = new String[] {};
        this.columns = new String[] {};
    }

    /**
     * Adds a where conditional clause to this statement.
     *
     * @param where
     *            The clause.
     */
    public void addWhereClause(ConditionalClause<T> where)
    {
        this.whereClauses.add(where);
    }

    /**
     * Gets a list with all added where clauses.
     *
     * @return The list of where clauses.
     */
    public List<ConditionalClause<T>> getWhereClauses()
    {
        return this.whereClauses;
    }

    /**
     * Adds a having conditional clause to this statement.
     *
     * @param having
     *            The clause.
     */
    public void addHavingClause(ConditionalClause<T> having)
    {
        this.havingClauses.add(having);
    }

    /**
     * Gets a list with all added having clauses.
     *
     * @return The list of having clauses.
     */
    public List<ConditionalClause<T>> getHavingClauses()
    {
        return this.havingClauses;
    }

    /**
     * Logs the given text to the logger instance of {@link DatabaseAccess} if shouldLog is true.
     *
     * @param text
     *            The text to log.
     * @param shouldLog
     *            true if it should be logged, false if it shouldn't be logged.
     */
    protected void log(String text, boolean shouldLog)
    {
        if (shouldLog)
        {
            if (text == null || text.isEmpty())
            {
                DatabaseAccess.log.printEmpty();
            }
            else
            {
                DatabaseAccess.log.setCallerStackIndex(3);
                DatabaseAccess.log.print(text);
                DatabaseAccess.log.setCallerStackIndex(2);
            }
        }
    }

    /**
     * Indicates whether this statement is treated like a prepared statement or not.
     *
     * <p>
     * All statements will be treated as prepared ones unless their unprepared() method is called.
     * </p>
     *
     * @return true = prepared statement, false = unpreparted statement.
     */
    public boolean isPrepared()
    {
        return this.prepared;
    }

    /**
     * Sets the {@link DatabseAccess} instance which should be used to execute this statement.
     *
     * @param db
     *            The database that should be used for the statement.
     */
    public void setDatabase(DatabaseAccess db)
    {
        this.db = db;
    }

    protected void startExecutionTime()
    {
        this.startTime = System.currentTimeMillis();
    }

    protected void endExecutionTime()
    {
        this.executionTime = System.currentTimeMillis() - this.startTime;
    }

    public long getExecutionTime()
    {
        return this.executionTime;
    }

    /**
     * Returns the String representing this statement.
     */
    @Override
    public String toString()
    {
        return "";
    }
}