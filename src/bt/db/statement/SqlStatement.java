package bt.db.statement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import bt.db.DatabaseAccess;
import bt.db.statement.clause.ConditionalClause;
import bt.db.statement.result.SqlResultSet;

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

    protected BiFunction<T, SQLException, SqlResultSet> onFail;

    protected boolean prepared = true;

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
    protected void addWhereClause(ConditionalClause<T> where)
    {
        this.whereClauses.add(where);
    }

    /**
     * Adds a having conditional clause to this statement.
     * 
     * @param where
     *            The clause.
     */
    protected void addHavingClause(ConditionalClause<T> having)
    {
        this.havingClauses.add(having);
    }

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

    public boolean isPrepared()
    {
        return this.prepared;
    }

    public void setDatabase(DatabaseAccess db)
    {
        this.db = db;
    }
}