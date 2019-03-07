package bt.db.statement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import bt.db.DatabaseAccess;
import bt.db.statement.clause.SetClause;

/**
 * Base class for data modifying statements (insert, update, delete).
 * 
 * @author &#8904
 */
public abstract class SqlModifyStatement<T extends SqlModifyStatement, K extends SqlStatement> extends SqlStatement<K>
{
    /** The errorcode of the exception that occours when a duplicate key is inserted into a table. */
    protected static final String DUPLICATE_KEY_ERROR = "23505";

    /**
     * The errorcode of the exception that occours when the program tries to create a database object (trigger,
     * procedure, ...) eventhough it already exists.
     */
    protected static final String ALREADY_EXISTS_ERROR = "X0Y32";

    /** A list containing all used set clauses for insert and update statements. */
    protected List<SetClause<T>> setClauses;

    protected BiFunction<T, SQLException, Integer> onFail;

    protected BiFunction<Integer, T, Integer> onLessThan;

    protected BiFunction<Integer, T, Integer> onMoreThan;

    protected int lowerThreshhold, higherThreshhold;

    protected boolean shouldCommit;

    /**
     * Creates a new instance.
     * 
     * @param db
     *            The database that should be used for the statement.
     */
    public SqlModifyStatement(DatabaseAccess db)
    {
        super(db);
        this.setClauses = new ArrayList<>();
    }

    /**
     * Adds a set clause to this statement.
     * 
     * @param set
     *            The value setting clause.
     */
    protected void addSetClause(SetClause<T> set)
    {
        this.setClauses.add(set);
    }

    /**
     * Makes this statement commit changes after SUCCESSFUL execution.
     * 
     * @return
     */
    public SqlModifyStatement<T, K> commit()
    {
        this.shouldCommit = true;
        return this;
    }

    public SqlModifyStatement<T, K> unprepared()
    {
        this.prepared = false;
        return this;
    }

    /**
     * Executes the built statement.
     * 
     * @return The return value of {@link PreparedStatement#executeUpdate()}.
     */
    public abstract int execute();

    /**
     * Executes the built statement.
     * 
     * @param printLogs
     *            true if information about the statement should be printed out.
     * @return The return value of {@link PreparedStatement#executeUpdate()}.
     */
    public abstract int execute(boolean printLogs);
}