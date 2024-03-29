package bt.db.statement;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlState;
import bt.db.exc.SqlExecutionException;
import bt.db.statement.clause.SetClause;
import bt.log.Log;
import bt.utils.Null;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Base class for data modifying statements (statement, update, delete, ...).
 *
 * @author &#8904
 */
public abstract class SqlModifyStatement<T extends SqlModifyStatement, K extends SqlStatement> extends SqlStatement<K>
{
    /**
     * A list containing all used set clauses for statement and update statements.
     */
    protected List<SetClause<T>> setClauses;

    /**
     * A defined method that is called if the statement execution finishes successfully.
     */
    protected BiConsumer<T, Integer> onSuccess;

    /**
     * A defined function that is called if the statement execution fails for any reason.
     */
    protected BiFunction<T, SqlExecutionException, Integer> onFail;

    /**
     * A defined function that is called if the statement execution fails due to a foreign key violation.
     */
    protected BiFunction<T, SqlExecutionException, Integer> onForeignKeyFail;

    /**
     * A defined function that is called if the statement execution fails due to a check constraint violation.
     */
    protected BiFunction<T, SqlExecutionException, Integer> onCheckFail;

    /**
     * A defined function that is called if the statement execution fails due to a duplicate primary key or unique
     * constraint violation.
     */
    protected BiFunction<T, SqlExecutionException, Integer> onDuplicateKey;

    /**
     * A defined function that is called if the number of affected rows is lower than {@link #lowerThreshhold}.
     */
    protected BiFunction<Integer, T, Integer> onLessThan;

    /**
     * A defined function that is called if the number of affected rows is higher than {@link #higherThreshhold}.
     */
    protected BiFunction<Integer, T, Integer> onMoreThan;

    /**
     * A defined function that is called if tthe statement tries to create a database object (trigger, procedure, ...)
     * eventhough it already exists.
     */
    protected BiFunction<T, SqlExecutionException, Integer> onAlreadyExists;

    protected Map<String, BiFunction<T, SqlExecutionException, Integer>> errorHandlers;

    /**
     * The threshhold indicating whether {@link #onLessThan} should be executed.
     */
    protected int lowerThreshhold;

    /**
     * The threshhold indicating whether {@link #onMoreThan} should be executed.
     */
    protected int higherThreshhold;

    /**
     * Indicates whether the transaction should automatically be commited after the execution of this statement.
     */
    protected boolean shouldCommit;

    /**
     * Creates a new instance.
     *
     * @param db The database that should be used for the statement.
     */
    public SqlModifyStatement(DatabaseAccess db)
    {
        super(db);
        this.setClauses = new ArrayList<>();
        this.onFail = this::defaultFail;
        this.errorHandlers = new HashMap<>();
    }

    public T handle(bt.db.constants.SqlState state, BiFunction<T, SqlExecutionException, Integer> handler)
    {
        return handle(state.toString(), handler);
    }

    public T handle(org.apache.derby.shared.common.reference.SQLState state, BiFunction<T, SqlExecutionException, Integer> handler)
    {
        return handle(state.toString(), handler);
    }

    public T handle(String state, BiFunction<T, SqlExecutionException, Integer> handler)
    {
        this.errorHandlers.put(state.toUpperCase(), handler);
        return (T)this;
    }

    private int defaultFail(T statement, SqlExecutionException e)
    {
        Log.error(statement.toString());
        this.db.dispatchException(e);
        return -1;
    }

    /**
     * Adds a set clause to this statement.
     *
     * @param set The value setting clause.
     */
    protected void addSetClause(SetClause<T> set)
    {
        this.setClauses.add(set);
    }

    /**
     * Makes this statement commit changes after SUCCESSFUL execution.
     *
     * @return This instance for chaining.
     */
    public T commit()
    {
        this.shouldCommit = true;
        return (T)this;
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
    public T unprepared()
    {
        this.prepared = false;
        return (T)this;
    }

    /**
     * Defines a consumer to execute if the statement executes successfully.
     *
     * <p>
     * The first parameter (SqlModifyingStatement) will be this statement instance, the second one is number of affected
     * rows.
     * </p>
     *
     * @param onSuccess The function to execute.
     *
     * @return This instance for chaining.
     */
    public T onSuccess(BiConsumer<T, Integer> onSuccess)
    {
        this.onSuccess = onSuccess;
        return (T)this;
    }

    /**
     * Defines a data modifying statement (statement, update, delete, ...) to execute if a check constraint is violated
     * by the statement.
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param statement The statement to execute.
     *
     * @return This instance for chaining.
     */
    public T onCheckViolation(SqlModifyStatement statement)
    {
        this.onCheckFail = (s, e) ->
        {
            return statement.execute();
        };
        return (T)this;
    }

    /**
     * Defines a function to execute if a check constraint is violated by the statement.
     *
     * <p>
     * The first parameter (SqlModifyingStatement) will be this statement instance, the second one is the
     * SqlExecutionException that caused the fail. The return value (Integer) will be returned by this instances
     * {@link #execute()}.
     * </p>
     *
     * @param onCheckViolation The function to execute.
     *
     * @return This instance for chaining.
     */
    public T onCheckViolation(BiFunction<T, SqlExecutionException, Integer> onCheckViolation)
    {
        this.onCheckFail = onCheckViolation;
        return (T)this;
    }

    /**
     * Defines a data modifying statement (statement, update, delete) to execute if a foreign key constraint is violated
     * by the statement.
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param statement The statement to execute.
     *
     * @return This instance for chaining.
     */
    public T onForeignKeyViolation(SqlModifyStatement statement)
    {
        this.onForeignKeyFail = (s, e) ->
        {
            return statement.execute();
        };
        return (T)this;
    }

    /**
     * Defines a function to execute if a foreign key constraint is violated by the statement.
     *
     * <p>
     * The first parameter (SqlModifyingStatement) will be this statement instance, the second one is the
     * SqlExecutionException that caused the fail. The return value (Integer) will be returned by this instances
     * {@link #execute()}.
     * </p>
     *
     * @param onForeignKeyViolation The function to execute.
     *
     * @return This instance for chaining.
     */
    public T onForeignKeyViolation(BiFunction<T, SqlExecutionException, Integer> onForeignKeyViolation)
    {
        this.onForeignKeyFail = onForeignKeyViolation;
        return (T)this;
    }

    /**
     * Defines a data modifying statement (statement, update, delete) to execute if the primary key used in the original
     * statement is already contained in the table or if a unique constraint is violated.
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param statement The statement to execute.
     *
     * @return This instance for chaining.
     */
    public T onDuplicateKey(SqlModifyStatement statement)
    {
        this.onDuplicateKey = (s, e) ->
        {
            return statement.execute();
        };
        return (T)this;
    }

    /**
     * Defines a function that will be executed if the primary key used in the original statement is already contained
     * in the table or if a unique constraint is violated.
     *
     * <p>
     * The first parameter (SqlModifyingStatement) will be this statement instance, the second one is the
     * SqlExecutionException that caused the fail. The return value (Integer) will be returned by this instances
     * {@link #execute()}.
     * </p>
     *
     * @param onDuplicate The function to execute.
     *
     * @return This instance for chaining.
     */
    public T onDuplicateKey(BiFunction<T, SqlExecutionException, Integer> onDuplicate)
    {
        this.onDuplicateKey = onDuplicate;
        return (T)this;
    }

    /**
     * Defines a data modifying statement (statement, update, delete) which will be executed if there was an error
     * during the execution of the original statement statement.
     *
     * <p>
     * The given statement is only executed if the error was not handled by one of the other exception functions
     * (onDuplicateKey, onCheckViolation, ...).
     * </p>
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param onFail The SqlModifyStatement to execute instead.
     *
     * @return This instance for chaining.
     */
    public T onFail(SqlModifyStatement onFail)
    {
        this.onFail = (statement, e) ->
        {
            return onFail.execute();
        };

        return (T)this;
    }

    /**
     * Defines a BiFunction that will be executed if there was an error during the execution of this statement.
     *
     * <p>
     * The first parameter (SqlModifyingStatement) will be this statement instance, the second one is the
     * SqlExecutionException that caused the fail. The return value (Integer) will be returned by this instances
     * {@link #execute()}.
     * </p>
     *
     * <p>
     * The given function is only executed if the error was not handled by one of the other exception functions
     * (onDuplicateKey, onCheckViolation, ...).
     * </p>
     *
     * @param onFail The BiFunction to execute.
     *
     * @return This instance for chaining.
     */
    public T onFail(BiFunction<T, SqlExecutionException, Integer> onFail)
    {
        this.onFail = onFail;
        return (T)this;
    }

    /**
     * Defines a data modifying statement (statement, update, delete) to execute if the original statement affected less
     * rows than the given lower threshhold.
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param lowerThreshhold The threshhold to check.
     * @param statement       The statement to execute.
     *
     * @return This instance for chaining.
     */
    public T onLessThan(int lowerThreshhold, SqlModifyStatement statement)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = (i, set) ->
        {
            return statement.execute();
        };
        return (T)this;
    }

    /**
     * Defines a BiFunction that will be executed if the original statement affected less rows than the given lower
     * threshhold.
     *
     * <p>
     * The first parameter (int) will be the number of rows affected, the second one is the SqlModifyingStatement from
     * the original statement. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param lowerThreshhold The threshhold to check.
     * @param onLessThan      The BiFunction to execute.
     *
     * @return This instance for chaining.
     */
    public T onLessThan(int lowerThreshhold, BiFunction<Integer, T, Integer> onLessThan)
    {
        this.lowerThreshhold = lowerThreshhold;
        this.onLessThan = onLessThan;
        return (T)this;
    }

    /**
     * Defines a data modifying statement (statement, update, delete) to execute if the original statement affected more
     * rows than the given higher threshhold.
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param higherThreshhold The threshhold to check.
     * @param statement        The statement to execute.
     *
     * @return This instance for chaining.
     */
    public T onMoreThan(int higherThreshhold, SqlModifyStatement statement)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = (i, set) ->
        {
            return statement.execute();
        };
        return (T)this;
    }

    /**
     * Defines a BiFunction that will be executed if the original statement affected more rows than the given higher
     * threshhold.
     *
     * <p>
     * The first parameter (int) will be the number of rows affected, the second one is the SqlModifyingStatement from
     * the original statement. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param higherThreshhold The threshhold to check.
     * @param onLessThan       The BiFunction to execute.
     *
     * @return This instance for chaining.
     */
    public T onMoreThan(int higherThreshhold,
                        BiFunction<Integer, T, Integer> onMoreThan)
    {
        this.higherThreshhold = higherThreshhold;
        this.onMoreThan = onMoreThan;
        return (T)this;
    }

    protected int handleFail(SqlExecutionException e)
    {
        endExecutionTime();
        int result = 0;

        if (this.onDuplicateKey != null && e.getSQLState().equals(SqlState.DUPLICATE_KEY.toString()))
        {
            result = this.onDuplicateKey.apply((T)this, e);
        }
        else if (this.onCheckFail != null && e.getSQLState().equals(SqlState.CHECK_CONSTRAINT_VIOLATION.toString()))
        {
            result = this.onCheckFail.apply((T)this, e);
        }
        else if (this.onForeignKeyFail != null && e.getSQLState().equals(SqlState.FOREIGN_KEY_VIOLATION.toString()))
        {
            result = this.onForeignKeyFail.apply((T)this, e);
        }
        else if (this.onAlreadyExists != null &&
                (e.getSQLState().equals(SqlState.ALREADY_EXISTS.toString()) || e.getSQLState().equals(SqlState.ALREADY_EXISTS_IN.toString())))
        {
            result = this.onAlreadyExists.apply((T)this, e);
        }
        else if (this.errorHandlers.get(e.getSQLState()) != null)
        {
            result = dispatchError(e);
        }
        else if (this.onFail != null)
        {
            result = this.onFail.apply((T)this, e);
        }
        else
        {
            this.db.dispatchException(e);
            result = -1;
        }

        return result;
    }

    private int dispatchError(SqlExecutionException e)
    {
        var handler = this.errorHandlers.get(e.getSQLState());

        if (handler != null)
        {
            return handler.apply((T)this, e);
        }

        return -1;
    }

    protected void handleSuccess(int result)
    {
        endExecutionTime();
        Null.checkConsume(this.onSuccess, result, (r) -> this.onSuccess.accept((T)this, r));
    }

    protected int handleThreshholds(int reached)
    {
        endExecutionTime();
        int result = 0;

        if (result < this.lowerThreshhold && this.onLessThan != null)
        {
            result = this.onLessThan.apply(result,
                                           (T)this);
        }
        else if (result > this.higherThreshhold && this.onMoreThan != null)
        {
            result = this.onMoreThan.apply(result,
                                           (T)this);
        }

        return result;
    }

    /**
     * Executes the built statement.
     *
     * @param printLogs true if information about the statement should be printed out.
     *
     * @return The return value of {@link PreparedStatement#executeUpdate()}.
     */
    public int execute()
    {
        startExecutionTime();
        int result = executeStatement();
        endExecutionTime();
        return result;
    }

    /**
     * Executes the built statement.
     *
     * @param printLogs true if information about the statement should be printed out.
     *
     * @return The return value of {@link PreparedStatement#executeUpdate()}.
     */
    protected abstract int executeStatement();
}