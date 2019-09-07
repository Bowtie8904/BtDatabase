package bt.db.statement.impl;

import java.util.function.BiFunction;

import bt.db.DatabaseAccess;
import bt.db.exc.SqlExecutionException;
import bt.db.statement.SqlModifyStatement;
import bt.db.statement.clause.TableColumn;

/**
 * Base class for all different CREATE statements.
 *
 * @author &#8904
 */
public abstract class CreateStatement<T extends CreateStatement, K extends CreateStatement>
                                     extends SqlModifyStatement<T, K>
{
    /** The name that is used in this create statement. This would be the name of the table, trigger, ... */
    protected String name;

    protected boolean saveObjectData = true;

    /**
     * Creates a new instance and initializes the fields.
     *
     * @param db
     *            The database that should be used for the statement.
     * @param name
     *            The name that is used in this statement. This would be the name of the table, trigger, ...
     */
    public CreateStatement(DatabaseAccess db, String name)
    {
        super(db);
        this.name = name.toUpperCase();
    }

    /**
     * This implementation does nothing and should be overriden by extensions.
     *
     * @param column
     *            The table collumn to add.
     * @return This instance for chaining.
     */
    public CreateStatement<T, K> addColumn(TableColumn column)
    {
        return this;
    }

    /**
     * Return the name that is used in this statement. This would be the name of the table, trigger, ...
     *
     * @return The name.
     */
    public String getName()
    {
        return this.name;
    }

    /**
     * Defines a data modifying statement (statement, update, delete) to execute if the statement tries to create a
     * database object (trigger, procedure, ...) eventhough it already exists.
     *
     * <p>
     * The return value of the given statements execute method will be returned by this instances {@link #execute()}.
     * </p>
     *
     * @param statement
     *            The statement to execute.
     * @return This instance for chaining.
     */
    public T onAlreadyExists(SqlModifyStatement statement)
    {
        this.onAlreadyExists = (s, e) ->
        {
            return statement.execute();
        };
        return (T)this;
    }

    /**
     * Indicates whether this database objects DDL should be saved in 'BT_OBJECT_DATA'.
     *
     * @param saveTableData
     * @return
     */
    public T saveObjectData(boolean saveObjectData)
    {
        this.saveObjectData = saveObjectData;
        return (T)this;
    }

    /**
     * Defines a function that is called if the statement tries to create a database object (trigger, procedure, ...)
     * eventhough it already exists.
     *
     * <p>
     * The first parameter (SqlModifyingStatement) will be this statement instance, the second one is the
     * SqlExecutionException that caused the fail. The return value (Integer) will be returned by this instances
     * {@link #execute()}.
     * </p>
     *
     * @param onForeignKeyViolation
     *            The function to execute.
     * @return This instance for chaining.
     */
    public T onAlreadyExists(BiFunction<T, SqlExecutionException, Integer> onAlreadyExists)
    {
        this.onAlreadyExists = onAlreadyExists;
        return (T)this;
    }
}