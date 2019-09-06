package bt.db.statement.impl;

import bt.db.DatabaseAccess;
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
}