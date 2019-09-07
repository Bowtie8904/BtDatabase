package bt.db.statement.clause.foreign;

import bt.db.constants.Delete;
import bt.db.constants.Update;
import bt.db.statement.impl.CreateStatement;

/**
 * The super class for foreign keys.
 *
 * @author &#8904
 */
public class ForeignKey<T extends ForeignKey, K extends CreateStatement>
{
    /** The name of this foreign key. */
    protected String name;

    /** The name of the referenced table. */
    protected String table;

    /** The used columns in the referenced table. */
    protected String[] parentColumns;

    /** The used columns in the table that the foreign key is created in. */
    protected String[] childColumns;

    /** The on delete behavior. */
    protected Delete onDelete;

    /** The on update behavior. */
    protected Update onUpdate;

    /** The statement that this foreign key is created from. */
    protected K statement;

    /**
     * Creates a new instance.
     *
     * @param statement
     *            The calling statement.
     */
    public ForeignKey(K statement)
    {
        this.statement = statement;
    }

    /**
     * Creates a new instance.
     *
     * @param statement
     *            The calling statement.
     * @param childColumns
     *            The columns in the table that this foreign key is created from.
     */
    public ForeignKey(K statement, String... childColumns)
    {
        this(statement);
        this.childColumns = childColumns;
    }

    /**
     * Sets a specific name for this foreign key.
     *
     * <p>
     * If this is not called, a name will be generated.
     * </p>
     *
     * @param name
     *            The name of the key.
     * @return This instance for chaining.
     */
    public T name(String name)
    {
        this.name = name;
        return (T)this;
    }

    /**
     * Defines the referenced table and its columns.
     *
     * @param table
     *            The referenced table.
     * @param columns
     *            The columns in the referenced table that are part of the foreign key.
     * @return This instance for chaining.
     */
    public T references(String table, String... columns)
    {
        this.table = table;
        this.parentColumns = columns;
        return (T)this;
    }

    /**
     * Defines on delete behavior.
     *
     * @param onDelete
     *            The behavior.
     * @return This instance for chaining.
     */
    public T on(Delete onDelete)
    {
        this.onDelete = onDelete;
        return (T)this;
    }

    /**
     * Defines on update behavior.
     *
     * @param onUpdate
     *            The behavior.
     * @return This instance for chaining.
     */
    public T on(Update onUpdate)
    {
        this.onUpdate = onUpdate;
        return (T)this;
    }

    /**
     * Defines the delete and update behavior.
     *
     * @param onDelete
     *            The delete behavior.
     * @param onUpdate
     *            The update behavior.
     * @return This instance for chaining.
     */
    public T on(Delete onDelete, Update onUpdate)
    {
        this.onDelete = onDelete;
        this.onUpdate = onUpdate;
        return (T)this;
    }

    /**
     * Defines the delete and update behavior.
     *
     * @param onDelete
     *            The delete behavior.
     * @param onUpdate
     *            The update behavior.
     * @return This instance for chaining.
     */
    public T on(Update onUpdate, Delete onDelete)
    {
        this.onUpdate = onUpdate;
        this.onDelete = onDelete;
        return (T)this;
    }

    /**
     * Finishes this foreign key and returns to the calling statement.
     *
     * @return The statement that created this foreign key.
     */
    public K add()
    {
        return this.statement;
    }
}