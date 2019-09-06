package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

import bt.db.DatabaseAccess;
import bt.db.statement.clause.TriggerAction;

/**
 * Represents an SQL CREATE TRIGGER statement which can be extended through method chaining.
 *
 * @author &#8904
 */
public class CreateTriggerStatement extends CreateStatement<CreateTriggerStatement, CreateTriggerStatement>
{
    private String when;
    private String triggerKeyword;
    private String oldAlias, newAlias;
    private boolean forEachRow = true;
    private TriggerAction triggerAction;
    private boolean replace;

    /**
     * Creates a new instance.
     *
     * @param db
     *            The database on which the trigger will be created.
     * @param name
     *            The name of the trigger.
     */
    public CreateTriggerStatement(DatabaseAccess db, String name)
    {
        super(db,
              name);
        this.statementKeyword = "CREATE TRIGGER";
    }

    /**
     * Indicates that this trigger should be called AFTER an action on the table.
     *
     * <p>
     * Actions are:
     * <ul>
     * <li>insert</li>
     * <li>update</li>
     * <li>delete</li>
     * </ul>
     * </p>
     *
     * @param action
     *            The action which should cause this trigger to be executed. Either 'insert', 'update' or 'delete'.
     * @return This instance for chaining.
     */
    public CreateTriggerStatement after(String action)
    {
        this.triggerKeyword = action.toUpperCase();
        this.when = "AFTER";
        return this;
    }

    /**
     * Indicates that this trigger should be called BEFORE an action on the table.
     *
     * <p>
     * Actions are:
     * <ul>
     * <li>insert</li>
     * <li>update</li>
     * <li>delete</li>
     * </ul>
     * </p>
     *
     * @param action
     *            The action which should cause this trigger to be executed. Either 'insert', 'update' or 'delete'.
     * @return This instance for chaining.
     */
    public CreateTriggerStatement before(String action)
    {
        this.triggerKeyword = action.toUpperCase();
        this.when = "NO CASCADE BEFORE";
        return this;
    }

    /**
     * Only usable for UPDATE triggers.
     *
     * <p>
     * Defines the columns that will cause this trigger to be executed when they are updated.
     * </p>
     *
     * <p>
     * Not calling this method to specify columns means that all columns will cause a trigger execution on update.
     * </p>
     *
     * @param columns
     *            The names of the columns.
     * @return This instance for chaining.
     */
    public CreateTriggerStatement of(String... columns)
    {
        if (!this.triggerKeyword.toUpperCase().equals("UPDATE"))
        {
            try
            {
                throw new SQLSyntaxErrorException("Can't define trigger columns on non UPDATE triggers.");
            }
            catch (SQLSyntaxErrorException e)
            {
                DatabaseAccess.log.print(e);
                return this;
            }
        }

        this.columns = columns;
        return this;
    }

    /**
     * Sets the table that this trigger should listen on.
     *
     * @param table
     *            The name of the table.
     * @return This instance for chaining.
     */
    public CreateTriggerStatement on(String table)
    {
        this.tables = new String[]
        {
          table
        };

        return this;
    }

    /**
     * Only usable for INSERT and UPDATE triggers.
     *
     * <p>
     * Sets an alias for the newly inserted/updated row.
     * </p>
     *
     * @param alias
     *            The alias of the row.
     * @return This instance for chaining.
     */
    public CreateTriggerStatement newAs(String alias)
    {
        this.newAlias = alias;
        return this;
    }

    /**
     * Only usable for DELETE and UPDATE triggers.
     *
     * <p>
     * Sets an alias for the old deleted/updated row.
     * </p>
     *
     * @param alias
     *            The alias of the row.
     * @return This instance for chaining.
     */
    public CreateTriggerStatement oldAs(String alias)
    {
        this.oldAlias = alias;
        return this;
    }

    /**
     * Indicates that this trigger should be executed on each affected row.
     *
     * <p>
     * I.e. an update statement that affected 30 rows will cause this trigger to be executed 30 times.
     * </p>
     *
     * @return The created {@link TriggerAction} to further extend the action.
     */
    public TriggerAction forEachRow()
    {
        this.triggerAction = new TriggerAction(this);
        this.forEachRow = true;
        return this.triggerAction;
    }

    /**
     * Indicates that this trigger should be executed on each affected row.
     *
     * <p>
     * I.e. an update statement that affected 30 rows will cause this trigger to only be executed once. If the statement
     * did not affect any rows, this trigger will still be executed once.
     * </p>
     *
     * @return The created {@link TriggerAction} to further extend the action.
     */
    public TriggerAction forEachStatement()
    {
        this.triggerAction = new TriggerAction(this);
        this.forEachRow = false;
        return this.triggerAction;
    }

    /**
     * This statement will attempt to replace the trigger if it already exists.
     *
     * @return This instance for chaining.
     */
    public CreateTriggerStatement replace()
    {
        this.replace = true;
        return this;
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#execute()
     */
    @Override
    public int execute()
    {
        return execute(false);
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#execute(boolean)
     */
    @Override
    public int execute(boolean printLogs)
    {
        String sql = toString();

        int result = Integer.MIN_VALUE;
        String log = "";

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
        {
            log("Executing: " + sql,
                printLogs);
            statement.executeUpdate();
            result = 1;

            if (this.shouldCommit)
            {
                this.db.commit();
            }

            if (this.onSuccess != null)
            {
                this.onSuccess.accept(this, result);
            }
        }
        catch (SQLException e)
        {
            if (e.getSQLState().equals(ALREADY_EXISTS_ERROR) && this.replace)
            {
                if (this.db.drop().trigger(this.name).execute(printLogs) > 0)
                {
                    try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
                    {
                        log("Replacing trigger '" + this.name + "'.",
                            printLogs);
                        statement.executeUpdate();

                        result = 1;

                        if (this.onSuccess != null)
                        {
                            this.onSuccess.accept(this, result);
                        }
                        return result;
                    }
                    catch (SQLException ex)
                    {
                        if (this.onFail != null)
                        {
                            result = this.onFail.apply(this,
                                                       ex);
                        }
                        else
                        {
                            DatabaseAccess.log.print(ex);
                            result = -1;
                        }
                    }
                }
                else
                {
                    log("Failed to drop trigger.",
                        printLogs);

                    if (this.onFail != null)
                    {
                        result = this.onFail.apply(this,
                                                   e);
                    }
                    else
                    {
                        DatabaseAccess.log.print(e);
                        result = -1;
                    }
                }
            }
            else
            {
                if (this.onFail != null)
                {
                    result = this.onFail.apply(this,
                                               e);
                }
                else
                {
                    result = -1;
                    DatabaseAccess.log.print(e);
                }
            }
        }

        return result;
    }

    /**
     * @see bt.db.statement.SqlStatement#toString()
     */
    @Override
    public String toString()
    {
        String sql = this.statementKeyword + " " + this.name + " " + this.when + " " + this.triggerKeyword + " ON "
                     + this.tables[0];

        if (this.newAlias != null || this.oldAlias != null)
        {
            sql += " REFERENCING";

            if (this.newAlias != null)
            {
                sql += " NEW AS " + this.newAlias;
            }

            if (this.oldAlias != null)
            {
                sql += " OLD AS " + this.oldAlias;
            }
        }

        sql += " FOR EACH " + (this.forEachRow ? "ROW " : "STATEMENT ");

        if (this.triggerAction != null)
        {
            sql += this.triggerAction.toString();
        }

        return sql;
    }
}