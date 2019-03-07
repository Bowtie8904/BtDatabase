package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

import bt.db.DatabaseAccess;
import bt.db.statement.clause.TriggerAction;

/**
 * @author &#8904
 *
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
     * @param db
     */
    public CreateTriggerStatement(DatabaseAccess db, String name)
    {
        super(db, name);
        this.statementKeyword = "CREATE TRIGGER";
    }

    public CreateTriggerStatement after(String trigger)
    {
        this.triggerKeyword = trigger.toUpperCase();
        this.when = "AFTER";
        return this;
    }

    public CreateTriggerStatement before(String trigger)
    {
        this.triggerKeyword = trigger.toUpperCase();
        this.when = "NO CASCADE BEFORE";
        return this;
    }

    @Override
    public CreateTriggerStatement commit()
    {
        return (CreateTriggerStatement)super.commit();
    }

    @Override
    public CreateTriggerStatement unprepared()
    {
        return (CreateTriggerStatement)super.unprepared();
    }

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

    public CreateTriggerStatement on(String table)
    {
        this.tables = new String[]
        {
                table
        };

        return this;
    }

    public CreateTriggerStatement newAs(String alias)
    {
        this.newAlias = alias;
        return this;
    }

    public CreateTriggerStatement oldAs(String alias)
    {
        this.oldAlias = alias;
        return this;
    }

    public TriggerAction forEachRow()
    {
        this.triggerAction = new TriggerAction(this);
        this.forEachRow = true;
        return this.triggerAction;
    }

    public TriggerAction forEachStatement()
    {
        this.triggerAction = new TriggerAction(this);
        this.forEachRow = false;
        return this.triggerAction;
    }

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
            log("Executing: " + sql, printLogs);
            statement.executeUpdate();
            result = 1;

            if (this.shouldCommit)
            {
                this.db.commit();
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
                        log("Replacing trigger '" + this.name + "'.", printLogs);
                        statement.executeUpdate();
                        return 1;
                    }
                    catch (SQLException ex)
                    {
                        if (this.onFail != null)
                        {
                            result = this.onFail.apply(this, ex);
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
                    log("Failed to drop trigger.", printLogs);

                    if (this.onFail != null)
                    {
                        result = this.onFail.apply(this, e);
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
                    result = this.onFail.apply(this, e);
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