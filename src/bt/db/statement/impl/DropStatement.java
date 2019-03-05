package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.BiFunction;

import bt.db.DatabaseAccess;
import bt.db.statement.SqlModifyStatement;

/**
 * @author &#8904
 *
 */
public class DropStatement extends SqlModifyStatement<DropStatement, DropStatement>
{
    private String keyword;
    private String name;

    public DropStatement(DatabaseAccess db)
    {
        super(db);
    }

    public String getName()
    {
        return this.name.toUpperCase();
    }

    @Override
    public DropStatement commit()
    {
        return (DropStatement)super.commit();
    }

    @Override
    public DropStatement unprepared()
    {
        return (DropStatement)super.unprepared();
    }

    public DropStatement table(String name)
    {
        this.keyword = "TABLE";
        this.name = name;
        return this;
    }

    public DropStatement procedure(String name)
    {
        this.keyword = "PROCEDURE";
        this.name = name;
        return this;
    }

    public DropStatement function(String name)
    {
        this.keyword = "FUNCTION";
        this.name = name;
        return this;
    }

    public DropStatement index(String name)
    {
        this.keyword = "INDEX";
        this.name = name;
        return this;
    }

    public DropStatement role(String name)
    {
        this.keyword = "ROLE";
        this.name = name;
        return this;
    }

    public DropStatement schema(String name)
    {
        this.keyword = "SCHEMA";
        this.name = name;
        return this;
    }

    public DropStatement sequence(String name)
    {
        this.keyword = "SEQUENCE";
        this.name = name;
        return this;
    }

    public DropStatement synonym(String name)
    {
        this.keyword = "SYNONYM";
        this.name = name;
        return this;
    }

    public DropStatement trigger(String name)
    {
        this.keyword = "TRIGGER";
        this.name = name;
        return this;
    }

    public DropStatement type(String name)
    {
        this.keyword = "TYPE";
        this.name = name;
        return this;
    }

    public DropStatement view(String name)
    {
        this.keyword = "VIEW";
        this.name = name;
        return this;
    }

    public DropStatement onFail(SqlModifyStatement onFail)
    {
        this.onFail = (statement, e) ->
        {
            return onFail.execute();
        };

        return this;
    }

    public DropStatement onFail(BiFunction<DropStatement, SQLException, Integer> onFail)
    {
        this.onFail = onFail;
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
        String sql = "DROP " + this.keyword + " " + this.name.toUpperCase();

        int result = Integer.MIN_VALUE;

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

        return result;
    }
}