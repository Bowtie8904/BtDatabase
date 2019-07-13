package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.BiFunction;

import bt.db.DatabaseAccess;
import bt.db.statement.SqlModifyStatement;

/**
 * Base class for all different DROP statements.
 * 
 * @author &#8904
 */
public class DropStatement extends SqlModifyStatement<DropStatement, DropStatement>
{
    private String keyword;

    /** The name that is used in this drop statement. This would be the name of the table, trigger, ... */
    private String name;

    /**
     * Creates a new instance.
     * 
     * @param db
     *            The database that should be used for the statement.
     */
    public DropStatement(DatabaseAccess db)
    {
        super(db);
    }

    /**
     * Gets the name of the database object that should be dropped.
     * 
     * @return The name of the database object.
     */
    public String getName()
    {
        return this.name.toUpperCase();
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#commit()
     */
    @Override
    public DropStatement commit()
    {
        return (DropStatement)super.commit();
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#unprepared()
     */
    @Override
    public DropStatement unprepared()
    {
        return (DropStatement)super.unprepared();
    }

    /**
     * Indictaes that this statement should drop a table with the given name.
     * 
     * @param name
     *            The name of the table.
     * @return This instance for chaining.
     */
    public DropStatement table(String name)
    {
        this.keyword = "TABLE";
        this.name = name;
        return this;
    }

    /**
     * Indictaes that this statement should drop a procedure with the given name.
     * 
     * @param name
     *            The name of the procedure.
     * @return This instance for chaining.
     */
    public DropStatement procedure(String name)
    {
        this.keyword = "PROCEDURE";
        this.name = name;
        return this;
    }

    /**
     * Indictaes that this statement should drop a function with the given name.
     * 
     * @param name
     *            The name of the function.
     * @return This instance for chaining.
     */
    public DropStatement function(String name)
    {
        this.keyword = "FUNCTION";
        this.name = name;
        return this;
    }

    /**
     * Indictaes that this statement should drop an index with the given name.
     * 
     * @param name
     *            The name of the index.
     * @return This instance for chaining.
     */
    public DropStatement index(String name)
    {
        this.keyword = "INDEX";
        this.name = name;
        return this;
    }

    /**
     * Indictaes that this statement should drop a role with the given name.
     * 
     * @param name
     *            The name of the role.
     * @return This instance for chaining.
     */
    public DropStatement role(String name)
    {
        this.keyword = "ROLE";
        this.name = name;
        return this;
    }

    /**
     * Indictaes that this statement should drop a schema with the given name.
     * 
     * @param name
     *            The name of the schema.
     * @return This instance for chaining.
     */
    public DropStatement schema(String name)
    {
        this.keyword = "SCHEMA";
        this.name = name;
        return this;
    }

    /**
     * Indictaes that this statement should drop a sequence with the given name.
     * 
     * @param name
     *            The name of the sequence.
     * @return This instance for chaining.
     */
    public DropStatement sequence(String name)
    {
        this.keyword = "SEQUENCE";
        this.name = name;
        return this;
    }

    /**
     * Indictaes that this statement should drop a synonym with the given name.
     * 
     * @param name
     *            The name of the synonym.
     * @return This instance for chaining.
     */
    public DropStatement synonym(String name)
    {
        this.keyword = "SYNONYM";
        this.name = name;
        return this;
    }

    /**
     * Indictaes that this statement should drop a trigger with the given name.
     * 
     * @param name
     *            The name of the trigger.
     * @return This instance for chaining.
     */
    public DropStatement trigger(String name)
    {
        this.keyword = "TRIGGER";
        this.name = name;
        return this;
    }

    /**
     * Indictaes that this statement should drop a type with the given name.
     * 
     * @param name
     *            The name of the type.
     * @return This instance for chaining.
     */
    public DropStatement type(String name)
    {
        this.keyword = "TYPE";
        this.name = name;
        return this;
    }

    /**
     * Indictaes that this statement should drop a view with the given name.
     * 
     * @param name
     *            The name of the view.
     * @return This instance for chaining.
     */
    public DropStatement view(String name)
    {
        this.keyword = "VIEW";
        this.name = name;
        return this;
    }

    /**
     * Defines a data modifying statement (insert, update, delete, ...) which will be executed if there was an error
     * during the execution of the original drop statement.
     * 
     * @param onFail
     *            The SqlModifyStatement to execute instead.
     * @return This instance for chaining.
     */
    public DropStatement onFail(SqlModifyStatement onFail)
    {
        this.onFail = (statement, e) -> {
            return onFail.execute();
        };

        return this;
    }

    /**
     * Defines a BiFunction that will be executed if there was an error during the execution of this statement.
     * 
     * <p>
     * The first parameter (DropStatement) will be this statement instance, the second one is the SQLException that
     * caused the fail. The return value (Integer) will be returned by this instances {@link #execute()}.
     * </p>
     * 
     * @param onFail
     *            The BiFunction to execute.
     * @return This instance for chaining.
     */
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
        String sql = toString();

        int result = Integer.MIN_VALUE;

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
        }
        catch (SQLException e)
        {
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

        return result;
    }

    /**
     * @see bt.db.statement.SqlStatement#toString()
     */
    @Override
    public String toString()
    {
        return "DROP " + this.keyword + " " + this.name.toUpperCase();
    }
}