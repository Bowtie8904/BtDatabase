package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlType;

/**
 * Represents an SQL CREATE PROCEDURE statement which can be extended through method chaining.
 *
 * <p>
 * Procedures can be used to call java methods from the database, i.e. to alert triggers to the program.
 * </p>
 *
 * @author &#8904
 */
public class CreateProcedureStatement extends CreateStatement<CreateProcedureStatement, CreateProcedureStatement>
{
    private List<String[]> parameters;
    private boolean replace;
    private String method;

    /**
     * Creates a new instance.
     *
     * @param db
     *            The database on which the procedure will be created.
     * @param name
     *            The name of the procedure.
     */
    public CreateProcedureStatement(DatabaseAccess db, String name)
    {
        super(db,
              name);
        this.parameters = new ArrayList<>();
        this.statementKeyword = "CREATE PROCEDURE";
    }

    /**
     * Adds a parameter to the procedure.
     *
     * @param name
     *            The name of the parameter.
     * @param type
     *            The {@link SqlType} of the parameter.
     * @return This instance for chaining.
     */
    public CreateProcedureStatement parameter(String name, SqlType type)
    {
        this.parameters.add(new String[]
        {
          name, type.toString()
        });

        return this;
    }

    /**
     * Sets the sizes of the last added parameter.
     *
     * @param sizes
     * @return This instance for chining.
     */
    public CreateProcedureStatement size(int... sizes)
    {
        if (!this.parameters.isEmpty())
        {
            String[] param = this.parameters.get(this.parameters.size() - 1);
            String size = "";

            size += "(";

            for (int i : sizes)
            {
                size += i + ", ";
            }

            size = size.substring(0,
                                  size.length() - 2);
            size += ")";

            param[1] = param[1] + size;
        }

        return this;
    }

    /**
     * Marks this procedure for replacement.
     *
     * <p>
     * If a procedure with this name already exists, this statement will attempt to drop the old one before creating
     * this one. Dropping a procedure does not work if it is still being used, for example by a trigger.
     * </p>
     *
     * @return This instance for chaining.
     */
    public CreateProcedureStatement replace()
    {
        this.replace = true;
        return this;
    }

    /**
     * Sets the java method that should be called by this procedure.
     *
     * @param javaMethod
     *            The full method name (class name + methodname).
     * @return This instance for chaining.
     */
    public CreateProcedureStatement call(String javaMethod)
    {
        this.method = javaMethod;
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

        try (Statement statement = this.db.getConnection().createStatement())
        {
            log("Executing: " + sql,
                printLogs);
            statement.execute(sql);
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
                log("Replacing procedure '" + this.name + "'.",
                    printLogs);

                DropStatement drop = this.db.drop().procedure(this.name).onFail((s, ex) ->
                {
                    log(ex.getMessage(),
                        printLogs);
                    return 0;
                });

                if (drop.execute(printLogs) > 0)
                {
                    try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
                    {
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
                    log("Failed to drop procedure.",
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
        String sql = this.statementKeyword + " " + this.name + " (";

        for (String[] param : this.parameters)
        {
            sql += param[0] + " " + param[1] + ", ";
        }

        sql = sql.substring(0,
                            sql.length() - 2);
        sql += ") PARAMETER STYLE JAVA LANGUAGE JAVA DYNAMIC RESULT SETS 0 EXTERNAL NAME '" + this.method + "'";

        return sql;
    }
}