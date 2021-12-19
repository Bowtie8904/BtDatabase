package bt.db.statement.impl;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlState;
import bt.db.constants.SqlType;
import bt.db.exc.SqlExecutionException;
import bt.db.func.Sql;
import bt.log.Log;
import bt.utils.Null;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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
    private String method;

    /**
     * Creates a new instance.
     *
     * @param db   The database on which the procedure will be created.
     * @param name The name of the procedure.
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
     * @param name The name of the parameter.
     * @param type The {@link SqlType} of the parameter.
     *
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
     *
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
     * Sets the java method that should be called by this procedure.
     *
     * @param javaMethod The full method name (class name + methodname).
     *
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
    protected int executeStatement()
    {
        String sql = toString();

        int result = Integer.MIN_VALUE;
        String log = "";

        try (Statement statement = this.db.getConnection().createStatement())
        {
            Log.debug("Executing: " + sql);
            statement.execute(sql);
            endExecutionTime();
            result = 1;

            if (this.saveObjectData)
            {
                this.db.insert()
                       .into(DatabaseAccess.OBJECT_DATA_TABLE)
                       .set("instanceID", this.db.getInstanceID())
                       .set("object_name", this.name.toUpperCase())
                       .set("object_ddl", sql + ";")
                       .onDuplicateKey((s, e2) ->
                                       {
                                           return this.db.update(DatabaseAccess.OBJECT_DATA_TABLE)
                                                         .set("instanceID", this.db.getInstanceID())
                                                         .set("object_name", this.name.toUpperCase())
                                                         .set("object_ddl", sql + ";")
                                                         .where(Sql.upper("object_name").toString()).equal(this.name.toUpperCase())
                                                         .onFail((st, ex) ->
                                                                 {
                                                                     return handleFail(new SqlExecutionException(ex.getMessage(), sql, ex));
                                                                 })
                                                         .execute();
                                       })
                       .execute();
            }

            if (this.shouldCommit)
            {
                this.db.commit();
            }

            Null.checkConsume(this.onSuccess, result, (r) -> this.onSuccess.accept(this, r));
        }
        catch (SQLException e)
        {
            if ((e.getSQLState().equals(SqlState.ALREADY_EXISTS.toString()) || e.getSQLState().equals(SqlState.ALREADY_EXISTS_IN.toString())) && this.replace)
            {
                Log.debug("Replacing procedure '" + this.name + "'.");

                DropStatement drop = this.db.drop()
                                            .procedure(this.name)
                                            .onFail((s, ex) ->
                                                    {
                                                        handleFail(new SqlExecutionException(e.getMessage(), sql, e));
                                                        return 0;
                                                    });

                if (drop.execute() > 0)
                {
                    try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
                    {
                        statement.executeUpdate();
                        endExecutionTime();
                        result = 1;

                        if (this.saveObjectData)
                        {
                            this.db.insert()
                                   .into(DatabaseAccess.OBJECT_DATA_TABLE)
                                   .set("instanceID", this.db.getInstanceID())
                                   .set("object_name", this.name.toUpperCase())
                                   .set("object_ddl", sql + ";")
                                   .onDuplicateKey((s, e2) ->
                                                   {
                                                       return this.db.update(DatabaseAccess.OBJECT_DATA_TABLE)
                                                                     .set("instanceID", this.db.getInstanceID())
                                                                     .set("object_name", this.name.toUpperCase())
                                                                     .set("object_ddl", sql + ";")
                                                                     .where(Sql.upper("object_name").toString()).equal(this.name.toUpperCase())
                                                                     .onFail((st, ex) ->
                                                                             {
                                                                                 return handleFail(new SqlExecutionException(ex.getMessage(), sql, ex));
                                                                             })
                                                                     .execute();
                                                   })
                                   .execute();
                        }

                        if (this.shouldCommit)
                        {
                            this.db.commit();
                        }

                        handleSuccess(result);

                        return result;
                    }
                    catch (SQLException ex)
                    {
                        result = handleFail(new SqlExecutionException(e.getMessage(), sql, ex));
                    }
                }
            }
            else
            {
                result = handleFail(new SqlExecutionException(e.getMessage(), sql, e));
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
                            sql.length() - 2) + ")" + System.lineSeparator();
        sql += " PARAMETER STYLE JAVA LANGUAGE JAVA DYNAMIC RESULT SETS 0 EXTERNAL NAME '" + this.method + "'";

        return sql;
    }
}