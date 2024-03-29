package bt.db.statement.impl;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlState;
import bt.db.constants.SqlType;
import bt.db.exc.SqlExecutionException;
import bt.db.func.Sql;
import bt.log.Log;
import bt.utils.Null;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * A statement to define an SQL function which calls a java method.
 *
 * @author &#8904
 */
public class CreateFunctionStatement extends CreateStatement<CreateFunctionStatement, CreateFunctionStatement>
{
    private Method method;

    private List<String[]> parameters;

    private boolean returnNullOnNull;

    /**
     * @param db
     * @param name
     */
    public CreateFunctionStatement(DatabaseAccess db, String name)
    {
        super(db, name);
        this.statementKeyword = "CREATE FUNCTION";
        this.parameters = new ArrayList<>();
    }

    /**
     * Defines the public static java method that is called.
     *
     * @param method The method that should be called.
     *
     * @return This instance for chaining.
     */
    public CreateFunctionStatement call(Method method)
    {
        this.method = method;

        for (Parameter p : this.method.getParameters())
        {
            String parameterType = SqlType.convert(p.getType()).toString();

            if (parameterType.equals(SqlType.VARCHAR.toString()))
            {
                parameterType += "(200)";
            }

            this.parameters.add(new String[]
                                        {
                                                p.getName(), parameterType
                                        });
        }
        return this;
    }

    /**
     * Defines the public static java method that is called.
     *
     * @param cls            The class that contains the method.
     * @param methodName     The case-sensitive name of the method.
     * @param parameterTypes The parameter types in the correct order.
     *
     * @return This instance for chaining.
     */
    public CreateFunctionStatement call(Class<?> cls, String methodName, Class<?>... parameterTypes)
    {
        Method foundMethod = null;
        try
        {
            foundMethod = cls.getMethod(methodName, parameterTypes);
        }
        catch (Exception e)
        {
            Log.error("COuld not find method", e);
            return this;
        }

        return call(foundMethod);
    }

    /**
     * Indicates that this function should not be invoked and instead return null if a null value is passed to it.
     *
     * @return This instance for chaining.
     */
    public CreateFunctionStatement returnNullOnNull()
    {
        this.returnNullOnNull = true;
        return this;
    }

    /**
     * @see bt.db.statement.SqlModifyStatement#executeStatement()
     */
    @Override
    protected int executeStatement()
    {
        String sql = toString();

        int result = Integer.MIN_VALUE;

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
                Log.debug("Replacing function '" + this.name + "'.");

                DropStatement drop = this.db.drop()
                                            .function(this.name)
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

        sql = sql.substring(0, sql.length() - 2) + ")" + System.lineSeparator();

        String returnType = SqlType.convert(this.method.getReturnType()).toString();

        if (returnType.equals(SqlType.VARCHAR.toString()))
        {
            returnType += "(9999)";
        }

        sql += " RETURNS " + returnType + System.lineSeparator();
        sql += " PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME '" + this.method.getDeclaringClass().getName() + "." + this.method.getName() + "'";

        if (this.returnNullOnNull)
        {
            sql += " RETURNS NULL ON NULL INPUT";
        }

        return sql;
    }
}