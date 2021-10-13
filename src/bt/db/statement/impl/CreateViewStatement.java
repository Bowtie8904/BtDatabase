package bt.db.statement.impl;

import bt.db.DatabaseAccess;
import bt.db.constants.SqlState;
import bt.db.exc.SqlExecutionException;
import bt.db.func.Sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CreateViewStatement extends CreateStatement<CreateViewStatement, CreateViewStatement>
{
    /**
     * The select to create a copy table of.
     */
    private SelectStatement asSelect;

    /**
     * Creates a new instance and initializes the fields.
     *
     * @param db   The database that should be used for the statement.
     * @param name
     */
    public CreateViewStatement(DatabaseAccess db, String name)
    {
        super(db, name);
        this.statementKeyword = "CREATE VIEW";
    }

    public CreateViewStatement as(SelectStatement select)
    {
        this.asSelect = select;
        return this;
    }

    @Override
    protected int executeStatement(boolean printLogs)
    {
        String sql = toString();

        int result = Integer.MIN_VALUE;

        try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
        {
            log("Executing: " + sql,
                printLogs);
            statement.executeUpdate();

            if (this.shouldCommit)
            {
                this.db.commit();
            }

            if (this.saveObjectData)
            {
                this.db.insert()
                       .into(DatabaseAccess.OBJECT_DATA_TABLE)
                       .set("instanceID", this.db.getInstanceID())
                       .set("object_name", this.name.toUpperCase())
                       .set("object_ddl", sql + ";")
                       .onDuplicateKey((s, e) ->
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
        }
        catch (SQLException e)
        {
            if ((e.getSQLState().equals(SqlState.ALREADY_EXISTS.toString()) || e.getSQLState().equals(SqlState.ALREADY_EXISTS_IN.toString())) && this.replace)
            {
                var drop = this.db.drop()
                                  .view(this.name)
                                  .onFail((s, ex) ->
                                          {
                                              return handleFail(new SqlExecutionException(e.getMessage(), sql, e));
                                          })
                                  .onSuccess((s, i) ->
                                             {
                                                 this.db.delete().from(DatabaseAccess.OBJECT_DATA_TABLE)
                                                        .where(Sql.upper("object_name").toString()).equal(this.name)
                                                        .onFail((s2, ex) ->
                                                                {
                                                                    return handleFail(new SqlExecutionException(e.getMessage(), sql, e));
                                                                })
                                                        .execute(printLogs);
                                             });

                if (drop.execute(printLogs) > 0)
                {
                    try (PreparedStatement statement = this.db.getConnection().prepareStatement(sql))
                    {
                        log("Replacing view '" + this.name + "'.", printLogs);
                        statement.executeUpdate();

                        result = 1;

                        endExecutionTime();

                        result = 1;

                        if (this.shouldCommit)
                        {
                            this.db.commit();
                        }

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
                    }
                    catch (SQLException ex)
                    {
                        result = handleFail(new SqlExecutionException(e.getMessage(), sql, ex));
                    }
                }
            }
        }

        return result;
    }

    /**
     * Returns the string representing this statement.
     */
    @Override
    public String toString()
    {
        String sql = this.statementKeyword + " " + this.name + " AS " + System.lineSeparator();

        if (this.asSelect != null)
        {
            sql += this.asSelect.unprepared().toString();
        }

        return sql;
    }
}