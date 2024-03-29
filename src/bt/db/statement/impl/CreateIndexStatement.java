package bt.db.statement.impl;

import bt.db.DatabaseAccess;
import bt.db.exc.SqlExecutionException;
import bt.db.func.Sql;
import bt.db.statement.clause.IndexColumnClause;
import bt.log.Log;
import bt.utils.Null;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * A statement to define an SQL index.
 *
 * @author &#8904
 */
public class CreateIndexStatement extends CreateStatement<CreateIndexStatement, CreateIndexStatement>
{
    private boolean unique;
    private List<IndexColumnClause> columnClauses;

    /**
     * @param db
     * @param name
     */
    public CreateIndexStatement(DatabaseAccess db, String name)
    {
        super(db, name);
        this.statementKeyword = "CREATE";
        this.columnClauses = new ArrayList<>();
    }

    public CreateIndexStatement on(String table)
    {
        this.tables = new String[]
                {
                        table
                };
        return this;
    }

    public IndexColumnClause column(String column)
    {
        var clause = new IndexColumnClause(this, column);
        this.columnClauses.add(clause);
        return clause;
    }

    public CreateIndexStatement unique()
    {
        this.unique = true;
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
            result = handleFail(new SqlExecutionException(e.getMessage(), sql, e));
        }

        return result;
    }

    /**
     * @see bt.db.statement.SqlStatement#toString()
     */
    @Override
    public String toString()
    {
        String sql = this.statementKeyword + (this.unique ? " UNIQUE " : " ") + "INDEX " + this.name + " ON ";

        sql += this.tables[0] + " (";

        for (var clause : this.columnClauses)
        {
            sql += clause.toString() + ", ";
        }

        sql = sql.substring(0, sql.length() - 2);

        sql += ")";

        return sql;
    }
}