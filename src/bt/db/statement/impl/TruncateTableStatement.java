package bt.db.statement.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import bt.db.DatabaseAccess;
import bt.db.exc.SqlExecutionException;
import bt.db.statement.SqlModifyStatement;

/**
 * Represents an SQL truncate table statement which can be extended through method chaining.
 *
 * @author &#8904
 */
public class TruncateTableStatement extends SqlModifyStatement<TruncateTableStatement, TruncateTableStatement>
{
    /**
     * @param db
     */
    public TruncateTableStatement(DatabaseAccess db, String table)
    {
        super(db);
        this.statementKeyword = "TRUNCATE TABLE";
        this.tables = new String[]
        {
          table
        };
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

            result = statement.executeUpdate();

            handleSuccess(result);
            result = handleThreshholds(result);
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
        String sql = this.statementKeyword + " ";
        sql += this.tables[0];
        return sql;
    }
}