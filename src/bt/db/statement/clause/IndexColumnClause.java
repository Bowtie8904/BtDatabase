package bt.db.statement.clause;

import bt.db.statement.impl.CreateIndexStatement;

/**
 * @author &#8904
 *
 */
public class IndexColumnClause
{
    private String column;
    private boolean asc = true;
    private CreateIndexStatement statement;

    public IndexColumnClause(CreateIndexStatement statement, String column)
    {
        this.column = column;
        this.statement = statement;
    }

    public CreateIndexStatement asc()
    {
        this.asc = true;
        return this.statement;
    }

    public CreateIndexStatement desc()
    {
        this.asc = false;
        return this.statement;
    }

    @Override
    public String toString()
    {
        return this.column + (this.asc ? " ASC" : " DESC");
    }
}