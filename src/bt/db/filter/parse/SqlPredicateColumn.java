package bt.db.filter.parse;

public class SqlPredicateColumn
{
    private String column;

    public SqlPredicateColumn(String column)
    {
        this.column = column;
    }

    @Override
    public String toString()
    {
        return this.column;
    }
}