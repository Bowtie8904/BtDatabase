package bt.db.statement.result;

import bt.log.Log;
import bt.types.UncheckedCloseable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author &#8904
 */
public class StreamableResultSet extends AbstractSpliterator<ResultSet> implements UncheckedCloseable
{
    private ResultSet results;
    private Statement statement;
    private SqlResultSet printableResultSet;

    public StreamableResultSet(ResultSet results, Statement statement)
    {
        super(Long.MAX_VALUE, Spliterator.ORDERED);
        this.results = results;
        this.statement = statement;
    }

    public Stream<ResultSet> stream()
    {
        return stream(false);
    }

    public Stream<ResultSet> stream(boolean parllel)
    {
        return StreamSupport.stream(this, parllel).onClose(this);
    }

    /**
     * @see java.util.Spliterator#tryAdvance(java.util.function.Consumer)
     */
    @Override
    public boolean tryAdvance(Consumer<? super ResultSet> action)
    {
        boolean advanced = false;
        try
        {
            if (this.results.next())
            {
                action.accept(this.results);
                advanced = true;
            }
        }
        catch (SQLException e)
        {
            Log.error("Failed to advance ResultSet", e);
        }

        return advanced;
    }

    /**
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() throws Exception
    {
        this.statement.close();
    }

    private SqlResultSet getPrintableResultSet()
    {
        if (this.printableResultSet == null)
        {
            this.printableResultSet = new SqlResultSet(this.results);
        }

        return this.printableResultSet;
    }

    /**
     * Prints a formatted table of the result.
     *
     * <p>
     * <b>NOTE</b> that this operation invalidates the stream meaning that it can not be traversed again.
     * </p>
     */
    public StreamableResultSet print()
    {
        getPrintableResultSet().print();
        return this;
    }
}