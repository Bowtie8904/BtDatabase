package bt.db.statement.result;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import bt.types.close.UncheckedCloseable;
import bt.utils.log.Logger;

/**
 * @author &#8904
 *
 */
public class StreamableResultSet extends AbstractSpliterator<ResultSet> implements UncheckedCloseable
{
    private ResultSet results;
    private PreparedStatement statement;
    private SqlResultSet printableResultSet;

    public StreamableResultSet(ResultSet results, PreparedStatement statement)
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
            Logger.global().print(e);
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
     * The given values define the widths of the columns.
     * </p>
     *
     * <p>
     * <b>NOTE</b> that this operation invalidates the stream meaning that it can not be traversed again.
     * </p>
     *
     * @param columnFormat
     *            If only one number is given, all columns will have the same width. If more than one value is given,
     *            there needs to be the same amount of numbers as there is columns.
     */
    public StreamableResultSet print(int... columnFormat)
    {
        getPrintableResultSet().print(columnFormat);
        return this;
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

    /**
     * Prints a formatted table of the result.
     *
     * <p>
     * <b>NOTE</b> that this operation invalidates the stream meaning that it can not be traversed again.
     * </p>
     *
     * @param log
     * @return
     */
    public StreamableResultSet print(Logger log)
    {
        getPrintableResultSet().print(log);
        return this;
    }
}