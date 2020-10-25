package bt.db.filter;

import bt.db.filter.parse.WhereClauseParser;
import bt.db.statement.clause.condition.ConditionalClause;
import net.sf.jsqlparser.JSQLParserException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Predicate;

/**
 * @author &#8904
 */
public class SqlPredicateBuilder
{
    /**
     * Creates a default {@link ClassFieldSqlPredicate} by parsing the given SQL select statement.
     *
     * @param searchCondition A full select statement. Full means that it is not enough to pass the where conditions.
     *                        The select must be syntactically correct, however it can contain references to non-existend tables.
     *                        <p>
     *                        select * from dummy where 1 = 1;
     *                        </p>
     * @return The created predicate.
     * @throws JSQLParserException If the given SQL could not be parsed correctly.
     */
    public static Predicate<Object> buildPredicate(String searchCondition) throws JSQLParserException
    {
        try
        {
            return SqlPredicateBuilder.buildPredicate(ClassFieldSqlPredicate.class, searchCondition);
        }
        catch (InstantiationException | InvocationTargetException | NoSuchMethodException e)
        {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * Creates a predicate structure by parsing the given SQL select statement and using the given type.
     *
     * @param predicateType   A class reference to the subtype of {@link SqlPredicate} which should be used. The subtype needs
     *                        to implements all constructors of {@link SqlPredicate}.
     * @param searchCondition A full select statement. Full means that it is not enough to pass the where conditions.
     *                        The select must be syntactically correct, however it can contain references to non-existend tables.
     *                        <p>
     *                        select * from dummy where 1 = 1;
     *                        </p>
     * @param <T>
     * @param <K>
     * @return The created predicate.
     * @throws InstantiationException    If the given type cant be instantiated.
     * @throws NoSuchMethodException     If the given type does not implement all constructors of {@link SqlPredicate}.
     * @throws InvocationTargetException If the invoked constructor of the type throws an exception.
     * @throws JSQLParserException       If the given SQL could not be parsed correctly.
     */
    public static <T extends SqlPredicate<K>, K> Predicate<K> buildPredicate(Class<T> predicateType, String searchCondition)
            throws InstantiationException, NoSuchMethodException, InvocationTargetException, JSQLParserException
    {
        if (searchCondition.isEmpty())
        {
            return SqlPredicateBuilder.createInstance(predicateType, new Class[] { boolean.class }, new Object[] { false });
        }

        var conditionals = WhereClauseParser.parse(searchCondition);

        if (conditionals != null)
        {
            Predicate<K> basePredicate = SqlPredicateBuilder.createInstance(predicateType, new Class[] { boolean.class }, new Object[] { true });

            for (var cond : conditionals)
            {
                int index = 0;
                String keyword;

                for (var expr : cond.getExpressions())
                {
                    var pred = expr.getPredicate(predicateType);

                    if (pred == null)
                    {
                        return SqlPredicateBuilder.createInstance(predicateType, new Class[] { boolean.class }, new Object[] { false });
                    }

                    if (index == 0)
                    {
                        keyword = cond.getKeyword();
                    }
                    else
                    {
                        keyword = expr.getKeyword();
                    }

                    if (keyword.equals(ConditionalClause.AND))
                    {
                        basePredicate = basePredicate.and(pred);
                    }
                    else if (keyword.equals(ConditionalClause.OR))
                    {
                        basePredicate = basePredicate.or(pred);
                    }

                    index++;
                }
            }

            return basePredicate;
        }
        else
        {
            return SqlPredicateBuilder.createInstance(predicateType, new Class[] { boolean.class }, new Object[] { false });
        }
    }

    public static <T extends SqlPredicate> T createInstance(Class<T> predicateType, Class[] parameters, Object[] values)
            throws InvocationTargetException, InstantiationException, NoSuchMethodException
    {
        T instance = null;

        try
        {
            Constructor<T> con = predicateType.getDeclaredConstructor(parameters);
            con.setAccessible(true);
            instance = con.newInstance(values);
        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
        }

        return instance;
    }
}