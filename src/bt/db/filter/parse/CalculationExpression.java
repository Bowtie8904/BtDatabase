package bt.db.filter.parse;

import bt.db.exc.UnsupportedSqlException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Date;
import java.util.Stack;

/**
 * @author &#8904
 */
public class CalculationExpression extends ExpressionDeParser
{
    private Stack<Long> stack = new Stack<>();
    private long factor = 1;

    public long evaluate(Expression expr)
    {
        expr.accept(this);
        return this.stack.pop();
    }

    @Override
    public void visit(Function function)
    {
        switch (function.getName().toLowerCase())
        {
            case "days":
            case "day":
                this.factor = 24 * 60 * 60 * 1000;
                break;
            case "hours":
            case "hour":
                this.factor = 60 * 60 * 1000;
                break;
            case "minutes":
            case "minute":
                this.factor = 60 * 1000;
                break;
            case "seconds":
            case "second":
                this.factor = 1000;
                break;
        }
        function.getParameters().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.DateTimeLiteralExpression)
     */
    @Override
    public void visit(DateTimeLiteralExpression literal)
    {
        try
        {
            Date date = DateUtils.parseDate(literal.getValue().replace("'", "").replace("/", "-").replace(".", "-"),
                                            "yyyy-MM-dd",
                                            "yyyy-MM-dd HH:mm",
                                            "yyyy-MM-dd HH:mm:ss",
                                            "yyyy-MM-dd HH:mm:ss,SSS");
            visit(new LongValue(date.getTime()));
        }
        catch (ParseException e)
        {
            throw new UnsupportedSqlException(e.getMessage(), e);
        }
    }

    @Override
    public void visit(StringValue stringValue)
    {
        try
        {
            Date date = DateUtils.parseDate(stringValue.getValue().replace("'", "").replace("/", "-").replace(".", "-"),
                                            "yyyy-MM-dd",
                                            "yyyy-MM-dd HH:mm",
                                            "yyyy-MM-dd HH:mm:ss",
                                            "yyyy-MM-dd HH:mm:ss,SSS");
            visit(new LongValue(date.getTime()));
        }
        catch (ParseException e)
        {

        }
    }

    /**
     * @see net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor#visit(net.sf.jsqlparser.expression.operators.relational.ExpressionList)
     */
    @Override
    public void visit(ExpressionList expressionList)
    {
        expressionList.getExpressions().forEach(e -> e.accept(this));
    }

    /**
     * @see net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor#visit(net.sf.jsqlparser.expression.operators.relational.NamedExpressionList)
     */
    @Override
    public void visit(NamedExpressionList namedExpressionList)
    {
        namedExpressionList.getExpressions().forEach(e -> e.accept(this));
    }

    /**
     * @see net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MultiExpressionList)
     */
    @Override
    public void visit(MultiExpressionList multiExprList)
    {
        multiExprList.getExprList().forEach(e -> e.accept(this));
    }

    @Override
    public void visit(Addition addition)
    {
        super.visit(addition);

        long sum1 = this.stack.pop();
        long sum2 = this.stack.pop();

        this.stack.push(sum1 + sum2);
    }

    @Override
    public void visit(Subtraction sub)
    {
        super.visit(sub);

        long num1 = this.stack.pop();
        long num2 = this.stack.pop();

        this.stack.push(num2 - num1);
    }

    @Override
    public void visit(Division div)
    {
        super.visit(div);

        long num1 = this.stack.pop();
        long num2 = this.stack.pop();

        this.stack.push(num2 / num1);
    }

    @Override
    public void visit(IntegerDivision div)
    {
        super.visit(div);

        long num1 = this.stack.pop();
        long num2 = this.stack.pop();

        this.stack.push(num2 / num1);
    }

    @Override
    public void visit(Multiplication multiplication)
    {
        super.visit(multiplication);

        long fac1 = this.stack.pop();
        long fac2 = this.stack.pop();

        this.stack.push(fac1 * fac2);
    }

    @Override
    public void visit(LongValue longValue)
    {
        super.visit(longValue);
        this.stack.push(longValue.getValue() * this.factor);
        this.factor = 1;
    }
}