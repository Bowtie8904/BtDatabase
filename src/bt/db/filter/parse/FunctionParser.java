package bt.db.filter.parse;

import bt.db.exc.UnsupportedSqlException;
import bt.log.Logger;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author &#8904
 */
public class FunctionParser implements ExpressionVisitor, ItemsListVisitor
{
    private Function function;
    private List<Object> values;

    public FunctionParser(Function function)
    {
        this.function = function;
        prepare();
    }

    public void prepare()
    {
        if (this.function != null)
        {
            this.values = new ArrayList<>();
            this.function.getParameters().accept(this);
        }
    }

    public String getName()
    {
        return this.function.getName();
    }

    public List<Object> getValues()
    {
        return this.values;
    }

    public void handle(Expression e)
    {
        Logger.global().print(e.getClass().getSimpleName());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.Parenthesis)
     */
    @Override
    public void visit(Parenthesis parenthesis)
    {
        parenthesis.getExpression().accept(this);
        throw new UnsupportedSqlException("Parenthesis are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift)
     */
    @Override
    public void visit(BitwiseRightShift aThis)
    {
        handle(aThis);
        throw new UnsupportedSqlException("Bitwise right shifts are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift)
     */
    @Override
    public void visit(BitwiseLeftShift aThis)
    {
        handle(aThis);
        throw new UnsupportedSqlException("Bitwise left shifts are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.NullValue)
     */
    @Override
    public void visit(NullValue nullValue)
    {
        handle(nullValue);
        throw new UnsupportedSqlException("Null values in functions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.Function)
     */
    @Override
    public void visit(Function function)
    {
        handle(function);
        this.values.add(function.toString());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.SignedExpression)
     */
    @Override
    public void visit(SignedExpression signedExpression)
    {
        handle(signedExpression);
        throw new UnsupportedSqlException("Signed expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.JdbcParameter)
     */
    @Override
    public void visit(JdbcParameter jdbcParameter)
    {
        handle(jdbcParameter);
        throw new UnsupportedSqlException("Dynamic parameters are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.JdbcNamedParameter)
     */
    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter)
    {
        handle(jdbcNamedParameter);
        throw new UnsupportedSqlException("Named parameters are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.DoubleValue)
     */
    @Override
    public void visit(DoubleValue doubleValue)
    {
        handle(doubleValue);
        this.values.add(doubleValue.getValue());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.LongValue)
     */
    @Override
    public void visit(LongValue longValue)
    {
        handle(longValue);
        this.values.add(longValue.getValue());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.HexValue)
     */
    @Override
    public void visit(HexValue hexValue)
    {
        handle(hexValue);
        this.values.add(hexValue.getValue());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.DateValue)
     */
    @Override
    public void visit(DateValue dateValue)
    {
        handle(dateValue);
        this.values.add(dateValue.getValue().getTime());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.TimeValue)
     */
    @Override
    public void visit(TimeValue timeValue)
    {
        handle(timeValue);
        this.values.add(timeValue.getValue().getTime());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.TimestampValue)
     */
    @Override
    public void visit(TimestampValue timestampValue)
    {
        handle(timestampValue);
        this.values.add(timestampValue.getValue().getTime());
    }

    @Override
    public void visit(StringValue stringValue)
    {
        handle(stringValue);
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
            this.values.add(stringValue.getValue());
        }
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Addition)
     */
    @Override
    public void visit(Addition addition)
    {
        handle(addition);
        this.values.add(addition.toString());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Division)
     */
    @Override
    public void visit(Division division)
    {
        handle(division);
        this.values.add(division.toString());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision)
     */
    @Override
    public void visit(IntegerDivision division)
    {
        handle(division);
        this.values.add(division.toString());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Multiplication)
     */
    @Override
    public void visit(Multiplication multiplication)
    {
        handle(multiplication);
        this.values.add(multiplication.toString());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Subtraction)
     */
    @Override
    public void visit(Subtraction subtraction)
    {
        handle(subtraction);
        this.values.add(subtraction.toString());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.Between)
     */
    @Override
    public void visit(Between between)
    {
        handle(between);
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.EqualsTo)
     */
    @Override
    public void visit(EqualsTo equalsTo)
    {
        handle(equalsTo);
        equalsTo.getLeftExpression().accept(this);
        equalsTo.getRightExpression().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.GreaterThan)
     */
    @Override
    public void visit(GreaterThan greaterThan)
    {
        handle(greaterThan);
        greaterThan.getLeftExpression().accept(this);
        greaterThan.getRightExpression().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals)
     */
    @Override
    public void visit(GreaterThanEquals greaterThanEquals)
    {
        handle(greaterThanEquals);
        greaterThanEquals.getLeftExpression().accept(this);
        greaterThanEquals.getRightExpression().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.InExpression)
     */
    @Override
    public void visit(InExpression inExpression)
    {
        handle(inExpression);
        inExpression.getLeftExpression().accept(this);
        inExpression.getRightItemsList().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.FullTextSearch)
     */
    @Override
    public void visit(FullTextSearch fullTextSearch)
    {
        handle(fullTextSearch);
        throw new UnsupportedSqlException("Full text search is not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.IsNullExpression)
     */
    @Override
    public void visit(IsNullExpression isNullExpression)
    {
        handle(isNullExpression);
        isNullExpression.getLeftExpression().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression)
     */
    @Override
    public void visit(IsBooleanExpression isBooleanExpression)
    {
        handle(isBooleanExpression);
        throw new UnsupportedSqlException("Is boolean expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.LikeExpression)
     */
    @Override
    public void visit(LikeExpression likeExpression)
    {
        handle(likeExpression);
        likeExpression.getLeftExpression().accept(this);
        likeExpression.getRightExpression().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThan)
     */
    @Override
    public void visit(MinorThan minorThan)
    {
        handle(minorThan);
        minorThan.getLeftExpression().accept(this);
        minorThan.getRightExpression().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
     */
    @Override
    public void visit(MinorThanEquals minorThanEquals)
    {
        handle(minorThanEquals);
        minorThanEquals.getLeftExpression().accept(this);
        minorThanEquals.getRightExpression().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.NotEqualsTo)
     */
    @Override
    public void visit(NotEqualsTo notEqualsTo)
    {
        handle(notEqualsTo);
        notEqualsTo.getLeftExpression().accept(this);
        notEqualsTo.getRightExpression().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.schema.Column)
     */
    @Override
    public void visit(Column tableColumn)
    {
        handle(tableColumn);

        this.values.add(tableColumn.getFullyQualifiedName());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.statement.select.SubSelect)
     */
    @Override
    public void visit(SubSelect subSelect)
    {
        handle(subSelect);
        throw new UnsupportedSqlException("SubSelects are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.CaseExpression)
     */
    @Override
    public void visit(CaseExpression caseExpression)
    {
        handle(caseExpression);
        throw new UnsupportedSqlException("Case expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.WhenClause)
     */
    @Override
    public void visit(WhenClause whenClause)
    {
        handle(whenClause);
        throw new UnsupportedSqlException("When clauses are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.ExistsExpression)
     */
    @Override
    public void visit(ExistsExpression existsExpression)
    {
        handle(existsExpression);
        throw new UnsupportedSqlException("Exists expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AllComparisonExpression)
     */
    @Override
    public void visit(AllComparisonExpression allComparisonExpression)
    {
        handle(allComparisonExpression);
        throw new UnsupportedSqlException("All comparissions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AnyComparisonExpression)
     */
    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression)
    {
        handle(anyComparisonExpression);
        throw new UnsupportedSqlException("Any comparissions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Concat)
     */
    @Override
    public void visit(Concat concat)
    {
        handle(concat);
        throw new UnsupportedSqlException("Concat operator is not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.Matches)
     */
    @Override
    public void visit(Matches matches)
    {
        handle(matches);
        throw new UnsupportedSqlException("Matches are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd)
     */
    @Override
    public void visit(BitwiseAnd bitwiseAnd)
    {
        handle(bitwiseAnd);
        throw new UnsupportedSqlException("Bitwise AND is not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr)
     */
    @Override
    public void visit(BitwiseOr bitwiseOr)
    {
        handle(bitwiseOr);
        throw new UnsupportedSqlException("Bitwise OR is not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor)
     */
    @Override
    public void visit(BitwiseXor bitwiseXor)
    {
        handle(bitwiseXor);
        throw new UnsupportedSqlException("Bitwise XOR is not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.CastExpression)
     */
    @Override
    public void visit(CastExpression cast)
    {
        handle(cast);
        throw new UnsupportedSqlException("Cast expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Modulo)
     */
    @Override
    public void visit(Modulo modulo)
    {
        handle(modulo);
        throw new UnsupportedSqlException("Modulo operator not supported, use MOD function instead.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AnalyticExpression)
     */
    @Override
    public void visit(AnalyticExpression aexpr)
    {
        handle(aexpr);
        throw new UnsupportedSqlException("Analytic expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.ExtractExpression)
     */
    @Override
    public void visit(ExtractExpression eexpr)
    {
        handle(eexpr);
        throw new UnsupportedSqlException("Extract expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.IntervalExpression)
     */
    @Override
    public void visit(IntervalExpression iexpr)
    {
        handle(iexpr);
        throw new UnsupportedSqlException("Interval expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.OracleHierarchicalExpression)
     */
    @Override
    public void visit(OracleHierarchicalExpression oexpr)
    {
        handle(oexpr);
        throw new UnsupportedSqlException("Oracle hierachical expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator)
     */
    @Override
    public void visit(RegExpMatchOperator rexpr)
    {
        handle(rexpr);
        throw new UnsupportedSqlException("RegEx match operator is not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.JsonExpression)
     */
    @Override
    public void visit(JsonExpression jsonExpr)
    {
        handle(jsonExpr);
        throw new UnsupportedSqlException("JSON expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.JsonOperator)
     */
    @Override
    public void visit(JsonOperator jsonExpr)
    {
        handle(jsonExpr);
        throw new UnsupportedSqlException("JSON operator is not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator)
     */
    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator)
    {
        handle(regExpMySQLOperator);
        throw new UnsupportedSqlException("RegEx MySQL operator is not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.UserVariable)
     */
    @Override
    public void visit(UserVariable var)
    {
        handle(var);
        throw new UnsupportedSqlException("User variables are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.NumericBind)
     */
    @Override
    public void visit(NumericBind bind)
    {
        handle(bind);
        throw new UnsupportedSqlException("Numeric binds are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.KeepExpression)
     */
    @Override
    public void visit(KeepExpression aexpr)
    {
        handle(aexpr);
        throw new UnsupportedSqlException("Keep expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.MySQLGroupConcat)
     */
    @Override
    public void visit(MySQLGroupConcat groupConcat)
    {
        handle(groupConcat);
        throw new UnsupportedSqlException("MySQL group concat expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.ValueListExpression)
     */
    @Override
    public void visit(ValueListExpression valueList)
    {
        handle(valueList);
        throw new UnsupportedSqlException("Value list expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.RowConstructor)
     */
    @Override
    public void visit(RowConstructor rowConstructor)
    {
        handle(rowConstructor);
        throw new UnsupportedSqlException("Row constructors are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.OracleHint)
     */
    @Override
    public void visit(OracleHint hint)
    {
        handle(hint);
        throw new UnsupportedSqlException("Oracle hints are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.TimeKeyExpression)
     */
    @Override
    public void visit(TimeKeyExpression timeKeyExpression)
    {
        handle(timeKeyExpression);

        this.values.add(timeKeyExpression.getStringValue());
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.DateTimeLiteralExpression)
     */
    @Override
    public void visit(DateTimeLiteralExpression literal)
    {
        handle(literal);

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

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.NotExpression)
     */
    @Override
    public void visit(NotExpression aThis)
    {
        handle(aThis);
        aThis.getExpression().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.NextValExpression)
     */
    @Override
    public void visit(NextValExpression aThis)
    {
        handle(aThis);
        throw new UnsupportedSqlException("Next val expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.CollateExpression)
     */
    @Override
    public void visit(CollateExpression aThis)
    {
        handle(aThis);
        throw new UnsupportedSqlException("Collate expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.SimilarToExpression)
     */
    @Override
    public void visit(SimilarToExpression aThis)
    {
        handle(aThis);
        throw new UnsupportedSqlException("Similar to expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.ArrayExpression)
     */
    @Override
    public void visit(ArrayExpression aThis)
    {
        handle(aThis);
        throw new UnsupportedSqlException("Array expressions are not supported.");
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.conditional.AndExpression)
     */
    @Override
    public void visit(AndExpression andExpression)
    {
        handle(andExpression);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.conditional.OrExpression)
     */
    @Override
    public void visit(OrExpression orExpression)
    {
        handle(orExpression);
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
}