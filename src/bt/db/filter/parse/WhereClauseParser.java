package bt.db.filter.parse;

import bt.db.statement.clause.condition.ConditionalClause;
import bt.log.Logger;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author &#8904
 */
public class WhereClauseParser implements SelectVisitor, ExpressionVisitor
{
    private List<ConditionalParser> conditionals;

    public static List<ConditionalParser> parse(String sql) throws JSQLParserException
    {
        Statement stmt = CCJSqlParserUtil.parse(sql);
        var parser = new WhereClauseParser();
        ((Select)stmt).getSelectBody().accept(parser);
        var list = parser.getConditionals();
        Collections.reverse(list);
        return list;
    }

    public WhereClauseParser()
    {
        this.conditionals = new ArrayList<>();
    }

    public List<ConditionalParser> getConditionals()
    {
        return this.conditionals;
    }

    public void handleExpression(Expression e)
    {
        //Logger.global().print(e.getClass().getSimpleName() + "  " + e);
        var cond = new ConditionalParser(ConditionalClause.AND);
        cond.addExpression(new ExpressionParser(ConditionalClause.AND, e));
        this.conditionals.add(cond);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.conditional.AndExpression)
     */
    @Override
    public void visit(AndExpression andExpression)
    {
        //Logger.global().print(andExpression.getClass().getSimpleName() + "  " + andExpression);
        var cond = new ConditionalParser(andExpression);
        this.conditionals.add(cond);
        andExpression.getLeftExpression().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.conditional.OrExpression)
     */
    @Override
    public void visit(OrExpression orExpression)
    {
        //Logger.global().print(orExpression.getClass().getSimpleName() + "  " + orExpression);
        var cond = new ConditionalParser(orExpression);
        this.conditionals.add(cond);
        orExpression.getLeftExpression().accept(this);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.Parenthesis)
     */
    @Override
    public void visit(Parenthesis parenthesis)
    {
        handleExpression(parenthesis);
    }

    /**
     * @see net.sf.jsqlparser.statement.select.SelectVisitor#visit(net.sf.jsqlparser.statement.select.PlainSelect)
     */
    @Override
    public void visit(PlainSelect plainSelect)
    {
        //Logger.global().print(plainSelect);
        if (plainSelect.getWhere() != null)
        {
            plainSelect.getWhere().accept(this);
        }
    }

    /**
     * @see net.sf.jsqlparser.statement.select.SelectVisitor#visit(net.sf.jsqlparser.statement.select.SetOperationList)
     */
    @Override
    public void visit(SetOperationList setOpList)
    {
        Logger.global().print(setOpList);
    }

    /**
     * @see net.sf.jsqlparser.statement.select.SelectVisitor#visit(net.sf.jsqlparser.statement.select.WithItem)
     */
    @Override
    public void visit(WithItem withItem)
    {
        Logger.global().print(withItem);
    }

    /**
     * @see net.sf.jsqlparser.statement.select.SelectVisitor#visit(net.sf.jsqlparser.statement.values.ValuesStatement)
     */
    @Override
    public void visit(ValuesStatement aThis)
    {
        Logger.global().print(aThis);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift)
     */
    @Override
    public void visit(BitwiseRightShift aThis)
    {
        handleExpression(aThis);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift)
     */
    @Override
    public void visit(BitwiseLeftShift aThis)
    {
        handleExpression(aThis);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.NullValue)
     */
    @Override
    public void visit(NullValue nullValue)
    {
        handleExpression(nullValue);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.Function)
     */
    @Override
    public void visit(Function function)
    {
        handleExpression(function);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.SignedExpression)
     */
    @Override
    public void visit(SignedExpression signedExpression)
    {
        handleExpression(signedExpression);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.JdbcParameter)
     */
    @Override
    public void visit(JdbcParameter jdbcParameter)
    {
        handleExpression(jdbcParameter);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.JdbcNamedParameter)
     */
    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter)
    {
        handleExpression(jdbcNamedParameter);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.DoubleValue)
     */
    @Override
    public void visit(DoubleValue doubleValue)
    {
        handleExpression(doubleValue);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.LongValue)
     */
    @Override
    public void visit(LongValue longValue)
    {
        handleExpression(longValue);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.HexValue)
     */
    @Override
    public void visit(HexValue hexValue)
    {
        handleExpression(hexValue);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.DateValue)
     */
    @Override
    public void visit(DateValue dateValue)
    {
        handleExpression(dateValue);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.TimeValue)
     */
    @Override
    public void visit(TimeValue timeValue)
    {
        handleExpression(timeValue);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.TimestampValue)
     */
    @Override
    public void visit(TimestampValue timestampValue)
    {
        handleExpression(timestampValue);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.StringValue)
     */
    @Override
    public void visit(StringValue stringValue)
    {
        handleExpression(stringValue);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Addition)
     */
    @Override
    public void visit(Addition addition)
    {
        handleExpression(addition);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Division)
     */
    @Override
    public void visit(Division division)
    {
        handleExpression(division);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision)
     */
    @Override
    public void visit(IntegerDivision division)
    {
        handleExpression(division);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Multiplication)
     */
    @Override
    public void visit(Multiplication multiplication)
    {
        handleExpression(multiplication);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Subtraction)
     */
    @Override
    public void visit(Subtraction subtraction)
    {
        handleExpression(subtraction);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.Between)
     */
    @Override
    public void visit(Between between)
    {
        handleExpression(between);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.EqualsTo)
     */
    @Override
    public void visit(EqualsTo equalsTo)
    {
        handleExpression(equalsTo);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.GreaterThan)
     */
    @Override
    public void visit(GreaterThan greaterThan)
    {
        handleExpression(greaterThan);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals)
     */
    @Override
    public void visit(GreaterThanEquals greaterThanEquals)
    {
        handleExpression(greaterThanEquals);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.InExpression)
     */
    @Override
    public void visit(InExpression inExpression)
    {
        handleExpression(inExpression);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.FullTextSearch)
     */
    @Override
    public void visit(FullTextSearch fullTextSearch)
    {
        handleExpression(fullTextSearch);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.IsNullExpression)
     */
    @Override
    public void visit(IsNullExpression isNullExpression)
    {
        handleExpression(isNullExpression);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression)
     */
    @Override
    public void visit(IsBooleanExpression isBooleanExpression)
    {
        handleExpression(isBooleanExpression);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.LikeExpression)
     */
    @Override
    public void visit(LikeExpression likeExpression)
    {
        handleExpression(likeExpression);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThan)
     */
    @Override
    public void visit(MinorThan minorThan)
    {
        handleExpression(minorThan);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
     */
    @Override
    public void visit(MinorThanEquals minorThanEquals)
    {
        handleExpression(minorThanEquals);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.NotEqualsTo)
     */
    @Override
    public void visit(NotEqualsTo notEqualsTo)
    {
        handleExpression(notEqualsTo);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.schema.Column)
     */
    @Override
    public void visit(Column tableColumn)
    {
        handleExpression(tableColumn);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.statement.select.SubSelect)
     */
    @Override
    public void visit(SubSelect subSelect)
    {
        handleExpression(subSelect);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.CaseExpression)
     */
    @Override
    public void visit(CaseExpression caseExpression)
    {
        handleExpression(caseExpression);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.WhenClause)
     */
    @Override
    public void visit(WhenClause whenClause)
    {
        handleExpression(whenClause);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.ExistsExpression)
     */
    @Override
    public void visit(ExistsExpression existsExpression)
    {
        handleExpression(existsExpression);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AllComparisonExpression)
     */
    @Override
    public void visit(AllComparisonExpression allComparisonExpression)
    {
        handleExpression(allComparisonExpression);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AnyComparisonExpression)
     */
    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression)
    {
        handleExpression(anyComparisonExpression);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Concat)
     */
    @Override
    public void visit(Concat concat)
    {
        handleExpression(concat);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.Matches)
     */
    @Override
    public void visit(Matches matches)
    {
        handleExpression(matches);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd)
     */
    @Override
    public void visit(BitwiseAnd bitwiseAnd)
    {
        handleExpression(bitwiseAnd);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr)
     */
    @Override
    public void visit(BitwiseOr bitwiseOr)
    {
        handleExpression(bitwiseOr);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor)
     */
    @Override
    public void visit(BitwiseXor bitwiseXor)
    {
        handleExpression(bitwiseXor);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.CastExpression)
     */
    @Override
    public void visit(CastExpression cast)
    {
        handleExpression(cast);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Modulo)
     */
    @Override
    public void visit(Modulo modulo)
    {
        handleExpression(modulo);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AnalyticExpression)
     */
    @Override
    public void visit(AnalyticExpression aexpr)
    {
        handleExpression(aexpr);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.ExtractExpression)
     */
    @Override
    public void visit(ExtractExpression eexpr)
    {
        handleExpression(eexpr);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.IntervalExpression)
     */
    @Override
    public void visit(IntervalExpression iexpr)
    {
        handleExpression(iexpr);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.OracleHierarchicalExpression)
     */
    @Override
    public void visit(OracleHierarchicalExpression oexpr)
    {
        handleExpression(oexpr);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator)
     */
    @Override
    public void visit(RegExpMatchOperator rexpr)
    {
        handleExpression(rexpr);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.JsonExpression)
     */
    @Override
    public void visit(JsonExpression jsonExpr)
    {
        handleExpression(jsonExpr);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.JsonOperator)
     */
    @Override
    public void visit(JsonOperator jsonExpr)
    {
        handleExpression(jsonExpr);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator)
     */
    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator)
    {
        handleExpression(regExpMySQLOperator);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.UserVariable)
     */
    @Override
    public void visit(UserVariable var)
    {
        handleExpression(var);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.NumericBind)
     */
    @Override
    public void visit(NumericBind bind)
    {
        handleExpression(bind);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.KeepExpression)
     */
    @Override
    public void visit(KeepExpression aexpr)
    {
        handleExpression(aexpr);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.MySQLGroupConcat)
     */
    @Override
    public void visit(MySQLGroupConcat groupConcat)
    {
        handleExpression(groupConcat);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.ValueListExpression)
     */
    @Override
    public void visit(ValueListExpression valueList)
    {
        handleExpression(valueList);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.RowConstructor)
     */
    @Override
    public void visit(RowConstructor rowConstructor)
    {
        handleExpression(rowConstructor);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.OracleHint)
     */
    @Override
    public void visit(OracleHint hint)
    {
        handleExpression(hint);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.TimeKeyExpression)
     */
    @Override
    public void visit(TimeKeyExpression timeKeyExpression)
    {
        handleExpression(timeKeyExpression);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.DateTimeLiteralExpression)
     */
    @Override
    public void visit(DateTimeLiteralExpression literal)
    {
        handleExpression(literal);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.NotExpression)
     */
    @Override
    public void visit(NotExpression aThis)
    {
        handleExpression(aThis);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.NextValExpression)
     */
    @Override
    public void visit(NextValExpression aThis)
    {
        handleExpression(aThis);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.CollateExpression)
     */
    @Override
    public void visit(CollateExpression aThis)
    {
        handleExpression(aThis);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.SimilarToExpression)
     */
    @Override
    public void visit(SimilarToExpression aThis)
    {
        handleExpression(aThis);
    }

    /**
     * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.ArrayExpression)
     */
    @Override
    public void visit(ArrayExpression aThis)
    {
        handleExpression(aThis);
    }
}