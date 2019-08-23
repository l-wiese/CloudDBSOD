package fragmentation;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import metadata.Attribute;
import metadata.Server;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;

//class needed to execute a query on a single server
public class ServerQuery {
	public Set<Server> servers=new HashSet<Server>();
	String query="";
	boolean rewritten=false;
	
	static final String placeholder="XYZPLACEHOLDERXYZ";
	static final String placeholder2="XYZPLACEHOLDERXYZ2";
	static final String aliasplaceholder="AS ALIASPLACEHOLDERALIAS";
	static final String substringplaceholder="SUBSTRINGPLACEHOLDERSUBSTRING";
	
	public ServerQuery(Set<Server> servers, String query) {
		super();
		this.servers=servers;
		this.query = query;
	}
	
	public ServerQuery(Server server, String query) {
		super();
		this.servers.add(server);
		this.query = query;
	}
	public Set<Server> getServer() {
		return servers;
	}
	public void setServer(Set<Server> server) {
		this.servers = server;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public void add(Server server){
		this.servers.add(server);
	}
	public ServerQuery(){
		
	}
	public String toString(){
		return servers.toString()+" "+query;
	}
	
	public String rewrite(String suffix){
		Server server=null;
		if(rewritten){
			return query;
		}
		if(servers.size()==0){
			System.out.println("Rewritting not possible");
		}
		else{
			try {
				server=servers.iterator().next();
				this.query=rewrite(query,suffix,server);
				rewritten=true;
				servers=new HashSet<Server>();
				servers.add(server);
			} catch (JSQLParserException e) {
				System.out.println("Rewritting not possible");
			}
		}
		
		return query;
	}
	
	private static String rewrite(String query,String suffix,Server server) throws JSQLParserException{
		CCJSqlParserManager sqlparser = new CCJSqlParserManager();
		
		List<String> allMatches = new ArrayList<String>();
		Matcher m=Pattern.compile(placeholder+"(.*?)"+placeholder).matcher(query);
		int k=0;
		while (m.find()) {
			   allMatches.add(m.group());
			   query=query.replace(allMatches.get(k), placeholder2+k);
			   k++;
		}
		
		
		List<String> allAliasMatches = new ArrayList<String>();
		Matcher malias=Pattern.compile("as\\s+\\S+\\s+\\(.*?\\)").matcher(query.toLowerCase());
		int kalias=0;
		while (malias.find()) {
			   allAliasMatches.add(malias.group());
			   query=query.replace(allAliasMatches.get(kalias), aliasplaceholder+kalias);
			   kalias++;
			   
		}
		
		Matcher msubstring=Pattern.compile("substring\\s*?\\(.*?from.*?for.*?\\)").matcher(query.toLowerCase());
		while (msubstring.find()) {
			String tmp=msubstring.group();
			String tmp2=msubstring.group();
			tmp2=tmp2.replace("from", ",");
			tmp2=tmp2.replace("for", ",");
			query=query.replace(tmp, tmp2);
			   
		}
		
		net.sf.jsqlparser.statement.Statement st=null;
		try {
			st = sqlparser.parse(new StringReader(query));
		} catch (JSQLParserException e) {
			e.printStackTrace();
			return null;
		}
		if (st instanceof Select) {
			SelectBody selectBody = ((Select) st).getSelectBody();
			HashMap<String,String> aliasToTable=new HashMap<String,String>();
			if (selectBody instanceof PlainSelect) {
				net.sf.jsqlparser.schema.Table table=null;
				if(((PlainSelect) selectBody).getJoins()!=null){
					List<Join> joins=((PlainSelect) selectBody).getJoins();
					for(Join join : joins){
						if (join.getRightItem() instanceof SubSelect) {
							SubSelect sub=(SubSelect) join.getRightItem();
							aliasToTable.put(sub.getAlias().toString(), "subselect");
							String sq=rewrite(sub.getSelectBody().toString(),suffix,server);
							Select s=(Select) sqlparser.parse(new StringReader(sq));
							sub.setSelectBody(s.getSelectBody());					
						}
						if(join.getRightItem() instanceof net.sf.jsqlparser.schema.Table){
							net.sf.jsqlparser.schema.Table table2=(net.sf.jsqlparser.schema.Table) join.getRightItem();
							table2.setName(server.getName()+"_"+table2.getName()+suffix);
						}
					}
				}
				if(((PlainSelect) selectBody).getFromItem() instanceof SubSelect){
					SubSelect sub=(SubSelect) ((PlainSelect) selectBody).getFromItem();
					aliasToTable.put(sub.getAlias().toString(), "subselect");
					String sq=rewrite(sub.getSelectBody().toString(),suffix,server);
					Select s=(Select) sqlparser.parse(new StringReader(sq));
					sub.setSelectBody(s.getSelectBody());

				}
				if(((PlainSelect) selectBody).getWhere()!=null){
					Expression exp=rewriteExp(((PlainSelect) selectBody).getWhere(),suffix,server);
					((PlainSelect) selectBody).setWhere(exp);
				}
				if(((PlainSelect) selectBody).getHaving()!=null){
					Expression exp=rewriteExp(((PlainSelect) selectBody).getHaving(),suffix,server);
					((PlainSelect) selectBody).setHaving(exp);
				}
				if(((PlainSelect) selectBody).getFromItem() instanceof net.sf.jsqlparser.schema.Table){
					table=(net.sf.jsqlparser.schema.Table) ((PlainSelect) selectBody).getFromItem();
					table.setName(server.getName()+"_"+table.getName()+suffix);	
				}
				
			}
			
		}
		query=st.toString();
		for(int j=0;j<k;j++){
			Matcher m2=Pattern.compile(placeholder2+j).matcher(query);
			while(m2.find()){
				 query=query.replace(m2.pattern().toString(), allMatches.get(j));
			 }
		}
		
		for(int j=0;j<kalias;j++){
			Matcher m2=Pattern.compile(aliasplaceholder+j).matcher(query);
			while(m2.find()){
				 query=query.replace(m2.pattern().toString(), allAliasMatches.get(j));
				 query=query.replace(m2.pattern().toString().toLowerCase(), allAliasMatches.get(j));
			 }
		}
		return query.replace(placeholder, " ");
	}
	private static Expression rewriteExp(Expression exp,String suffix,Server server) throws JSQLParserException{
		CCJSqlParserManager sqlparser = new CCJSqlParserManager();
		if(exp==null){
			return exp;
		}
		if (exp instanceof Column) {
			return exp;
		}
		else if (exp instanceof BinaryExpression) {
			Expression left=((BinaryExpression) exp).getLeftExpression();
			Expression right=((BinaryExpression) exp).getRightExpression();
			((BinaryExpression) exp).setLeftExpression(rewriteExp(left,suffix,server));
			((BinaryExpression) exp).setRightExpression(rewriteExp(right,suffix,server));
		}
		else if (exp instanceof Parenthesis) {
			exp=rewriteExp(((Parenthesis) exp).getExpression(),suffix,server);
		}
		else if(exp instanceof InExpression){
			Expression left=((InExpression) exp).getLeftExpression();
			ItemsList right=((InExpression) exp).getRightItemsList();
			((InExpression) exp).setLeftExpression(rewriteExp((Expression)left,suffix,server));
			if(right instanceof Expression){
				((InExpression) exp).setRightItemsList((ItemsList) rewriteExp((Expression)right,suffix,server));
			}
			else if(right instanceof SubSelect){
				((InExpression) exp).setRightItemsList((ItemsList) rewriteExp((Expression)right,suffix,server));
				
			}
		}
		else if(exp instanceof Between){
			Expression left=((Between) exp).getLeftExpression();
			Expression start=((Between) exp).getBetweenExpressionStart();
			Expression end=((Between) exp).getBetweenExpressionEnd();
			((Between) exp).setLeftExpression(rewriteExp(left,suffix,server));
			((Between) exp).setBetweenExpressionStart(rewriteExp(start,suffix,server));
			((Between) exp).setBetweenExpressionEnd(rewriteExp(end,suffix,server));
		}
		else if(exp instanceof CastExpression){
			Expression left=((CastExpression) exp).getLeftExpression();
			((CastExpression) exp).setLeftExpression(rewriteExp(left,suffix,server));
		}
		else if(exp instanceof Parenthesis){
			Expression e=((Parenthesis) exp).getExpression();
			((Parenthesis) exp).setExpression(rewriteExp(e,suffix,server));
		}
		else if(exp instanceof ExistsExpression){
			Expression right=((ExistsExpression) exp).getRightExpression();
			((ExistsExpression) exp).setRightExpression(rewriteExp(right,suffix,server));
		}

		else if (exp instanceof Function) {
			if(((Function) exp).isAllColumns()){
				return exp;
			}
			else{
				for (Expression e : ((Function) exp).getParameters().getExpressions()) {
					e=rewriteExp(e,suffix,server);
				}
			}

		}
		else if (exp instanceof SubSelect) {
			SubSelect sub=(SubSelect) exp;
			String sq=rewrite(sub.getSelectBody().toString(),suffix,server);
			Select s=(Select) sqlparser.parse(new StringReader(sq));
			sub.setSelectBody(s.getSelectBody());
			exp=sub;
		}
		return exp;
	}
}
