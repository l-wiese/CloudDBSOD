package fragmentation;

import java.io.Serializable;
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
import metadata.Table;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

//this class is used for query rewriting
public class QueryRewriter implements Serializable{
	private static final long serialVersionUID = 2866302973284989912L;

	public QueryRewriter(String suffix, String dbname, HashMap<Attribute, List<Server>> attributeToServer,
			HashMap<Server, List<Attribute>> serverToAttribute, HashMap<String, Table> tablenameToTable,
			Server orig_owner, Server frag_owner) {
		super();
		this.suffix = suffix;
		this.dbname = dbname;
		this.attributeToServer = attributeToServer;
		this.serverToAttribute = serverToAttribute;
		this.tablenameToTable = tablenameToTable;
		this.orig_owner = orig_owner;
		this.frag_owner = frag_owner;
	}

	private String suffix = "";
	private String dbname = "";
	private HashMap<Attribute, List<Server>> attributeToServer = new HashMap<Attribute, List<Server>>();
	private HashMap<Server, List<Attribute>> serverToAttribute = new HashMap<Server, List<Attribute>>();
	private HashMap<String, Table> tablenameToTable = new HashMap<String, Table>();

	private final String placeholder = "XYZPLACEHOLDERXYZ";
	private final String placeholder2 = "XYZPLACEHOLDERXYZ2";
	private final String aliasplaceholder = "AS ALIASPLACEHOLDERALIAS";
	private final String substringplaceholder = "SUBSTRINGPLACEHOLDERSUBSTRING";

	private Server orig_owner = null;
	private Server frag_owner = null;

	
	//complicated functions necessary for rewriting the queries
	public String parse(String query, boolean fdw) throws Exception {
		return parse2(query, fdw, "", false);
	}

	private  String parse2(String query, boolean fdw, String al, boolean exclude_where) throws Exception {
		return parse3(query, fdw, al, exclude_where, false);
	}

	public ServerQuery evaluableOnSingleServer(String query) throws JSQLParserException {
		return evaluableOnSingleServer(query, true);
	}

	private  ServerQuery evaluableOnSingleServer(String query, boolean rewrite) throws JSQLParserException {
		String original_query = query;
		Set<Server> servers = new HashSet<Server>(serverToAttribute.keySet());

		CCJSqlParserManager sqlparser = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement st = null;
		List<String> allMatches = new ArrayList<String>();
		Matcher m = Pattern.compile(placeholder + "(.*?)" + placeholder).matcher(query);
		int k = 0;
		while (m.find()) {
			allMatches.add(m.group());
			query = query.replace(allMatches.get(k), placeholder2 + k);
			k++;
		}

		List<String> allAliasMatches = new ArrayList<String>();
		Matcher malias = Pattern.compile("as\\s+\\S+\\s+\\(.*?\\)").matcher(query.toLowerCase());
		int kalias = 0;
		while (malias.find()) {
			allAliasMatches.add(malias.group());
			query = query.replace(allAliasMatches.get(kalias), aliasplaceholder + kalias);
			kalias++;

		}

		Matcher msubstring = Pattern.compile("substring\\s*?\\(.*?from.*?for.*?\\)").matcher(query.toLowerCase());
		while (msubstring.find()) {
			String tmp = msubstring.group();
			String tmp2 = msubstring.group();
			tmp2 = tmp2.replace("from", ",");
			tmp2 = tmp2.replace("for", ",");
			query = query.replace(tmp, tmp2);
		}

		if (query.toLowerCase().contains("drop") && query.toLowerCase().contains("view")) {
			return null;
		}
		try {
			st = sqlparser.parse(new StringReader(query));
		} catch (JSQLParserException e) {
			//System.out.println("Query not parsable: "+query);
			//System.out.println(e.getMessage());
			return null;
		}
		if (st instanceof CreateView) {

			return null;
		}
		if (st instanceof Select) {

			SelectBody selectBody = ((Select) st).getSelectBody();
			HashMap<String, String> aliasToTable = new HashMap<String, String>();
			if (selectBody instanceof PlainSelect) {
				PlainSelect select = (PlainSelect) selectBody;
				Set<Attribute> necessaryAttributes = new HashSet<Attribute>();
				net.sf.jsqlparser.schema.Table table = null;
				if (((PlainSelect) selectBody).getJoins() != null) {
					List<Join> joins = ((PlainSelect) selectBody).getJoins();
					for (Join join : joins) {
						if (join.getRightItem() instanceof SubSelect) {
							SubSelect sub = (SubSelect) join.getRightItem();
							if (sub.getAlias() != null) {
								aliasToTable.put(sub.getAlias().toString(), "subselect");
								ServerQuery sq = evaluableOnSingleServer(sub.getSelectBody().toString());
								if (sq == null) {
									return null;
								}
								servers.retainAll(sq.getServer());
								if (servers.isEmpty()) {
									return null;
								}
							} else {
								return null;
							}

						}
						if (join.getRightItem() instanceof net.sf.jsqlparser.schema.Table) {
							net.sf.jsqlparser.schema.Table table2 = (net.sf.jsqlparser.schema.Table) join
									.getRightItem();
							if (!tablenameToTable.containsKey(table2.getName().toLowerCase())) {
								return null;
							}
							if (table2.getAlias() != null) {
								aliasToTable.put(table2.getAlias().toString(), table2.getName());
							} else {
								aliasToTable.put(table2.getName(), table2.getName());
							}
						}
					}
				}
				if (((PlainSelect) selectBody).getIntoTables() != null)
					return null;
				if (((PlainSelect) selectBody).getFromItem() instanceof SubSelect) {
					SubSelect sub = (SubSelect) ((PlainSelect) selectBody).getFromItem();
					if (sub.getAlias() != null) {
						aliasToTable.put(sub.getAlias().toString(), "subselect");
						ServerQuery sq = evaluableOnSingleServer(sub.getSelectBody().toString());
						if (sq == null) {
							return null;
						}
						servers.retainAll(sq.getServer());
						if (servers.isEmpty()) {
							return null;
						}
					} else {
						return null;
					}
					if (servers.size() == 0) {
						return null;
					}
					return new ServerQuery(servers, original_query);
				}
				if (((PlainSelect) selectBody).getFromItem() instanceof net.sf.jsqlparser.schema.Table) {

					table = (net.sf.jsqlparser.schema.Table) select.getFromItem();
					if (!tablenameToTable.containsKey(table.getName().toLowerCase())) {
						return null;
					}
					if (table.getAlias() != null) {
						aliasToTable.put(table.getAlias().toString(), table.getName());
					} else {
						aliasToTable.put(table.getName(), table.getName());
					}
					necessaryAttributes.addAll(getAttributes(select, aliasToTable));
					Expression having = select.getHaving();
					Expression where = select.getWhere();
					Set<Server> possible_servers = getServers(new ArrayList<Attribute>(necessaryAttributes));
					servers.retainAll(possible_servers);
					if (((PlainSelect) selectBody).getWhere() != null) {
						servers.retainAll(getServersFromExp(where));
					}
					if (((PlainSelect) selectBody).getHaving() != null) {
						servers.retainAll(getServersFromExp(having));
					}

					if (servers.size() == 0) {
						return null;
					}
					return new ServerQuery(servers, original_query);

				}
			}

		}
		return null;
	}

	private Set<Server> getServers(List<Attribute> attributes) {
		Set<Server> res = new HashSet<Server>();
		for (Server server : serverToAttribute.keySet()) {
			if (serverToAttribute.get(server).containsAll(attributes)) {
				res.add(server);
			}
		}
		return res;
	}

	private Set<Attribute> getAttributes(PlainSelect exp, HashMap<String, String> aliasToTable) {
		Set<Attribute> attributes = new HashSet<Attribute>();
		PlainSelect sb = (PlainSelect) exp;
		List<SelectItem> select = sb.getSelectItems();
		for (SelectItem si : select) {
			if (si instanceof SelectExpressionItem) {
				attributes.addAll(getAttributesFromExp(((SelectExpressionItem) si).getExpression(), aliasToTable));
			}
			if (si instanceof AllColumns) {
				for (String tablename : aliasToTable.values()) {
					if (tablename.equals("subselect"))
						continue;
					Table table = tablenameToTable.get(tablename);
					for (Attribute attr : table.getAttributes()) {
						attributes.add(attr);
					}
				}
			}
		}
		Expression where = sb.getWhere();
		Expression having = sb.getHaving();
		attributes.addAll(getAttributesFromExp(where, aliasToTable));
		attributes.addAll(getAttributesFromExp(having, aliasToTable));

		return attributes;
	}

	private Set<Server> getServersFromExp(Expression exp) {
		Set<Server> servers = new HashSet<Server>(serverToAttribute.keySet());
		if (exp == null) {
			return servers;
		}
		if (exp instanceof Column) {
			return servers;
		} else if (exp instanceof BinaryExpression) {
			servers.retainAll(getServersFromExp(((BinaryExpression) exp).getLeftExpression()));
			servers.retainAll(getServersFromExp(((BinaryExpression) exp).getRightExpression()));
		} else if (exp instanceof Parenthesis) {
			servers.retainAll(getServersFromExp(((Parenthesis) exp).getExpression()));
		} else if (exp instanceof InExpression) {
			Expression left = ((InExpression) exp).getLeftExpression();
			ItemsList right = ((InExpression) exp).getRightItemsList();
			servers.retainAll(getServersFromExp(left));
			if (right instanceof SubSelect) {
				servers.retainAll(getServersFromExp((Expression) right));
			}
		} else if (exp instanceof Between) {
			Expression left = ((Between) exp).getLeftExpression();
			Expression start = ((Between) exp).getBetweenExpressionStart();
			Expression end = ((Between) exp).getBetweenExpressionEnd();
			servers.retainAll(getServersFromExp(left));
			servers.retainAll(getServersFromExp(start));
			servers.retainAll(getServersFromExp(end));
		} else if (exp instanceof CastExpression) {
			Expression left = ((CastExpression) exp).getLeftExpression();
			servers.retainAll(getServersFromExp(left));
		} else if (exp instanceof Parenthesis) {
			Expression e = ((Parenthesis) exp).getExpression();
			servers.retainAll(getServersFromExp(e));
		} else if (exp instanceof ExistsExpression) {
			Expression right = ((ExistsExpression) exp).getRightExpression();
			servers.retainAll(getServersFromExp(right));
		}

		else if (exp instanceof Function) {
			if (((Function) exp).isAllColumns()) {
				return servers;
			} else {
				for (Expression e : ((Function) exp).getParameters().getExpressions()) {
					servers.retainAll(getServersFromExp(e));
				}
			}

		} else if (exp instanceof SubSelect) {
			ServerQuery sq = null;
			try {
				sq = evaluableOnSingleServer(((SubSelect) exp).getSelectBody().toString());
			} catch (JSQLParserException e) {
				System.out.println(e.getMessage());
			}
			if (sq == null) {
				servers = new HashSet<Server>();
			} else {
				servers.retainAll(sq.getServer());
			}
		}
		return servers;
	}

	private  Set<Attribute> getAttributesFromExp(Expression exp, HashMap<String, String> aliasToTable) {
		Set<Attribute> attributes = new HashSet<Attribute>();
		if (exp == null) {
			return attributes;
		}
		if (exp instanceof Column) {

			String fully_qualified = ((Column) exp).getFullyQualifiedName();
			String alias = "";
			if (fully_qualified.contains(".")) {
				alias = ((Column) exp).getFullyQualifiedName().split("\\.")[0];
			}
			if (!alias.equals("")) {
				if (aliasToTable.get(alias) != null) {
					String tablename = aliasToTable.get(alias);
					if (tablename.equals("subselect")) {
						return attributes;
					} else {
						if (attributeToServer.keySet()
								.contains((new Attribute(((Column) exp).getColumnName(), tablename.toLowerCase())))) {
							attributes.add((new Attribute(((Column) exp).getColumnName(), tablename)));
						}
					}
				}
			} else {
				for (String tablename : aliasToTable.values()) {
					if (attributeToServer.keySet()
							.contains((new Attribute(((Column) exp).getColumnName(), tablename.toLowerCase())))) {
						attributes.add((new Attribute(((Column) exp).getColumnName(), tablename)));
					}
				}
			}
		} else if (exp instanceof BinaryExpression) {
			attributes.addAll(getAttributesFromExp(((BinaryExpression) exp).getLeftExpression(), aliasToTable));
			attributes.addAll(getAttributesFromExp(((BinaryExpression) exp).getRightExpression(), aliasToTable));
		} else if (exp instanceof Parenthesis) {
			attributes.addAll(getAttributesFromExp(((Parenthesis) exp).getExpression(), aliasToTable));
		} else if (exp instanceof InExpression) {
			Expression left = ((InExpression) exp).getLeftExpression();
			ItemsList right = ((InExpression) exp).getRightItemsList();
			attributes.addAll(getAttributesFromExp(left, aliasToTable));
			if (right instanceof SubSelect) {

			} else {
				if (right instanceof Expression) {
					attributes.addAll(getAttributesFromExp((Expression) right, aliasToTable));
				}
				if (right instanceof SubSelect) {

				}
			}
		} else if (exp instanceof Between) {
			Expression left = ((Between) exp).getLeftExpression();
			Expression start = ((Between) exp).getBetweenExpressionStart();
			Expression end = ((Between) exp).getBetweenExpressionEnd();
			attributes.addAll(getAttributesFromExp(left, aliasToTable));
			attributes.addAll(getAttributesFromExp(start, aliasToTable));
			attributes.addAll(getAttributesFromExp(end, aliasToTable));
		} else if (exp instanceof CastExpression) {
			Expression left = ((CastExpression) exp).getLeftExpression();
			attributes.addAll(getAttributesFromExp(left, aliasToTable));
		} else if (exp instanceof Parenthesis) {
			Expression e = ((Parenthesis) exp).getExpression();
			attributes.addAll(getAttributesFromExp(e, aliasToTable));
		} else if (exp instanceof ExistsExpression) {
			Expression right = ((ExistsExpression) exp).getRightExpression();
			attributes.addAll(getAttributesFromExp(right, aliasToTable));
		}

		else if (exp instanceof Function) {
			if (((Function) exp).isAllColumns()) {
				for (String tablename : aliasToTable.values()) {
					if (tablename.equals("subselect"))
						continue;
					Table table = tablenameToTable.get(tablename);
					for (Attribute attr : table.getAttributes()) {
						attributes.add(attr);
					}

				}
			} else {
				for (Expression e : ((Function) exp).getParameters().getExpressions()) {
					attributes.addAll(getAttributesFromExp(e, aliasToTable));
				}
			}

		}
		return attributes;
	}

	private  String parse3(String query, boolean fdw, String al, boolean exclude_where, boolean istable) throws Exception {
		if (al == null) {
			al = "";
		}
		String qu = "";
		String into = "";
		String top = "";
		String where = "";
		CCJSqlParserManager sqlparser = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement st = null;
		CreateView createView = null;
		List<String> allMatches = new ArrayList<String>();
		Matcher m = Pattern.compile(placeholder + "(.*?)" + placeholder).matcher(query);
		int k = 0;
		while (m.find()) {
			allMatches.add(m.group());
			query = query.replace(allMatches.get(k), placeholder2 + k);
			k++;
		}

		List<String> allAliasMatches = new ArrayList<String>();
		Matcher malias = Pattern.compile("as\\s+\\S+\\s+\\(.*?\\)").matcher(query.toLowerCase());
		int kalias = 0;
		while (malias.find()) {
			allAliasMatches.add(malias.group());
			query = query.replace(allAliasMatches.get(kalias), aliasplaceholder + kalias);
			kalias++;

		}

		Matcher msubstring = Pattern.compile("substring\\s+\\(.*?from.*?for.*?\\)").matcher(query.toLowerCase());
		while (msubstring.find()) {
			String tmp = msubstring.group();
			String tmp2 = msubstring.group();
			tmp2 = tmp2.replace("from", ",");
			tmp2 = tmp2.replace("for", ",");
			query = query.replace(tmp, tmp2);

		}
		
		boolean parsed = false;

		if (!parsed) {
			try {
				st = sqlparser.parse(new StringReader(query));
				parsed = true;
			} catch (Exception e) {

			}
		}

		if (!parsed) {
			String tmp = query;
			tmp = tmp.replaceAll("\\s+", "");
			if (tmp.toLowerCase().contains("dropview")) {
				parsed = true;
				return query;
			}
		}
		if (!parsed) {
			//System.out.println("Could not parse query: " + query);
			st = sqlparser.parse(new StringReader(query));
		}
		if(st instanceof CreateView){
			try {
				createView = (CreateView) st;
				parsed = true;
				if (createView.isOrReplace()) {
					qu = "CREATE OR REPLACE VIEW " + createView.getView().getName() + "";
				} else {
					qu = "CREATE VIEW " + createView.getView().getName() + "";
				}
				if (createView.getColumnNames() != null && createView.getColumnNames().size() > 0) {
					qu += "(";
					for (int i = 0; i < createView.getColumnNames().size(); i++) {
						qu += createView.getColumnNames().get(i) + " ,";
					}
					qu = qu.substring(0, qu.length() - 1) + ")";
				}
				qu += " AS (" + parse2(createView.getSelectBody().toString() + ";", fdw, "", exclude_where)+")";
				return qu;
			} catch (Exception e1) {

			}
		}
		if (st instanceof Select) {
			SelectBody selectBody = ((Select) st).getSelectBody();
			if (selectBody instanceof PlainSelect) {
				PlainSelect plainSelect = (PlainSelect) selectBody;
				List<Join> joins = plainSelect.getJoins();
				if(plainSelect.getDistinct()!=null){
					qu += "SELECT DISTINCT ";
				}else{
					qu += "SELECT ";
				}
				for (int i = 0; i < plainSelect.getSelectItems().size(); i++) {
					qu += plainSelect.getSelectItems().get(i) + ",";
				}
				if (plainSelect.getSelectItems().size() > 0) {
					qu = qu.substring(0, qu.length() - 1);
				}

				if (plainSelect.getWhere() != null) {
					where = " WHERE " + parseWhere(plainSelect.getWhere(), fdw) + " ";
				}

				if (((PlainSelect) selectBody).getTop() != null) {
					top += " " + ((PlainSelect) selectBody).getTop() + " ";
				}

				if (((PlainSelect) selectBody).getIntoTables() != null
						&& !((PlainSelect) selectBody).getIntoTables().isEmpty()) {
					into += " INTO ";
					for (net.sf.jsqlparser.schema.Table table : ((PlainSelect) selectBody).getIntoTables()) {
						into += " " + table + ",";
					}
					into = into.substring(0, into.length() - 1);
				}

				if (joins != null) {
					qu += " FROM ";
					if (plainSelect.getFromItem() instanceof SubSelect) {
						SubSelect subSelect = (SubSelect) plainSelect.getFromItem();
						qu += "(" + parse2(subSelect.getSelectBody() + ";", fdw, "", exclude_where) + ") ";
						if (subSelect.getAlias() != null) {
							qu += " " + subSelect.getAlias();
						}

					}
					if (plainSelect.getFromItem() instanceof net.sf.jsqlparser.schema.Table) {
						net.sf.jsqlparser.schema.Table table = (net.sf.jsqlparser.schema.Table) plainSelect
								.getFromItem();
						String tmp_al = "";
						if (table.getAlias() != null) {
							tmp_al = table.getAlias().toString();
						}
						if (tablenameToTable.get(table.getName().toLowerCase()) == null) {
							qu+=table.getName()+" ";
						} else {
							String tablealias=null;
							if(table.getAlias()!=null){
								tablealias=table.getAlias().getName();
							}
							Set<String> selectitemStrings=getAttributes(plainSelect,frag_owner,false,table.getName().toLowerCase(),tablealias);
							String selectitems="  *  ";
							if(selectitemStrings!=null){
								for(Join join:joins){
									if(table.getAlias()!=null){
										selectitemStrings.addAll(getAttributesFromExp(join.getOnExpression(),table.getName().toLowerCase(), table.getAlias().getName(),frag_owner,false));
									}
									else{
										selectitemStrings.addAll(getAttributesFromExp(join.getOnExpression(),table.getName().toLowerCase(), null,frag_owner,false));
									}
								}
								selectitems=selectitemStrings.toString();
								selectitems=selectitems.substring(1, selectitems.length()-1);
								if(selectitems.equals("")){
									selectitems=" * ";
								}
							}
							qu += "(" + parse3("SELECT "+selectitems +" FROM " + table.getName().toLowerCase() + " " + where + ";", fdw,
									tmp_al, true, true) + ")";
						}

						if (table.getAlias() == null) {
							qu += "AS " + table.getName();
						}

						if (table.getAlias() != null) {
							qu += " " + table.getAlias();
						}
					}
					for (Join join : joins) {
						if (join.isFull()) {
							qu += " FULL ";
						}
						if (join.isLeft()) {
							qu += " LEFT ";
						}
						if (join.isRight()) {
							qu += " RIGHT ";
						}
						if (join.isSimple()) {
							qu += ",";
						}
						if (join.isCross()) {
							qu += " CROSS ";
						}
						if (join.isNatural()) {
							qu += " NATURAL ";
						}
						if (join.isSemi()) {
							qu += " SEMI ";
						}
						if (join.isInner()) {
							qu += " INNER ";
						}
						if (join.isOuter()) {
							qu += " OUTER ";
						}
						if (!join.isSimple()) {
							qu += " JOIN ";
						}
						if (join.getRightItem() instanceof SubSelect) {
							SubSelect subSelect = (SubSelect) join.getRightItem();
							qu += "(" + parse2(subSelect.getSelectBody() + ";", fdw, null, exclude_where) + ") ";
							if (subSelect.getAlias() != null) {
								qu += " " + subSelect.getAlias();
							}
							if (join.getOnExpression() != null) {
								qu += " ON " + join.getOnExpression();
							}

						}
						if (join.getRightItem() instanceof net.sf.jsqlparser.schema.Table) {
							net.sf.jsqlparser.schema.Table table = (net.sf.jsqlparser.schema.Table) join.getRightItem();
							String tmp_al = "";
							if (table.getAlias() != null) {
								tmp_al = table.getAlias().toString();
							}
							if (tablenameToTable.get(table.getName().toLowerCase()) == null) {
								qu+=table.getName()+" ";
							} else {
								String tablealias=null;
								if(table.getAlias()!=null){
									tablealias=table.getAlias().getName();
								}
								Set<String> selectitemStrings=getAttributes(plainSelect,frag_owner,false,table.getName().toLowerCase(),tablealias);
								String selectitems="  *  ";
								if(selectitemStrings!=null ){
									if(table.getAlias()!=null){
										selectitemStrings.addAll(getAttributesFromExp(join.getOnExpression(),table.getName().toLowerCase(),table.getAlias().getName(),frag_owner,false));
									}
									else{
										selectitemStrings.addAll(getAttributesFromExp(join.getOnExpression(),table.getName().toLowerCase(),null,frag_owner,false));

									}
									selectitems=selectitemStrings.toString();
									selectitems=selectitems.substring(1, selectitems.length()-1);
									if(selectitems.equals("")){
										selectitems=" * ";
									}
								}
								qu += "(" + parse3("SELECT "+selectitems +" FROM " + table.getName().toLowerCase() + " " + where + ";",
										fdw, tmp_al, true, true) + ")";
							}
							if (table.getAlias() == null) {
								qu += "AS " + table.getName();
							}

							if (table.getAlias() != null) {
								qu += " " + table.getAlias();
							}
							if (join.getOnExpression() != null) {
								qu += " ON " + join.getOnExpression();
							}
						}

					}
					qu += where;

				} else {
					Object o = plainSelect.getFromItem();
					if (o instanceof SubSelect) {
						qu += " FROM ("
								+ parse2(((SubSelect) o).getSelectBody().toString() + ";", fdw, "", exclude_where);
						qu += ") ";
						if (((SubSelect) o).getAlias() != null) {
							qu += " " + ((SubSelect) o).getAlias() + " ";
						}
						qu += where;

					} else {
						String qu2 = qu + " FROM " + plainSelect.getFromItem() + " ";
						qu2 += where;
							if (((PlainSelect) selectBody).getHaving() != null) {
								Expression having = ((PlainSelect) selectBody).getHaving();
								qu = parseWithoutJoin(qu2 + ";", top, into, al, exclude_where, istable, having,
										fdw);
							} else {
								qu = parseWithoutJoin(qu2 + ";", top, into, al, exclude_where, istable, null, fdw);
							}
					}
				}

			}

			if (((PlainSelect) selectBody).getGroupByColumnReferences() != null
					&& !((PlainSelect) selectBody).getGroupByColumnReferences().isEmpty()) {
				qu += " GROUP BY ";
				for (Expression expr : ((PlainSelect) selectBody).getGroupByColumnReferences()) {
					qu += " " + parseWhere(expr, fdw) + ",";
				}
				qu = qu.substring(0, qu.length() - 1);
			}
			if (((PlainSelect) selectBody).getHaving() != null) {
				qu += " HAVING " + parseWhere(((PlainSelect) selectBody).getHaving(), fdw);
			}
			if (((PlainSelect) selectBody).getOrderByElements() != null
					&& !((PlainSelect) selectBody).getOrderByElements().isEmpty()) {
				qu += " " + (PlainSelect.orderByToString(((PlainSelect) selectBody).getOrderByElements()));
			}

			if (((PlainSelect) selectBody).getLimit() != null) {
				qu += " " + ((PlainSelect) selectBody).getLimit();
			}
		}
		for (int j = 0; j < k; j++) {
			Matcher m2 = Pattern.compile(placeholder2 + j).matcher(qu);
			while (m2.find()) {
				qu = qu.replace(m2.pattern().toString(), allMatches.get(j));
			}
		}

		for (int j = 0; j < kalias; j++) {
			Matcher m2 = Pattern.compile(aliasplaceholder + j).matcher(qu);
			while (m2.find()) {
				qu = qu.replace(m2.pattern().toString(), allAliasMatches.get(j));
				qu = qu.replace(m2.pattern().toString().toLowerCase(), allAliasMatches.get(j));
			}
		}

		return qu.replaceAll("\\s{2,}", " ").trim().replace(placeholder, " ");
	}

	private String parseWithoutJoin(String query, String top, String into, String al, boolean exclude_where,
			boolean isTable, Expression having, boolean fdw) throws Exception {
		PlainSelect st = null;
		CCJSqlParserManager sqlparser = new CCJSqlParserManager();
		net.sf.jsqlparser.statement.Statement statement = null;
		List<String> allMatches = new ArrayList<String>();
		Matcher m = Pattern.compile(placeholder + "(.*?)" + placeholder).matcher(query);
		int k = 0;
		while (m.find()) {
			allMatches.add(m.group());
			query = query.replace(allMatches.get(k), placeholder2 + k);
			k++;
		}

		try {
			statement = sqlparser.parse(new StringReader(query));
		} catch (Exception e) {
			System.out.println(allMatches);
			throw (e);
		}
		if (statement instanceof Select) {
			SelectBody selectBody = ((Select) statement).getSelectBody();
			if (selectBody == null) {
				return query;
			}
			if (selectBody instanceof PlainSelect) {
				st = (PlainSelect) selectBody;
			}
		}
		String select = "";
		String from = "";
		if (!isTable) {
			select = "SELECT ";
			for (int i = 0; i < st.getSelectItems().size(); i++) {
				select += st.getSelectItems().get(i).toString().toLowerCase() + " ,";
			}

			select = select.substring(0, select.length() - 1);
			from = " FROM " + placeholder;
		}

		String where = "";
		if (st.getWhere() != null && !exclude_where) {
			where = " WHERE " + st.getWhere();
		}

		List<Attribute> attributes = new ArrayList<Attribute>();
		Object o = st.getFromItem();
		String fromTable = "";
		net.sf.jsqlparser.schema.Table fromItem = null;
		try {
			fromItem = (net.sf.jsqlparser.schema.Table) o;
		} catch (Exception e) {
			throw (e);
		}
		String tablename = fromItem.getName().toLowerCase();
		String tablealias =null; 
		if(fromItem.getAlias()!=null){
			tablealias=fromItem.getAlias().getName();
		}
		Table table = tablenameToTable.get(tablename.toLowerCase());
		if (table == null) {
			return " " + st.toString() + " ";
		}
		String select2 = "Select ";

		HashMap<Server, String> serverToSelect = new HashMap<Server, String>();
		HashMap<Server, String> serverToAs = new HashMap<Server, String>();
		HashMap<Server, List<Attribute>> StoA = new HashMap<Server, List<Attribute>>();
		Set<String> necessary_attribute_names = getAttributes(st, orig_owner);
		if (having != null) {
			necessary_attribute_names.addAll(getAttributesFromExp(having, tablename,tablealias ,orig_owner, false));

		}
		Set<Attribute> necessary_attributes = new HashSet<Attribute>();

		for (String s : necessary_attribute_names) {
			Attribute attr = new Attribute(s, tablename);
			necessary_attributes.add(attr);
		}

		HashMap<Attribute, Server> assignment = getAssignment(new ArrayList<Attribute>(necessary_attributes));
		for (String s : necessary_attribute_names) {
			Attribute attr = new Attribute(s, tablename);
			if (!attributes.contains(attr)) {
				Server server = null;
				try {
					server = assignment.get(attr);

				} catch (Exception e) {
					continue;
				}
				if (!serverToSelect.containsKey(server)) {
					if(fdw){
						select2 = " (Select ";
						serverToSelect.put(server, select2);
						String as = " AS " + tablename + server.getName();
						serverToAs.put(server, as);
					}
					else{
						select2 = "dblink('host=" + server.getHost() + " port=" + server.getPort() + " user="
								+ server.getUsername() + " password=" + server.getPassword() + " dbname=" + dbname + suffix
								+ "',' Select ";
						serverToSelect.put(server, select2);
						String as = " AS " + tablename + server.getName() + "(";
						serverToAs.put(server, as);
					}

					int index = serverToAttribute.get(server).indexOf(attr);
					attr = serverToAttribute.get(server).get(index);

				}
				if (StoA.get(server) == null) {
					StoA.put(server, new ArrayList<Attribute>());
				}
				StoA.get(server).add(attr);
				int index = serverToAttribute.get(server).indexOf(attr);
				attr = serverToAttribute.get(server).get(index);
				if(fdw){
					serverToAs.put(server, serverToAs.get(server) + "");
				}else{
					serverToAs.put(server,serverToAs.get(server) + attr.getName() + " " + attr.getDomain().getSQLType() + ",");
				}
				serverToSelect.put(server, serverToSelect.get(server) + attr.getName() + ",");
			}
		}
		for (int i = 0; i < table.getTid().size(); i++) {
			Attribute attr = table.getTid().get(i);
			List<Server> servers = attributeToServer.get(attr);
			for (Server server : servers) {
				if (StoA.keySet().contains(server) && !StoA.get(server).contains(attr)) {
					if (!serverToSelect.containsKey(server)) {
						if (fdw) {
							select2 = " (Select ";
							serverToSelect.put(server, select2);
							String as = " AS " + tablename + server.getName();
							serverToAs.put(server, as);
						} else {
							select2 = "dblink('host=" + server.getHost() + " port=" + server.getPort() + " user="
									+ server.getUsername() + " password=" + server.getPassword() + " dbname=" + dbname
									+ suffix + "',' Select ";
							serverToSelect.put(server, select2);
							String as = " AS " + tablename + server.getName() + "(";
							serverToAs.put(server, as);
						}
						

					}
					if (StoA.get(server) == null) {
						StoA.put(server, new ArrayList<Attribute>());
					}
					StoA.get(server).add(attr);
					int index = serverToAttribute.get(server).indexOf(attr);
					attr = serverToAttribute.get(server).get(index);
					if (fdw) {
						serverToAs.put(server, serverToAs.get(server) + "");
						serverToSelect.put(server, serverToSelect.get(server) + attr.getName() + ",");
					} else {
						serverToAs.put(server,
								serverToAs.get(server) + attr.getName() + " " + attr.getDomain().getSQLType() + ",");
						serverToSelect.put(server, serverToSelect.get(server) + attr.getName() + ",");
					}
				}
			}
		}

		boolean flag = true;
		if (serverToAs.keySet().size() > 1) {
			List<Server> joinServers = new ArrayList<Server>();
			for (Server server : serverToAs.keySet()) {
				joinServers.add(server);
				if (joinServers.size() > 2) {
					joinServers.remove(0);
				}
				select2 = serverToSelect.get(server);
				String as = serverToAs.get(server);
				String where2 = getCondition(st.getWhere(), tablename, al, server);
				String having2 = "";
				if (!where2.equals("")) {
					where2 = " WHERE " + where2 + " ";
				}
				if (fdw) {
					serverToSelect.put(server, select2.substring(0, select2.length() - 1) + " FROM " + server.getName()+"_"
							+ tablename + suffix + where2 + having2 + ")");
					serverToAs.put(server, as.substring(0, as.length()) + "");
				} else {
					serverToSelect.put(server, select2.substring(0, select2.length() - 1) + " FROM " + server.getName()+"_"
							+ tablename + suffix + where2 + "')");
					serverToAs.put(server, as.substring(0, as.length() - 1) + ")");
				}

				select2 = serverToSelect.get(server);
				as = serverToAs.get(server);

				serverToSelect.put(server, select2 + as);
				if (flag) {
					flag = false;
					fromTable += " " + serverToSelect.get(server);
				} else {
					String where3 = "";
					if (!(serverToAs.keySet().size() > 1)) {
						where3 = "";
					} else {
						boolean flag2 = true;
						for (int i = 0; i < table.getTid().size(); i++) {
							Attribute attr = table.getTid().get(i);
							List<Server> serv = joinServers;
							for (int j = 0; j < joinServers.size() - 1; j++) {
								if (flag2) {
									where3 += "";
									flag2 = false;
								} else {
									where3 += " AND ";
								}
								where3 += tablename + serv.get(j).getName() + "." + attr.getName() + "=" + tablename
										+ serv.get(j + 1).getName() + "." + attr.getName();

							}
						}

					}
					fromTable += " INNER JOIN  " + serverToSelect.get(server) + " ON " + where3;

				}

			}
		} else {
			Server server = serverToAs.keySet().iterator().next();
			select2 = serverToSelect.get(server);
			String as = serverToAs.get(server);

			String where2 = getCondition(st.getWhere(), tablename, al, server);
			String having2 = "";
			if (!where2.equals("")) {
				where2 = " WHERE " + where2 + " ";
			}
			if(fdw){
			serverToSelect.put(server, select2.substring(0, select2.length() - 1) + " FROM " + server.getName()+"_"
					+ tablename + suffix + where2 + having2 + ")");
			serverToAs.put(server, as.substring(0, as.length()) + "");
			}
			else{
				serverToSelect.put(server,
						select2.substring(0, select2.length() - 1) + " FROM " +server.getName()+"_"+ tablename + suffix + where2 + "')");
				serverToAs.put(server, as.substring(0, as.length() - 1) + ")");
			}
			select2 = serverToSelect.get(server);
			as = serverToAs.get(server);
			serverToSelect.put(server, select2 + as);
			fromTable += " " + serverToSelect.get(server);
		}

		String as = " ";
		attributes = new ArrayList<Attribute>();
		String which = " ";
		for (Server server : StoA.keySet()) {
			List<Attribute> attrs = StoA.get(server);
			for (Attribute attr : attrs) {
				if (attributes.contains(attr)) {
					continue;
				}
				if (as.equals(" ")) {
					if (st.getFromItem().getAlias() == null) {
						as += "AS " + tablename;
					} else {

						as = "AS " + st.getFromItem().getAlias().getName();
					}

				} else {

				}
				which += tablename + server.getName() + "." + attr.getName() + " AS " + attr.getName() + ",";
				attributes.add(attr);
			}
		}
		which = which.substring(0, which.length() - 1) + " FROM";
		fromTable = "select " + which + fromTable;
		
		if(!isTable){
			from += "(" + fromTable + " ) " + as + ",";
		}
		else{
			from += "(" + fromTable + " ) ";
		}

		from = from.substring(0, from.length() - 1);
		String sql = select + top + into + from + " " + placeholder + where;

		for (int j = 0; j < k; j++) {
			Matcher m2 = Pattern.compile(placeholder2 + j).matcher(sql);
			while (m2.find()) {
				sql = sql.replace(m2.pattern().toString(), allMatches.get(j));
			}
		}
		if(fdw){
			return sql.replace("''", "'");
		}
		else{
			return sql;
		}
	}

	public HashMap<Attribute, Server> getAssignment(List<Attribute> attributes) {
		return getAssignment(attributes, false);
	}

	public HashMap<Attribute, Server> getAssignment(List<Attribute> attributes, boolean include) {
		HashMap<Attribute, Server> assignment = new HashMap<Attribute, Server>();
		List<Attribute> tmpattrs = new ArrayList<Attribute>(attributes);
		for (Attribute attr : tmpattrs) {
			try {
				attributeToServer.get(attr).get(0);
			} catch (Exception e) {
				if (!include) {
					attributes.remove(attr);
				} else {
					return null;
				}
			}
		}
		while (!attributes.isEmpty()) {
			Server max = null;
			int max_card = 0;
			List<Attribute> max_attr = null;
			for (Server server : serverToAttribute.keySet()) {
				if (max == null) {
					max = server;
				}
				List<Attribute> tmp = new ArrayList<Attribute>(serverToAttribute.get(server));
				tmp.retainAll(attributes);
				if (tmp.size() > max_card) {
					max_card = tmp.size();
					max = server;
					max_attr = tmp;
				}
			}
			for (Attribute attr : max_attr) {
				attributes.remove(attr);
				assignment.put(attr, max);
			}

		}
		return assignment;
	}

	private String parseWhere(Expression exp, boolean fdw) throws Exception {
		String where = "";
		if (BinaryExpression.class.isAssignableFrom(exp.getClass())) {
			where += " " + parseWhere(((BinaryExpression) exp).getLeftExpression(), fdw) + " ";
			where += ((BinaryExpression) exp).getStringExpression() + " ";
			where += parseWhere(((BinaryExpression) exp).getRightExpression(), fdw) + " ";
		} else if (exp instanceof Column) {
			where += exp.toString();
		} else if (exp instanceof InExpression) {
			if (((InExpression) exp).isNot()) {
				if (((InExpression) exp).getRightItemsList() instanceof SubSelect) {
					where += " " + parseWhere(((InExpression) exp).getLeftExpression(), fdw) + " NOT IN "
							+ parseWhere((Expression) (((InExpression) exp).getRightItemsList()), fdw);
				} else {
					where += " " + parseWhere(((InExpression) exp).getLeftExpression(), fdw) + " NOT IN "
							+ ((InExpression) exp).getRightItemsList();
				}
			} else {
				if (((InExpression) exp).getRightItemsList() instanceof SubSelect) {
					where += " " + parseWhere(((InExpression) exp).getLeftExpression(), fdw) + " IN "
							+ parseWhere((Expression) (((InExpression) exp).getRightItemsList()), fdw);
				} else {
					where += " " + parseWhere(((InExpression) exp).getLeftExpression(), fdw) + " IN "
							+ ((InExpression) exp).getRightItemsList();
				}
			}
		} else if (exp instanceof Between) {
			if (((Between) exp).isNot()) {
				where += " (" + parseWhere(((Between) exp).getLeftExpression(), fdw) + " NOT BETWEEN ";
			} else {
				where += " (" + parseWhere(((Between) exp).getLeftExpression(), fdw) + " BETWEEN ";
			}
			where += parseWhere(((Between) exp).getBetweenExpressionStart(), fdw) + " AND ";
			where += parseWhere(((Between) exp).getBetweenExpressionEnd(), fdw) + ") ";
		} else if (exp instanceof CastExpression) {
			where += " CAST(";
			where += parseWhere(((CastExpression) exp).getLeftExpression(), fdw) + " AS ";
			where += ((CastExpression) exp).getType() + ") ";
		} else if (exp instanceof Parenthesis) {
			where += "(" + parseWhere(((Parenthesis) exp).getExpression(), fdw) + ")";
		} else if (exp instanceof DateTimeLiteralExpression) {
			where += ((DateTimeLiteralExpression) exp).getType() + " ";
			where += ((DateTimeLiteralExpression) exp).getValue() + " ";

		} else if (exp instanceof ExistsExpression) {
			where += ((ExistsExpression) exp).getStringExpression() + "  ";
			where += parseWhere(((ExistsExpression) exp).getRightExpression(), fdw);
			where += " ";
		} else if (exp instanceof IntervalExpression) {
			where += exp;
		} else if (exp instanceof Function) {
			where += ((Function) exp).getName() + "( ";
			ExpressionList params = ((Function) exp).getParameters();
			for (Expression expr : params.getExpressions()) {
				where += parseWhere(expr, fdw) + " ,";
			}
			where = where.substring(0, where.length() - 1);
			where += ") ";
		}

		else if (exp instanceof SubSelect) {
			where += " (" + parse2(((SubSelect) exp).getSelectBody().toString() + ";", fdw, "", false) + ") ";
			if (((SubSelect) exp).getAlias() != null) {
				where += " " + ((SubSelect) exp).getAlias() + " ";
			}

		} else {
			where += exp.toString();
		}
		return where + "";
	}

	private Set<String> getAttributes(PlainSelect exp, Server server) {
		return getAttributes(exp, server, false);
	}
	
	


	private Set<String> getAttributes(PlainSelect exp, Server server, boolean include) {
		Set<String> attributes = new HashSet<String>();
		PlainSelect sb = (PlainSelect) exp;
		List<SelectItem> select = sb.getSelectItems();
		String tablename = ((net.sf.jsqlparser.schema.Table) sb.getFromItem()).getName().toLowerCase();
		String tablealias=null;
		
		if(((net.sf.jsqlparser.schema.Table) sb.getFromItem()).getAlias()!=null){
			tablealias = ((net.sf.jsqlparser.schema.Table) sb.getFromItem()).getName().toLowerCase();
		}
		for (SelectItem si : select) {
			if (si instanceof SelectExpressionItem) {
				attributes.addAll(
						getAttributesFromExp(((SelectExpressionItem) si).getExpression(), tablename,tablealias ,server, include));
			}
			if (si instanceof AllColumns) {
				Table table = tablenameToTable.get(tablename);
				if (table == null) {
					if (include) {
						return null;
					}
					return attributes;
				}
				for (Attribute attr : table.getAttributes()) {
					attributes.add(attr.getName());
				}
			}
		}
		Expression where = sb.getWhere();
		Expression having = sb.getHaving();
		attributes.addAll(getAttributesFromExp(where, tablename, tablealias ,server, include));
		attributes.addAll(getAttributesFromExp(having, tablename,tablealias , server, include));

		return attributes;
	}
	
	private Set<String> getAttributes(PlainSelect exp,String tablename, String alias ,Server server, boolean include) {
		Set<String> attributes = new HashSet<String>();
		PlainSelect sb = (PlainSelect) exp;
		List<SelectItem> select = sb.getSelectItems();
		
		for (SelectItem si : select) {
			if (si instanceof SelectExpressionItem) {
				attributes.addAll(
						getAttributesFromExp(((SelectExpressionItem) si).getExpression(), tablename,alias ,server, include));
			}
			if (si instanceof AllColumns) {
				Table table = tablenameToTable.get(tablename);
				if (table == null) {
					if (include) {
						return null;
					}
					return attributes;
				}
				for (Attribute attr : table.getAttributes()) {
					attributes.add(attr.getName());
				}
			}
		}
		Expression where = sb.getWhere();
		Expression having = sb.getHaving();
		attributes.addAll(getAttributesFromExp(where, tablename,alias ,server, include));
		attributes.addAll(getAttributesFromExp(having, tablename,alias ,server, include));

		return attributes;
	}
	
	private Set<String> getAttributes(PlainSelect exp, Server server, boolean include, String tablename,String alias) {
		Set<String> attributes = new HashSet<String>();
		PlainSelect sb = (PlainSelect) exp;
		List<SelectItem> select = sb.getSelectItems();
		
		for (SelectItem si : select) {
			if (si instanceof SelectExpressionItem) {
				attributes.addAll(
						getAttributesFromExp(((SelectExpressionItem) si).getExpression(), tablename,alias ,server, include));
			}
			if (si instanceof AllColumns) {
				Table table = tablenameToTable.get(tablename);
				if (table == null) {
					if (include) {
						return null;
					}
					return attributes;
				}
				for (Attribute attr : table.getAttributes()) {
					attributes.add(attr.getName());
				}
			}
		}
		Expression where = sb.getWhere();
		Expression having = sb.getHaving();
		attributes.addAll(getAttributesFromExp(where, tablename,alias ,server, include));
		attributes.addAll(getAttributesFromExp(having, tablename,alias ,server, include));

		return attributes;
	}

	private Set<String> getAttributesFromExp(Expression si, String tablename, String alias ,Server server, boolean include) {
		Set<String> attributes = new HashSet<String>();
		if (si == null) {
			return attributes;
		}
		else if (si instanceof BinaryExpression) {
			attributes.addAll(
					getAttributesFromExp(((BinaryExpression) si).getLeftExpression(), tablename,alias ,server, include));
			attributes.addAll(
					getAttributesFromExp(((BinaryExpression) si).getRightExpression(), tablename,alias ,server, include));
		}
		else if (si instanceof Parenthesis) {
			attributes.addAll(getAttributesFromExp(((Parenthesis) si).getExpression(), tablename, alias,server, include));
		}
		else if (si instanceof Column) {
			if (!include) {
					if(((Column) si).getTable()!=null && tablename!=null &&
							!((Column) si).getTable().toString().toLowerCase().equals(tablename.toLowerCase()) && alias!=null &&
							!((Column) si).getTable().toString().toLowerCase().equals(alias.toLowerCase())){
							return attributes;
					}

				if (attributeToServer.keySet().contains((new Attribute(((Column) si).getColumnName(), tablename)))) {
					attributes.add(((Column) si).getColumnName());
					return attributes;
				}
			} else {
				attributes.add(((Column) si).getColumnName());
				return attributes;
			}

		}
		else if (si instanceof Function) {
			if (((Function) si).isAllColumns()) {
				Table table = tablenameToTable.get(tablename);
				for (Attribute attr : table.getAttributes()) {
					attributes.add(attr.getName());
				}
			} else {
				for (Expression e : ((Function) si).getParameters().getExpressions()) {
					attributes.addAll(getAttributesFromExp(e, tablename,alias ,server, include));
				}
			}

		}
		else if(si instanceof InExpression){
			attributes.addAll(getAttributesFromExp(((InExpression) si).getLeftExpression(), tablename, alias ,server, include));
			
		}
		else if(si instanceof CaseExpression){
			attributes.addAll(getAttributesFromExp(((CaseExpression) si).getElseExpression(), tablename, alias ,server, include));
			attributes.addAll(getAttributesFromExp(((CaseExpression) si).getSwitchExpression(), tablename, alias ,server, include));
			for (Expression e : ((CaseExpression) si).getWhenClauses()) {
				attributes.addAll(getAttributesFromExp(e, tablename, alias ,server, include));
			}
		}
		else if(si instanceof WhenClause){
			attributes.addAll(getAttributesFromExp(((WhenClause) si).getThenExpression(), tablename, alias ,server, include));
			attributes.addAll(getAttributesFromExp(((WhenClause) si).getWhenExpression(), tablename, alias ,server, include));
		}
		else if(si instanceof ExtractExpression){
			attributes.addAll(getAttributesFromExp(((ExtractExpression) si).getExpression(), tablename, alias ,server, include));
		}
		else if(si instanceof ExistsExpression){
			attributes.addAll(getAttributesFromExp(((ExistsExpression) si).getRightExpression(), tablename, alias ,server, include));

		}
		else if(si instanceof IntervalExpression){
			
		}
		else if(si instanceof LongValue){
			
		}
		else if(si instanceof DoubleValue){
			
		}
		else if(si instanceof StringValue){
			
		}
		else if(si instanceof Between){
			attributes.addAll(getAttributesFromExp(((Between) si).getBetweenExpressionEnd(), tablename, alias ,server, include));
			attributes.addAll(getAttributesFromExp(((Between) si).getBetweenExpressionStart(), tablename, alias ,server, include));
			attributes.addAll(getAttributesFromExp(((Between) si).getLeftExpression(), tablename, alias ,server, include));
		}
		else if(si instanceof DateTimeLiteralExpression){
			
		}
		else if(si instanceof SubSelect){
			attributes.addAll(getAttributes((PlainSelect)(((SubSelect) si).getSelectBody()), tablename, alias ,server, include));
		}
		else if(si instanceof CastExpression){
			attributes.addAll(getAttributesFromExp(((CastExpression) si).getLeftExpression(), tablename, alias ,server, include));
		}
		
		else{}
		return attributes;
	}

	private String getCondition(Expression exp, String tablename, String al, Server server) {
		String res = "";
		tablename = tablename.toLowerCase();
		if (exp instanceof AndExpression) {
			res += "(" + getCondition(((AndExpression) exp).getLeftExpression(), tablename, al, server);
			res += " AND " + getCondition(((AndExpression) exp).getRightExpression(), tablename, al, server) + ")";
		} else if (exp instanceof OrExpression) {
			res += "(" + getCondition(((OrExpression) exp).getLeftExpression(), tablename, al, server);
			res += " OR " + getCondition(((OrExpression) exp).getRightExpression(), tablename, al, server) + ")";
		} else if (exp instanceof BinaryExpression) {
			Expression left = ((BinaryExpression) exp).getLeftExpression();
			Expression right = ((BinaryExpression) exp).getRightExpression();
			String l = getCondition(left, tablename, al, server);
			String r = getCondition(right, tablename, al, server);
			if (l.equals(" true ") || r.equals(" true ")) {
				return " true ";
			} else {
				String op = ((BinaryExpression) exp).getStringExpression();
				res += " " + l + " " + op + " " + r + " ";
			}
		} else if (exp instanceof Column) {
			if (((Column) exp).getFullyQualifiedName().contains(".")) {
				String al2 = ((Column) exp).getFullyQualifiedName().split("\\.")[0].replace(" ", "");
				al = al.replace(" ", "");
				if (!al2.equals(al)) {
					return " true ";
				}
			}
			if (!serverToAttribute.get(server)
					.contains(new Attribute(((Column) exp).getColumnName().toLowerCase(), tablename))) {
				return " true ";
			} else {
				return " " + ((Column) exp).getColumnName() + " ";
			}
		} else if (exp instanceof LongValue) {
			return exp.toString().replace("'", "''");
		} else if (exp instanceof StringValue) {
			return exp.toString().replace("'", "''");
		} else if (exp instanceof Parenthesis) {
			res += "(" + getCondition(((Parenthesis) exp).getExpression(), tablename, al, server) + ")";

		} else if (exp instanceof Between) {
			String left = getCondition(((Between) exp).getLeftExpression(), tablename, al, server);
			String start = getCondition(((Between) exp).getBetweenExpressionStart(), tablename, al, server);
			String end = getCondition(((Between) exp).getBetweenExpressionEnd(), tablename, al, server);
			if (left.equals(" true ") || start.equals(" true ") || end.equals(" true ")) {
				return " true ";
			}
			if (((Between) exp).isNot()) {
				res += " " + left + " NOT BETWEEN " + start + " AND " + end + " ";
			} else {
				res += " " + left + " BETWEEN " + start + " AND " + end + " ";
			}
			return res;
		}

		else if (exp instanceof InExpression) {
			Expression left = ((InExpression) exp).getLeftExpression();
			String tmp = getCondition(left, tablename, al, server);
			if (tmp.equals(" true ")) {
				return " true ";
			} else {
				if (((InExpression) exp).isNot()) {

					tmp = tmp + " NOT IN (";
				} else {
					tmp += " IN (";
				}
				ItemsList l = ((InExpression) exp).getRightItemsList();
				if (l instanceof ExpressionList) {
					List<Expression> expl = ((ExpressionList) l).getExpressions();
					for (Expression e : expl) {
						String tmp2 = getCondition(e, tablename, al, server);
						if (tmp2.equals(" true ")) {
							return " true ";
						} else {
							tmp += tmp2 + ",";
						}
					}

				} else if (l instanceof SubSelect) {
					return " true ";
				} else {
					tmp = tmp.substring(0, tmp.length() - 1) + ((InExpression) exp).getRightItemsList();
				}
			}
			res += " " + tmp.substring(0, tmp.length() - 1) + ")";

			return res;
		} else if (exp instanceof Function) {
			boolean flag = true;
			String tmp = ((Function) exp).getName() + "(";
			String tmp2 = ((Function) exp).getName() + "(";
			ExpressionList params = ((Function) exp).getParameters();
			for (Expression expr : params.getExpressions()) {
				if (flag) {
					tmp2 = getCondition(expr, tablename, al, server);
					if (tmp2.equals(" true ")) {
						flag = false;
						tmp = " true ";
						return tmp;
					}
					tmp += tmp2 + ",";
				}
			}
			res += " " + tmp.substring(0, tmp.length() - 1) + ") ";
		} else {
			return " true ";
		}
		return res;
	}
}
