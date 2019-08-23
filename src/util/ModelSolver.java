package util;

import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import constraints.Constraint;
import constraints.Dependency;
import fragmentation.DistributedDatabaseClient;
import ilog.concert.IloAnd;
import ilog.concert.IloException;
import ilog.concert.IloIntVar;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloOr;
import ilog.cplex.IloCplex;
import metadata.Attribute;
import metadata.Server;
import metadata.Table;

//class used to solve the separation of duties problem
public class ModelSolver {
	private static int verbose=0;
	public static int getVerbose() {
		return verbose;
	}

	public static void setVerbose(int verbose) {
		ModelSolver.verbose = verbose;
	}

	public static DistributedDatabaseClient solve(List<Table> tables,
			List<Server> servers, List<Constraint> C, List<Constraint> V2,
			List<Constraint> CC, List<Dependency> D, double[] weights,boolean preSolve) {
		return solve(tables, servers, C, V2, CC, D, weights, -1.0, -1.0,preSolve);
	}

	public static DistributedDatabaseClient solve(List<Table> tables,
			List<Server> servers, List<Constraint> C, List<Constraint> V2,
			List<Constraint> CC, List<Dependency> D, double[] weights,
			Double timeLimit,boolean preSolve) {
		return solve(tables, servers, C, V2, CC, D, weights, -1.0,
				timeLimit,preSolve);
	}

	public static DistributedDatabaseClient solve(List<Table> tables,
			List<Server> servers, List<Constraint> C, List<Constraint> V2,
			List<Constraint> CC, List<Dependency> D, double[] weights,
			Double mipGap, Double timeLimit,boolean preSolve) {
		if (timeLimit == null || timeLimit<0.0) {
			return solve(tables, servers, C, V2, CC, D, weights, mipGap,
					new Double[] { -1.0, -1.0 },preSolve);
		}
		return solve(tables, servers, C, V2, CC, D, weights, mipGap,
				new Double[] { timeLimit * 0.5, timeLimit * 0.5 },preSolve);
	}

	public static DistributedDatabaseClient solve(List<Table> tables,
			List<Server> servers, List<Constraint> C, List<Constraint> V2,
			List<Constraint> CC, List<Dependency> D, double[] weights,
			Double mipGap, Double[] timeLimit,boolean preSolve) {
		Double addToLastTimeLimit = 0.0;
		int[] MIPEmphasis=new int[]{0,0};
		int[] nodeAlg=new int[]{0,0};
		boolean twice=preSolve;
		try {
			IloCplex cplex = new IloCplex();
			cplex.setName("SOD");			
			if(verbose==0  || verbose==2){
				cplex.setParam(IloCplex.IntParam.MIPDisplay, 0);
			}
			cplex.setParam(IloCplex.IntParam.MIPEmphasis, MIPEmphasis[0]);
			cplex.setParam(IloCplex.IntParam.RootAlg, 6);
			cplex.setParam(IloCplex.IntParam.NodeAlg, nodeAlg[0]);

			if (mipGap != null && mipGap > 0) {
				cplex.setParam(IloCplex.Param.MIP.Tolerances.MIPGap, mipGap);
			}

			if (D == null) {
				D = new ArrayList<Dependency>();
			}
			if (V2 == null) {
				V2 = new ArrayList<Constraint>();
			}
			if (C == null) {
				C = new ArrayList<Constraint>();
			}
			if (CC == null) {
				CC = new ArrayList<Constraint>();
			}
			boolean correct = false;
			if (V2.size() == 0 && CC.size() == 0 && D.size() == 0) {
				correct = true;
			}
			double ownerCapacity = Utility.getOwner(servers).getCapacity();
			List<Constraint> V = new ArrayList<Constraint>(V2);
			for (Constraint v : V2) {
				for (Constraint c : C) {
					if (v.getAttributes().containsAll(c.getAttributes())) {
						double vCapacity = 0;
						for (Attribute i : v.getAttributes()) {
							vCapacity += i.getWeight();
						}
						if (vCapacity > ownerCapacity) {
							V.remove(v);
						}
					}
				}
				Set<Attribute> inflated = Utility.inflate(v.getAttributes(), D);
				for (Constraint c : C) {
					if (inflated.containsAll(c.getAttributes())) {
						double vCapacity = 0;
						for (Attribute i : v.getAttributes()) {
							vCapacity += i.getWeight();
						}
						if (vCapacity > ownerCapacity) {
							V.remove(v);
						}
					}
				}
			}
			HashMap<Attribute, List<Server>> attr_to_server = new HashMap<Attribute, List<Server>>();
			Server owner = null;
			for (int i = 0; i < servers.size(); i++) {
				if (servers.get(i).isOwner()) {
					owner = servers.get(i);
					if (i != 0) {
						servers.remove(i);
						servers.add(0, owner);
					}
				}
			}

			HashMap<String, List<Attribute>> attributes = new HashMap<String, List<Attribute>>();
			HashMap<String, List<Attribute>> tid = new HashMap<String, List<Attribute>>();
			HashMap<String, Integer> ns = new HashMap<String, Integer>();
			for (int i = 0; i < tables.size(); i++) {
				attributes.put(tables.get(i).getName(), new ArrayList<Attribute>(tables
						.get(i).getAttributes()));
				tid.put(tables.get(i).getName(), new ArrayList<Attribute>(tables.get(i)
						.getTid()));
				ns.put(tables.get(i).getName(), tables.get(i).getAttributes()
						.size());
			}

			// variables
			// x
			HashMap<Table, HashMap<Attribute, HashMap<Server, IloNumVar>>> x = new HashMap<Table, HashMap<Attribute, HashMap<Server, IloNumVar>>>();
			HashMap<Table, HashMap<Attribute, HashMap<Server, IloNumVar>>> tid_x = new HashMap<Table, HashMap<Attribute, HashMap<Server, IloNumVar>>>();
			for (Table s : tables) {
				x.put(s, new HashMap<Attribute, HashMap<Server, IloNumVar>>());
				tid_x.put(s,
						new HashMap<Attribute, HashMap<Server, IloNumVar>>());
				for (Attribute i : s.getAttributes()) {
					x.get(s).put(i, new HashMap<Server, IloNumVar>());
					if (i.isTid()) {
						tid_x.get(s).put(i, new HashMap<Server, IloNumVar>());
					}
					for (Server j : servers) {
						IloIntVar var = cplex.boolVar("x[" + s.getName() + " "
								+ i.getName() + " " + j.getName() + "]");
						x.get(s).get(i).put(j, var);
						if (i.isTid()) {
							tid_x.get(s).get(i).put(j, var);
						}
					}

				}
			}

			// y
			HashMap<Server, IloNumVar> y = new HashMap<Server, IloNumVar>();
			for (Server j : servers) {
				IloIntVar var = cplex.boolVar("y[" + j + "]");
				y.put(j, var);
			}

			// u,z
			int cardV = V.size();
			HashMap<Constraint, HashMap<Server, IloNumVar>> u = new HashMap<Constraint, HashMap<Server, IloNumVar>>();
			HashMap<Constraint, IloNumVar> z = new HashMap<Constraint, IloNumVar>();
			for (Constraint v : V) {
				IloIntVar var = cplex.boolVar("z[" + v + "]");
				z.put(v, var);
				u.put(v, new HashMap<Server, IloNumVar>());
				for (Server j : servers) {
					var = cplex.boolVar("u[v" + (v) + "," + j + "]");
					u.get(v).put(j, var);
				}
			}

			// q
			HashMap<Constraint, HashMap<Server, IloNumVar>> q = new HashMap<Constraint, HashMap<Server, IloNumVar>>();
			for (Constraint cc : CC) {
				q.put(cc, new HashMap<Server, IloNumVar>());
				for (Server j : servers) {
					IloIntVar var = cplex
							.boolVar("q[cc" + (cc) + "," + j + "]");
					q.get(cc).put(j, var);
				}
			}

			// p
			HashMap<Dependency, HashMap<Server, IloNumVar>> p = new HashMap<Dependency, HashMap<Server, IloNumVar>>();
			for (Dependency d : D) {
				p.put(d, new HashMap<Server, IloNumVar>());
				for (Server j : servers) {
					if (j.isOwner()) {
						continue;
					}
					IloIntVar var = cplex.boolVar("p[d" + (d) + "," + j + "]");
					p.get(d).put(j, var);
				}
			}

			// objective
			IloLinearNumExpr objective = cplex.linearNumExpr();
			for (Server j : servers) {
				double a1 = weights[0];
				IloNumVar yj = y.get(j);
				objective.addTerm(a1 * j.getImportance(), yj);
			}
			cplex.addMinimize(objective);

			// Redundant constraints for the optimizer
			IloNumVar penalty=cplex.boolVar();
			cplex.add(cplex.eq(penalty, 0));
			
			List<Server> ordered = new ArrayList<Server>();
			for (Server j : servers) {
				if (j.isOwner()) {
					continue;
				}
				int i = 0;
				while (i < ordered.size()
						&& j.getCapacity() <= ordered.get(i).getCapacity()) {
					i++;
				}
				ordered.add(i, j);
			}
			Server last = ordered.get(0);
			for (int i = 1; i < ordered.size(); i++) {
				cplex.addGe(y.get(last), y.get(ordered.get(i)));
				last = ordered.get(i);
			}

			// Disjointness and completeness non-tid
			for (Table s : tables) {
				for (Attribute i : s.getAttributes()) {			
					if (i.isTid()) {
						continue;
					}
					IloLinearNumExpr constr = cplex.linearNumExpr();
					IloAnd and=cplex.and();
					for (Server j : servers) {
						constr.addTerm(1, x.get(s).get(i).get(j));
						and.add(cplex.not(cplex.eq(x.get(s).get(i).get(j), 1)));
					}
					if (correct) {
						cplex.addEq(constr, 1);
						cplex.add(cplex.ifThen(and, cplex.eq(penalty, 1)));
					} else {
						cplex.addGe(constr, 1);
						cplex.add(cplex.ifThen(and, cplex.eq(penalty, 1)));
					}
				}
			}

			// Completeness tid
			for (Table s : tables) {
				for (Attribute i : s.getTid()) {
					IloLinearNumExpr constr = cplex.linearNumExpr();
					IloAnd and=cplex.and();
					for (Server j : servers) {
						constr.addTerm(1, x.get(s).get(i).get(j));
						and.add(cplex.not(cplex.eq(x.get(s).get(i).get(j), 1)));
					}
					cplex.addGe(constr, 1);
					cplex.add(cplex.ifThen(and, cplex.eq(penalty, 1)));
				}
			}

			// tid placement
			for (Server j : servers) {
				for (Table s : tables) {
					IloOr or=cplex.or();
					if (s.getTid().containsAll(s.getAttributes())) {
						continue;
					}
					int sum = 0;
					IloLinearNumExpr left = cplex.linearNumExpr();
					for (Attribute i : s.getAttributes()) {
						if (!s.getTid().contains(i)) {
							sum = sum + 1;
							left.addTerm(1, x.get(s).get(i).get(j));
							or.add(cplex.not(cplex.eq(x.get(s).get(i).get(j),0)));
						}
					}

					for (Attribute id : s.getTid()) {
						IloLinearNumExpr right = cplex.linearNumExpr();
						right.addTerm(sum, x.get(s).get(id).get(j));
						cplex.addLe(left, right);
						cplex.add(cplex.ifThen(or, cplex.eq(x.get(s).get(id).get(j), 1)));
					}
				}
			}

			// tid non-redundancy
			for (Server j : servers) {
				for (Table s : tables) {
					IloAnd and=cplex.and();
					if (s.getTid().containsAll(s.getAttributes())) {
						continue;
					}
					IloLinearNumExpr left = cplex.linearNumExpr();
					for (Attribute i : s.getAttributes()) {
						if (!s.getTid().contains(i)) {
							left.addTerm(1, x.get(s).get(i).get(j));
							and.add(cplex.eq(x.get(s).get(i).get(j),0));
						}
					}

					for (Attribute id : s.getTid()) {
						IloLinearNumExpr right = cplex.linearNumExpr();
						right.addTerm(1, x.get(s).get(id).get(j));
						cplex.addGe(left, right);
						cplex.add(cplex.ifThen(and, cplex.eq(x.get(s).get(id).get(j), 0)));
					}
				}
			}
			
			// weights
			double max_capacity=0.0;
			for(Table table:tables) {
				for(Attribute attr:table.getAttributes()) {
					max_capacity+=attr.getWeight();
				}
			}
			for (Server j : servers) {
				double max_weight=0.0;
				IloLinearNumExpr left = cplex.linearNumExpr();
				IloLinearNumExpr right = cplex.linearNumExpr();
				right.addTerm(j.getCapacity(), y.get(j));
				if(j.getCapacity()>max_capacity) {
					right = cplex.linearNumExpr();
					right.addTerm(max_capacity, y.get(j));
				}
				for (Table s : tables) {
					for (Attribute i : s.getAttributes()) {
						max_weight+=i.getWeight();
						left.addTerm(i.getWeight(), x.get(s).get(i).get(j));
					}
				}
				if(max_weight<j.getCapacity()) {
					IloOr or=cplex.or();
					left = cplex.linearNumExpr();
					int sum=0;
					for (Table s : tables) {
						for (Attribute i : s.getAttributes()) {
							sum++;
							left.addTerm(1, x.get(s).get(i).get(j));
							or.add(cplex.not(cplex.eq(x.get(s).get(i).get(j), 0)));
						}
					}
					right = cplex.linearNumExpr();
					right.addTerm(sum, y.get(j));
					cplex.addLe(left, right);
					cplex.add(cplex.ifThen(or, cplex.eq(y.get(j), 1)));
				}
				else if(j.getCapacity()==0.0) {
					left = cplex.linearNumExpr();
					for (Table s : tables) {
						for (Attribute i : s.getAttributes()) {
							left.addTerm(1, x.get(s).get(i).get(j));
						}
					}
					cplex.addEq(left, 0);
					
				}
				else {
					cplex.addLe(left, right);
				}
			}
			
			// confidentiality constraints
			for (Server j : servers) {
				if (j.isOwner()) {
					continue;
				}
				for (Constraint c : C) {
					IloLinearNumExpr left = cplex.linearNumExpr();
					IloAnd and=cplex.and();
					int cardC = c.size();
					boolean flag = false;
					for (Attribute r : c.getAttributes()) {
						for (Table s : tables) {
							for (Attribute i : s.getAttributes()) {
								if (r.equals(i)) {
									left.addTerm(1, x.get(s).get(i).get(j));
									and.add(cplex.not(cplex.eq(x.get(s).get(i).get(j), 0)));
									flag = true;
								}
							}
						}
					}
					if (flag) {
						cplex.add(cplex.ifThen(and, cplex.eq(penalty, 1)));
						cplex.addLe(left, cardC - 1);
					}
				}
			}

			// dependencies
			for (Dependency d : D) {
				for (Server j : servers) {
					if (j.isOwner()) {
						continue;
					}
					IloAnd and=cplex.and();
					boolean flag = false;
					for (Attribute t : d.getPremise()) {
						for (Table s : tables) {
							for (Attribute i : s.getAttributes()) {
								if (i.equals(t)) {
									if (i.equals(t)) {
										and.add(cplex.not(cplex.eq(x.get(s).get(i).get(j),0)));
									}
								}
							}
						}
						if (flag) {
							cplex.add(cplex.ifThen(and, cplex.eq(p.get(d).get(j), 1.0)));
						}
					}
				}
			}
			
			for (Dependency d : D) {
				int cardPremise = d.getPremise().size();
				for (Server j : servers) {
					if (j.isOwner()) {
						continue;
					}
					IloLinearNumExpr left = cplex.linearNumExpr();
					IloLinearNumExpr right = cplex.linearNumExpr();
					right.addTerm(cardPremise, p.get(d).get(j));
					boolean flag = false;
					for (Attribute t : d.getPremise()) {
						for (Table s : tables) {
							for (Attribute i : s.getAttributes()) {
								if (i.equals(t)) {
									if (i.equals(t)) {
										left.addTerm(1, x.get(s).get(i).get(j));
										flag = true;
									}
								}
							}
						}
						if (flag) {
							cplex.addGe(left, right);
						}
					}
				}
			}

			for (Dependency d : D) {
				int cardConsequence = d.getConsequence().size();
				for (Server j : servers) {
					if (j.isOwner()) {
						continue;
					}
					IloLinearNumExpr left = cplex.linearNumExpr();
					IloLinearNumExpr right = cplex.linearNumExpr();
					right.addTerm(cardConsequence, p.get(d).get(j));
					boolean flag = false;
					for (Attribute t : d.getConsequence()) {
						for (Table s : tables) {
							for (Attribute i : s.getAttributes()) {
								if (i.equals(t)) {
									if (i.equals(t)) {
										left.addTerm(1, x.get(s).get(i).get(j));
										flag = true;
									}
								}
							}
						}
						if (flag) {
							cplex.addGe(left, right);
						}
					}
				}
			}

			// u,z
			for (Server j : servers) {
				for (Constraint v : V) {
					cardV = v.size();
					IloAnd and=cplex.and();
					IloLinearNumExpr leftu = cplex.linearNumExpr();
					IloLinearNumExpr rightu = cplex.linearNumExpr();
					rightu.addTerm(cardV, u.get(v).get(j));
					boolean flag = false;
					for (Attribute t : v.getAttributes()) {
						for (Table s : tables) {
							for (Attribute i : s.getAttributes()) {
								if (i.equals(t)) {
									and.add(cplex.not(cplex.eq(x.get(s).get(i).get(j), 1)));
									leftu.addTerm(1, x.get(s).get(i).get(j));
									flag = true;
								}
							}
						}
					}
					if (flag) {
						cplex.add(cplex.ifThen(cplex.not(cplex.eq(leftu, cardV)), cplex.eq(u.get(v).get(j), 0)));
					}
				}
			}

			for (Constraint v : V) {
				IloLinearNumExpr left = cplex.linearNumExpr();
				IloOr or=cplex.or();
				for (Server j : servers) {
					left.addTerm(1, u.get(v).get(j));
					or.add(cplex.not(cplex.eq(u.get(v).get(j), 0)));
				}
				cplex.addLe(left, 1);
				cplex.add(cplex.ifThen(or, cplex.eq(left, 1)));
			}

			// closeness constraints
			for (Constraint cc : CC) {
				int cardCC = cc.size();
				for (Server j : servers) {
					IloLinearNumExpr left = cplex.linearNumExpr();
					IloLinearNumExpr right = cplex.linearNumExpr();
					IloLinearNumExpr right2 = cplex.linearNumExpr();
					right.addTerm(cardCC, q.get(cc).get(j));
					right2.addTerm(1, q.get(cc).get(j));
					boolean flag = true;
					for (Attribute t : cc.getAttributes()) {
						for (Table s : tables) {
							for (Attribute i : s.getAttributes()) {
								if (i.equals(t)) {
									left.addTerm(1, x.get(s).get(i).get(j));
									flag = true;
								}

							}
						}
						if (flag) {
							cplex.addLe(left, right);
							cplex.add(cplex.ifThen(cplex.not(cplex.eq(left, 0)), cplex.eq(q.get(cc).get(j), 1)));
						}
					}
				}
			}

			if (timeLimit != null && timeLimit[0] != null && timeLimit[0] > 0) {
				cplex.setParam(IloCplex.DoubleParam.TiLim, timeLimit[0]);
			}

			double start = System.currentTimeMillis();
			if(verbose==2 || verbose==3){
				cplex.use(new Info(y, u, q, V.size(), V2.size(), weights, start,
						cplex));
			}
			boolean solved = false;
			double time = 0.0;
			
			if (timeLimit != null && timeLimit[0] != null && timeLimit[0] > 0) {
				cplex.setParam(IloCplex.DoubleParam.TiLim, timeLimit[0]);
			}
			start = System.currentTimeMillis();
			if (V2.size() > 0) {
				if(twice){
					solved = cplex.solve();
				}
				time = (System.currentTimeMillis() - start) / 1000.0;
				time = precision(time, 2);
				if (timeLimit != null && timeLimit[0] != null && timeLimit[0] > 0) {
					if (time < timeLimit[0]) {
						addToLastTimeLimit += timeLimit[0] - time;
					}
				}
				if(solved){
					if(verbose==3) {
					printStats(y, u, q, V.size(), V2.size(), weights, cplex, time);
					}
				}
				
			} else {
				if (timeLimit != null && timeLimit[1] != null
						&& timeLimit[1] > 0) {
					addToLastTimeLimit = timeLimit[1];
				}
			}
			if(solved && cplex.getStatus().Optimal.equals(IloCplex.Status.Optimal)) {
				HashMap<Server,Double> yvalues=new HashMap<Server,Double>();
				for(Server j:servers) {
					yvalues.put(j, cplex.getValue(y.get(j)));
				}
				for(Server j:servers) {
					cplex.addEq(y.get(j), yvalues.get(j));
				}
			}
			

			//add visibility constraints to objective
			for (Constraint v : V) {
				double a2 = weights[1];
				double factor = v.getWeight() * a2;
				for (Server j : servers) {
					objective.addTerm(-factor, u.get(v).get(j));
				}
			}
			
			cplex.getObjective().setExpr(objective);
	
			//add closeness constraints to objective
			for (Constraint cc : CC) {
				double a3 = weights[2];
				double factor = cc.getWeight() * a3;
				for (Server j : servers) {
					objective.addTerm(factor, q.get(cc).get(j));
				}

			}

			cplex.getObjective().setExpr(objective);
			cplex.setParam(IloCplex.IntParam.MIPEmphasis, MIPEmphasis[1]);

			if (timeLimit != null && timeLimit[1] != null && timeLimit[1] > 0) {
				cplex.setParam(IloCplex.DoubleParam.TiLim, timeLimit[1]
						+ addToLastTimeLimit);
			}
			 
			start = System.currentTimeMillis();
			solved = cplex.solve();
			time = (System.currentTimeMillis() - start) / 1000.0;
			time = precision(time, 2);
			printStats(y, u, q, V.size(), V2.size(), weights, cplex, time);
			if (!solved) {
				System.out.println("No solution exists!");
				return null;
			} else {
				for (Table s : tables) {
					for (Attribute i : s.getAttributes()) {
						attr_to_server.put(i, new ArrayList<Server>());
					}
				}
				for (Server j : servers) {
					if (Math.round(cplex.getValue(y.get(j))) == 1) {
						for (Table s : tables) {
							for (Attribute i : s.getAttributes()) {
								if (Math.round(cplex.getValue(x.get(s).get(i)
										.get(j))) == 1) {
									if (!attr_to_server.get(i).contains(j)) {
										attr_to_server.get(i).add(j);
									}
								}
							}
						}
					}
				}
				HashMap<Server, List<Attribute>> serverToAttribute = new HashMap<Server, List<Attribute>>();
				for (Attribute attr : attr_to_server.keySet()) {
					for (Server server : attr_to_server.get(attr)) {
						if (serverToAttribute.get(server) == null) {
							serverToAttribute.put(server,
									new ArrayList<Attribute>());
						}
						serverToAttribute.get(server).add(attr);
					}
				}
				for (Server server : serverToAttribute.keySet()) {
					Collections.sort(serverToAttribute.get(server),
							new Comparator<Attribute>() {
								public int compare(Attribute f1, Attribute f2) {
									return f1.toString().compareTo(
											f2.toString());
								}
							});
				}
				int viscount = 0;
				for (Constraint v : V) {
					for (Server j : servers) {
						viscount += Math.round(cplex.getValue(u.get(v).get(j)));
					}
				}
				System.out.println("Visibility Constraints: " + viscount);

				DistributedDatabaseClient frag = new DistributedDatabaseClient(owner, attr_to_server,
						tables);
				Set<String> tableFragments = new HashSet<String>();
				for (Attribute attr : attr_to_server.keySet()) {
					for (Server server : attr_to_server.get(attr)) {
						tableFragments.add(attr.getTable().toLowerCase() + ""
								+ server.toString());
					}
				}
				System.out.println("Table Fragments: "+tableFragments.size());
				Set<Constraint> tmpConstraints = new HashSet<Constraint>();
				for (Constraint v : V2) {
					for (Server server : servers) {
						Set<Attribute> tmpattributes = new HashSet<Attribute>();
						for (Attribute attr : attr_to_server.keySet()) {
							if (attr_to_server.get(attr).contains(server)) {
								tmpattributes.add(attr);
							}
						}
						if (tmpattributes.containsAll(v.getAttributes())) {
							tmpConstraints.add(v);
						}
					}
				}
				return frag;
			}

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	static class Info extends IloCplex.MIPInfoCallback {
		HashMap<Constraint, HashMap<Server, IloNumVar>> u = new HashMap<Constraint, HashMap<Server, IloNumVar>>();
		HashMap<Server, IloNumVar> y = new HashMap<Server, IloNumVar>();
		HashMap<Constraint, HashMap<Server, IloNumVar>> q = new HashMap<Constraint, HashMap<Server, IloNumVar>>();
		IloCplex cplex;
		int sercount = 0;
		int viscount = 0;
		int cccount = 0;
		double[] weights = null;
		static double last = 0;
		double startTime = 0;
		static PrintWriter out;
		static int thisfile = -1;
		int nvis = 0;
		int nfvis = 0;

		public Info(HashMap<Server, IloNumVar> y,
				HashMap<Constraint, HashMap<Server, IloNumVar>> u,
				HashMap<Constraint, HashMap<Server, IloNumVar>> q, int nfvis,
				int nvis, double[] weights, double startTime, IloCplex cplex) {
			super();
			this.u = u;
			this.y = y;
			this.q = q;
			this.cplex = cplex;
			this.weights = weights;
			this.startTime = startTime;
			out = null;
			this.nvis = nvis;
			this.nfvis = nfvis;
		}

		public void main() throws IloException {
			int sercount2 = sercount;
			int viscount2 = viscount;
			int cccount2 = cccount;
			sercount = 0;
			viscount = 0;
			cccount = 0;
			int nviscount = 0;
			double end = this.getCplexTime();
			if (((int) (end - last)) > 3) {
				if (this.hasIncumbent()) {
					for (Server j : y.keySet()) {
						sercount += Math
								.round(this.getIncumbentValue(y.get(j)));
					}
					try {
						for (Constraint v : u.keySet()) {
							boolean incr = true;
							for (Server j : u.get(v).keySet()) {
								viscount += Math.round(this.getIncumbentValue(u
										.get(v).get(j)));
								if (Math.round(this.getIncumbentValue(u.get(v)
										.get(j))) == 1) {
									incr = false;
								}
							}
							if (incr) {
								nviscount += 1;
								
							}
						}
					} catch (Exception e) {

					}

					try {
						for (Constraint cc : q.keySet()) {
							for (Server j : q.get(cc).keySet()) {
								cccount += Math.round(this.getIncumbentValue(q
										.get(cc).get(j)));
							}
						}
					} catch (Exception e) {

					}
					Double mipGap = precision(this.getMIPRelativeGap() * 100, 2);
					Double lowerBound = this.getIncumbentObjValue()
							- (this.getMIPRelativeGap() * this
									.getIncumbentObjValue());
					int visGap = (int) ((this.getIncumbentObjValue() - lowerBound) / Math
							.abs(weights[1]+1e-15));
					int ccGap = (int) ((this.getIncumbentObjValue() - lowerBound) / Math
							.abs(weights[2]+1e-15));
					int sGap = (int) ((this.getIncumbentObjValue() - lowerBound) / Math
							.abs(weights[0]+1e-15));
					if (last == 0) {
						last = System.currentTimeMillis();
					}

					double time = (System.currentTimeMillis() - startTime) / 1000;
					time = precision(time, 2);
					if (((int) (end - last)) > 3 || sercount != sercount2
							|| viscount != viscount2 || cccount2 != cccount) {
						System.out.println("ZStats:	t: " + ((time)) + "	s: "
											+ sercount + "	vis: " + viscount + "/"
											+ nfvis + "/" + nvis + "	cc: "
											+ cccount + "	gap: " + mipGap + "%"
											+ "	sGap: " + sGap + "	vGap: " + visGap
											+ "/" + nviscount + "	ccGap: " + ccGap);
					}
				}
			}
			last = this.getCplexTime();
		}

	}

	private static void printStats(HashMap<Server, IloNumVar> y,
			HashMap<Constraint, HashMap<Server, IloNumVar>> u,
			HashMap<Constraint, HashMap<Server, IloNumVar>> q, int nfvis,
			int nvis, double[] weights, IloCplex cplex, double time) throws IloException {
		int sercount = 0;
		int viscount = 0;
		int cccount = 0;
		int nviscount = 0;
		for (Server j : y.keySet()) {
			sercount += Math.round(cplex.getValue(y.get(j)));
		}
		try {
			for (Constraint v : u.keySet()) {
				boolean incr = true;
				for (Server j : u.get(v).keySet()) {
					viscount += Math.round(cplex.getValue(u.get(v).get(j)));
					if (Math.round(cplex.getValue(u.get(v).get(j))) == 1) {
						incr = false;
					}
				}
				if (incr) {
					nviscount += 1;
				}
			}
		} catch (Exception e) {

		}

		try {
			for (Constraint cc : q.keySet()) {
				for (Server j : q.get(cc).keySet()) {
					cccount += Math.round(cplex.getValue(q.get(cc).get(j)));
				}
			}
		} catch (Exception e) {

		}
		
		Double mipGap = precision(cplex.getMIPRelativeGap() * 100, 2);
		Double lowerBound = cplex.getBestObjValue() - cplex.getMIPRelativeGap()
				* cplex.getBestObjValue();
		int ccGap = (int) ((cplex.getBestObjValue() - lowerBound) / Math.abs(weights[2]+1e-15));
		int visGap = (int) ((cplex.getBestObjValue() - lowerBound) / Math.abs(weights[1]+1e-15));
		int sGap = (int) ((cplex.getBestObjValue() - lowerBound) / Math.abs(weights[0]+1e-15));
		System.out.println("Stats:	t: " + ((time)) + "	s: " + sercount
				+ "	vis: " + viscount + "/" + nfvis + "/" + nvis + "	cc: "
				+ cccount + "	gap: " + mipGap + "%" + "	sGap: " + sGap
				+ "	vGap: " + visGap + "/" + nviscount + "	ccGap: " + ccGap);
	}

	private static Double precision(Double a, int precision) {
		Double a2 = new Double(a);
		a = BigDecimal.valueOf(a2).setScale(precision, RoundingMode.HALF_UP)
				.doubleValue();
		return a;
	}

}
