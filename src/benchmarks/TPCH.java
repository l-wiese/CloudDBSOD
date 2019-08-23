package benchmarks;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import constraints.Constraint;
import constraints.Dependency;
import fragmentation.DistributedDatabaseClient;
import metadata.Server;
import metadata.Table;
import util.ModelSolver;
import util.Utility;

//Class that executes the TPC-H benchmark
public class TPCH {
	
	//Runs the benchmark
	public static void tpch(List<Server> servers, boolean load) throws Exception {
		String dbname = "tpch";
		Server owner = new Server(Utility.getOwner(servers));
		owner.open(dbname);
		ModelSolver.setVerbose(0);

		DistributedDatabaseClient frag = null;
		if (!load) {
			List<Table> tables = Utility.getTables(owner.getHost(), owner.getPort(), owner.getUsername(),
					owner.getPassword(), dbname);
			String filename = Paths.get(".").toAbsolutePath().normalize().toString()
					+ "/fragmentations/tpch/confidentiality.txt";
			List<Constraint> C = Utility.readConstraints(filename, tables);
			C = Utility.wellDefined(tables, C, true);
			filename = Paths.get(".").toAbsolutePath().normalize().toString() + "/fragmentations/tpch/visibility.txt";
			List<Constraint> V = Utility.readConstraints(filename, tables);
			filename = Paths.get(".").toAbsolutePath().normalize().toString() + "/fragmentations/tpch/closeness.txt";
			List<Constraint> CC = Utility.readConstraints(filename, tables);
			filename = Paths.get(".").toAbsolutePath().normalize().toString() + "/fragmentations/tpch/dependency.txt";
			List<Dependency> D = Utility.readDependencies(filename, tables);

			double ownerCapacity = Utility.suggestOwnerCapacity(tables, C, D);
			for (Server s : servers) {
				if (s.isOwner()) {
					s.setCapacity(ownerCapacity);
				}
			}

			double[] a = Utility.suggestWeights(servers.size(), V, CC);
			double time = System.currentTimeMillis();
			frag = ModelSolver.solve(tables, servers, C, V, CC, D, a, 1.0e-10, -1.0, false);
			int threads = Runtime.getRuntime().availableProcessors();
			time=System.currentTimeMillis();
			frag.create(dbname, "_frag", 1000000, threads, null);
			time=System.currentTimeMillis()-time;
			System.out.println("Setup time: "+time+"ms");

		} else {
			frag = DistributedDatabaseClient.load(dbname);
		}

		int count = 0;
		int countsingleserver = 0;

		BenchmarkResultSet rs = null;
		File f = new File("./res/queries.sql");
		Scanner sc = new Scanner(f);
		sc.useDelimiter("BEGIN;");
		sc.next();
		while (sc.hasNext()) {
			String query = "";
			query = sc.next().replaceAll("COMMIT;", "");
			query = query.replace("\n", " ");
			query = query.replace("\t", " ");
			query = query.replaceAll("\\s{2,}", " ").trim();
			List<String> queries = Arrays.asList(query.split(";"));
			long time = System.currentTimeMillis();
			long time2 = System.currentTimeMillis();
			long time3 = System.currentTimeMillis();
			long time4 = System.currentTimeMillis();
			int timeout = -1;
			count++;
			if (count != 17 && count != 20) {
				timeout = -1;
			} else {
				timeout = 1800000;
			}
			System.out.println("Exectuing query: " + count);
			try {
				ResultSet rs2 = null;
				if (queries.size() == 1) {
					String q = queries.get(0);
					time3 = System.currentTimeMillis();
					Connection connection = owner.getConnection();
					Statement statement = connection.createStatement();
					if (timeout > 0) {
						try {
							connection.setAutoCommit(false);
							statement.executeUpdate("SET LOCAL statement_timeout=" + timeout + ";");
							rs2 = statement.executeQuery(q + ";");
							connection.commit();
						} catch (Exception e) {
							System.out.println(e.getMessage());
							connection.close();
						}
					} else {
						rs2 = statement.executeQuery(q + ";");
					}
					time3 = System.currentTimeMillis() - time3;
					new BenchmarkResultSet(rs2, 0, time3, 1).printStats();
				}
				if (queries.size() == 3) {
					time3 = 0;
					for (int i = 0; i < queries.size(); i++) {
						String q = queries.get(i);
						long tmptime = 0;
						rs2 = null;
						if (q.toLowerCase().contains("view")) {
							tmptime = System.currentTimeMillis();
							owner.executeUpdate(q + ";");
							time3 += System.currentTimeMillis() - tmptime;
						} else {
							tmptime = System.currentTimeMillis();
							rs2 = owner.executeQuery(q + ";", false);
							time3 += System.currentTimeMillis() - tmptime;
						}
					}
					new BenchmarkResultSet(rs2, 0, time3, 1).printStats();

				}
			} catch (Exception e) {
				System.out.println(query);
				System.out.println(e.getMessage());
			}
			try {
				if (queries.size() == 1) {
					String q = queries.get(0);
					time = System.currentTimeMillis();
					rs = frag.executeQuery(q + ";", "fdw", timeout);
					time = System.currentTimeMillis() - time;
					rs.printStats();
				}
				if (queries.size() == 3) {
					time = 0;
					rs = new BenchmarkResultSet(null);
					BenchmarkResultSet rs2 = new BenchmarkResultSet(null);
					for (int i = 0; i < queries.size(); i++) {
						long tmptime = 0;
						String q = queries.get(i);

						if (q.toLowerCase().contains("view")) {
							tmptime += System.currentTimeMillis();
							rs2 = frag.executeUpdate(q + ";", "fdw");
							rs.setAnalyzeAndRewriteTime(rs.getAnalyzeAndRewriteTime() + rs2.getAnalyzeAndRewriteTime());
							rs.setExecutionTime(rs.getExecutionTime() + rs2.getExecutionTime());
							rs.setInvolvedServers(rs.getInvolvedServers() + rs2.getInvolvedServers());
							rs.setInvolvedTableFragments(rs.getInvolvedServers() + rs2.getInvolvedTableFragments());
							time += System.currentTimeMillis() - tmptime;
						} else {
							tmptime += System.currentTimeMillis();
							rs2 = frag.executeQuery(q + ";", "fdw");
							rs.setAnalyzeAndRewriteTime(rs.getAnalyzeAndRewriteTime() + rs2.getAnalyzeAndRewriteTime());
							rs.setExecutionTime(rs.getExecutionTime() + rs2.getExecutionTime());
							rs.setRs(rs2.getRs());
							time += System.currentTimeMillis() - tmptime;
						}
					}
					rs.printStats();
				}

			} catch (Exception e) {
				System.out.println(query);
				System.out.println(e.getMessage());
			}

			try {

				if (queries.size() == 1) {
					String q = queries.get(0);
					time4 = System.currentTimeMillis();
					rs = frag.executeQuery(q + ";", "view", timeout);
					time4 = System.currentTimeMillis() - time4;
					rs.printStats();

				}
				if (queries.size() == 3) {
					time4 = 0;
					rs = new BenchmarkResultSet(null);
					BenchmarkResultSet rs2 = new BenchmarkResultSet(null);
					for (int i = 0; i < queries.size(); i++) {
						long tmptime = 0;
						String q = queries.get(i);
						if (q.toLowerCase().contains("view")) {
							tmptime += System.currentTimeMillis();
							rs2 = frag.executeUpdate(q + ";", "view");
							rs.setAnalyzeAndRewriteTime(rs.getAnalyzeAndRewriteTime() + rs2.getAnalyzeAndRewriteTime());
							rs.setExecutionTime(rs.getExecutionTime() + rs2.getExecutionTime());
							rs.setInvolvedServers(rs.getInvolvedServers() + rs2.getInvolvedServers());
							rs.setInvolvedTableFragments(
									rs.getInvolvedTableFragments() + rs2.getInvolvedTableFragments());
							time4 += System.currentTimeMillis() - tmptime;
						} else {
							tmptime += System.currentTimeMillis();
							rs2 = frag.executeQuery(q + ";", "view");
							rs.setAnalyzeAndRewriteTime(rs.getAnalyzeAndRewriteTime() + rs2.getAnalyzeAndRewriteTime());
							rs.setExecutionTime(rs.getExecutionTime() + rs2.getExecutionTime());
							rs.setRs(rs2.getRs());
							time4 += System.currentTimeMillis() - tmptime;
						}
					}
					rs.printStats();
				}
			} catch (Exception e) {
				System.out.println(query);
				System.out.println(e.getMessage());
			}

			try {
				if (queries.size() == 1) {
					String q = queries.get(0);
					time2 = System.currentTimeMillis();
					rs = frag.executeQuery(q + ";", "single", timeout);
					if (rs != null) {
						time2 = System.currentTimeMillis() - time2;
						rs.printStats();
						countsingleserver++;
					} else {
						System.out.println("-");
						time2 = 0;
					}

				}
				if (queries.size() == 3) {
					time2 = 0;
					System.out.println("-");
				}
			} catch (Exception e) {
				System.out.println(query);
				System.out.println(e.getMessage());
			}
			//System.out.println(tex + "\\\\ \\hline");
		}
		System.out.println(
				"Executed " + count + " queries ; " + countsingleserver + " was/were executed on a single server");
	}

}
