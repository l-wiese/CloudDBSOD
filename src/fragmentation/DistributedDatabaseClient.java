package fragmentation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyInputStream;
import org.postgresql.copy.PGCopyOutputStream;

import benchmarks.BenchmarkResultSet;
import metadata.Attribute;
import metadata.Server;
import metadata.Table;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import util.StatusBar;

//An implementation of the distributed database client
public class DistributedDatabaseClient implements Serializable {
	private static final long serialVersionUID = -5338648719422834911L;
	private HashMap<Attribute, List<Server>> attributeToServer = new HashMap<Attribute, List<Server>>();
	private HashMap<Server, List<Attribute>> serverToAttribute = new HashMap<Server, List<Attribute>>();
	private HashMap<String, Table> tablenameToTable = new HashMap<String, Table>();
	private HashMap<String, Integer> viewCount = new HashMap<String, Integer>();
	private HashMap<String, Set<Server>> viewToServer = new HashMap<String, Set<Server>>();
	private String suffix = "";
	private String dbname = "";
	private Server orig_owner = null;
	private Server frag_owner = null;
	private HashMap<String, Set<Server>> tablefragments = new HashMap<String, Set<Server>>();
	private QueryRewriter qr;
	private StatusBar sb = null;

	public DistributedDatabaseClient() {
	};

	public DistributedDatabaseClient(Server owner, HashMap<Attribute, List<Server>> attributeToServer,
			List<Table> tables) {
		super();
		this.orig_owner = owner;
		this.frag_owner = new Server(owner);
		this.attributeToServer = attributeToServer;
		for (Table table : tables) {
			tablenameToTable.put(table.getName(), table);
		}
	}

	public String toString() {
		return attributeToServer.toString();
	}

	public List<Server> getServer(Attribute attr) {
		return attributeToServer.get(attr);
	}

	public void add(Attribute attr, List<Server> server) {
		attributeToServer.put(attr, server);
	}

	public HashMap<Server, List<Attribute>> getServerToAttribute() {
		return serverToAttribute;
	}

	public void setServerToAttribute(HashMap<Server, List<Attribute>> serverToAttribute) {
		this.serverToAttribute = serverToAttribute;
	}

	private void serverToAttribute() {
		for (Attribute attr : attributeToServer.keySet()) {
			for (Server server : attributeToServer.get(attr)) {
				if (serverToAttribute.get(server) == null) {
					serverToAttribute.put(server, new ArrayList<Attribute>());
				}
				serverToAttribute.get(server).add(attr);
			}
		}
	}

	// method to execute queries
	public BenchmarkResultSet executeQuery(String query, String method) throws Exception {
		return executeQuery(query, method, -1);
	}

	// method to execute queries
	public BenchmarkResultSet executeQuery(String query, String method, int timeout) throws Exception {
		int involvedServerCount = 0;
		int involvedServerFragments = 0;
		long time1 = 0;
		long time2 = 0;
		long time3 = 0;
		long time4 = 0;
		boolean fdw = true;
		if (method.equals("dblink")) {
			fdw = false;
		}
		ResultSet rs = null;
		if (method.equals("single")) {
			ServerQuery sq = null;
			try {
				time1 = System.currentTimeMillis();
				sq = qr.evaluableOnSingleServer(query);
			} catch (Exception e) {
				//e.printStackTrace();
				return null;
			}
			if (sq != null) {
				sq.rewrite(suffix);
				time2 = System.currentTimeMillis();
				Server server = sq.getServer().iterator().next();
				time3 = System.currentTimeMillis();
				Connection connection = server.getConnection();
				Statement statement = connection.createStatement();
				if (timeout > 0) {
					try {
						connection.setAutoCommit(false);
						statement.executeUpdate("SET LOCAL statement_timeout=" + timeout + ";");
						rs = statement.executeQuery(sq.getQuery());
						connection.commit();
					} catch (Exception e) {
						System.out.println(e.getMessage());
						connection.close();
					}
				} else {
					rs = server.executeQuery(sq.getQuery());
				}
				time4 = System.currentTimeMillis();
				return new BenchmarkResultSet(rs, time2 - time1, time4 - time3, 1);
			} else {
				return null;
			}
		}
		Connection connection = frag_owner.getConnection();
		Statement statement = connection.createStatement();
		if (method.equals("view")) {
			time3 = System.currentTimeMillis();
			if (timeout > 0) {
				try {
					connection.setAutoCommit(false);
					statement.executeUpdate("SET LOCAL statement_timeout=" + timeout + ";");
					rs = statement.executeQuery(query);
					connection.commit();
				} catch (Exception e) {
					System.out.println(e.getMessage());
					connection.close();
				}
			} else {
				rs = frag_owner.executeQuery(query + ";");
			}
			time4 = System.currentTimeMillis();
			Set<Server> involvedServers = new HashSet<Server>();
			for (String tablename : tablenameToTable.keySet()) {
				if (query.toLowerCase().contains(tablename)) {
					involvedServerCount += viewCount.get(tablename);
					if (viewToServer.containsKey(tablename)) {
						involvedServers.addAll(viewToServer.get(tablename));
					}
				}
			}
			return new BenchmarkResultSet(rs, 0, time4 - time3, involvedServers.size(), involvedServerCount);
		} else {
			String parsedQuery = "";
			time1 = System.currentTimeMillis();
			parsedQuery = rewrite(query, fdw);
			time2 = System.currentTimeMillis();

			for (Server server : serverToAttribute.keySet()) {
				if (parsedQuery.toLowerCase().contains(server.getName().toLowerCase())) {
					involvedServerCount++;
				}
				for (String table : tablenameToTable.keySet()) {
					if (parsedQuery.toLowerCase().contains(server.getName().toLowerCase() + "_" + table + "_frag")) {
						involvedServerFragments++;
					}
				}
			}
			time3 = System.currentTimeMillis();
			if (timeout > 0) {
				try {
					connection.setAutoCommit(false);
					statement.executeUpdate("SET LOCAL statement_timeout=" + timeout + ";");
					rs = statement.executeQuery(parsedQuery + ";");
					connection.commit();
				} catch (Exception e) {
					System.out.println(e.getMessage());
					connection.close();
				}
			} else {
				rs = statement.executeQuery(parsedQuery + ";");
			}
			time4 = System.currentTimeMillis();

		}
		return new BenchmarkResultSet(rs, time2 - time1, time4 - time3, involvedServerCount, involvedServerFragments);
	}

	// method to execute queries - uses a single server if possible
	public BenchmarkResultSet executeQuery(String query, String method, boolean singleServer) throws Exception {
		int involvedServerCount = 0;
		long time1 = 0;
		long time2 = 0;
		long time3 = 0;
		long time4 = 0;
		boolean fdw = true;
		if (method.equals("dblink")) {
			fdw = false;
		}
		ResultSet rs = null;
		Connection connection = frag_owner.getConnection();
		Statement statement = connection.createStatement();
		if (method.equals("single") || singleServer) {
			ServerQuery sq = null;
			try {
				time1 = System.currentTimeMillis();
				sq = qr.evaluableOnSingleServer(query);
			} catch (Exception e) {
				//e.printStackTrace();
				return null;
			}
			if (sq != null) {
				sq.rewrite(suffix);
				time2 = System.currentTimeMillis();
				Server server = sq.getServer().iterator().next();
				time3 = System.currentTimeMillis();
				rs = server.executeQuery(sq.getQuery());
				time4 = System.currentTimeMillis();
				Set<Server> involvedServers = new HashSet<Server>();
				for (String tablename : tablenameToTable.keySet()) {
					if (query.toLowerCase().contains(tablename)) {
						involvedServerCount += viewCount.get(tablename);
						if (viewToServer.containsKey(tablename)) {
							involvedServers.addAll(viewToServer.get(tablename));
						}
					}
				}
				return new BenchmarkResultSet(rs, time2 - time1, time4 - time3, involvedServers.size());
			}
		}
		if (method.equals("view")) {
			time3 = System.currentTimeMillis();
			rs = frag_owner.executeQuery(query);
			time4 = System.currentTimeMillis();
			for (String tablename : tablenameToTable.keySet()) {
				if (query.toLowerCase().contains(tablename)) {
					involvedServerCount += viewCount.get(tablename);
				}
			}
			return new BenchmarkResultSet(rs, 0, time4 - time3, involvedServerCount);
		} else {
			String parsedQuery = "";
			time1 = System.currentTimeMillis();
			parsedQuery = rewrite(query, fdw);
			time2 = System.currentTimeMillis();
			for (Server server : serverToAttribute.keySet()) {
				if (parsedQuery.toLowerCase().contains(server.getName().toLowerCase())) {
					involvedServerCount++;
				}
			}
			time3 = System.currentTimeMillis();
			rs = statement.executeQuery(parsedQuery + ";");
			time4 = System.currentTimeMillis();
		}
		return new BenchmarkResultSet(rs, time2 - time1, time4 - time3, involvedServerCount);
	}

	// method to execute updates
	public BenchmarkResultSet executeUpdate(String query, String method) throws Exception {
		int involvedServerCount = 0;
		int involvedServerFragments = 0;
		long time1 = 0;
		long time2 = 0;
		long time3 = 0;
		long time4 = 0;
		boolean fdw = true;
		if (method.equals("dblink")) {
			fdw = false;
		}
		ResultSet rs = null;
		Connection connection = frag_owner.getConnection();
		Statement statement = connection.createStatement();
		String parsedQuery = "";
		if (method.equals("view")) {
			time3 = System.currentTimeMillis();
			rs = null;
			try {
				statement.executeUpdate(query);
			} catch (Exception e) {
				//e.printStackTrace();
			}
			time4 = System.currentTimeMillis();
			Set<Server> involvedServers = new HashSet<Server>();
			for (String tablename : tablenameToTable.keySet()) {
				if (query.toLowerCase().contains(tablename)) {
					involvedServerCount += viewCount.get(tablename);
					if (viewToServer.containsKey(tablename)) {
						involvedServers.addAll(viewToServer.get(tablename));
					}
				}
			}
			return new BenchmarkResultSet(rs, 0, time4 - time3, involvedServers.size(), involvedServerCount);
		} else {
			time1 = System.currentTimeMillis();
			parsedQuery = rewrite(query, fdw);
			time2 = System.currentTimeMillis();
			for (Server server : serverToAttribute.keySet()) {
				if (parsedQuery.toLowerCase().contains(server.getName().toLowerCase())) {
					involvedServerCount++;
				}
				for (String table : tablenameToTable.keySet()) {
					if (parsedQuery.toLowerCase().contains(server.getName().toLowerCase() + "_" + table + "_frag")) {
						involvedServerFragments++;
					}
				}
			}
		}
		try {
			time3 = System.currentTimeMillis();
			rs = null;
			statement.executeUpdate(parsedQuery + ";");
			time4 = System.currentTimeMillis();
		} catch (Exception e) {
			if (!e.getMessage().equals("Die Abfrage lieferte kein Ergebnis.")) {
				//e.printStackTrace();
				throw e;
			}

		}
		return new BenchmarkResultSet(rs, time2 - time1, time4 - time3, involvedServerCount, involvedServerFragments);
	}

	// call to the QueryRewriter
	public String rewrite(String query, boolean fdw) throws Exception {
		return qr.parse(query, fdw);
	}

	// sets up the vertically fragmented database
	public boolean create(String dbname, String suffix, int batchsize, int threads, Long limit)
			throws ClassNotFoundException, SQLException {
		if (frag_owner.getType().equals("psql")) {
			return createPsql(dbname, suffix, batchsize, threads, limit);
		} else {
			return false;
		}
	}

	// sets up the PostgreSQL database
	private boolean createPsql(String dbname, String suffix, int batchsize, int threads, Long limit)
			throws ClassNotFoundException, SQLException {
		this.suffix = suffix;
		this.dbname = dbname;
		orig_owner.open(orig_owner.getHost(), orig_owner.getPort(), orig_owner.getPassword(), orig_owner.getPassword());
		ResultSet rs = orig_owner.executeQuery("SELECT 1 FROM pg_database WHERE datname = '" + dbname + suffix + "'");

		boolean exists = false;
		try {
			while (rs.next()) {
				exists = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (exists) {
			orig_owner.executeQuery("SELECT pg_terminate_backend(pg_stat_activity.pid) "
					+ "FROM pg_stat_activity WHERE pg_stat_activity.datname = '" + dbname + suffix
					+ "'AND pid <> pg_backend_pid();");
			orig_owner.executeUpdate("DROP DATABASE IF EXISTS " + dbname + suffix + ";");
		}
		orig_owner.executeUpdate("CREATE DATABASE " + dbname + suffix + ";");

		orig_owner.open(orig_owner.getHost(), orig_owner.getPort(), dbname, orig_owner.getPassword(),
				orig_owner.getPassword());
		frag_owner.open(frag_owner.getHost(), frag_owner.getPort(), dbname + suffix, frag_owner.getUsername(),
				frag_owner.getPassword());

		try {
			Connection connection = frag_owner.getConnection();
			Statement statement = connection.createStatement();
			statement.execute("CREATE EXTENSION dblink;");
			statement.close();
			connection.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		try {
			Connection connection = frag_owner.getConnection();
			Statement statement = connection.createStatement();
			statement.execute("CREATE EXTENSION postgres_fdw;");
			statement.close();
			connection.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		serverToAttribute();
		ExecutorService executor = Executors.newFixedThreadPool(threads);

		executor.submit(new prepareThread(new Server(frag_owner),new Server(frag_owner) ,suffix, batchsize));
		for (Server server : serverToAttribute.keySet()) {
			if (!server.isOwner()) {
				executor.submit(new prepareThread(server,new Server(frag_owner), suffix, batchsize));
			}
		}
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		executor = Executors.newFixedThreadPool(threads);

		int count = 0;
		TreeSet<String> t = new TreeSet<String>();
		for (Server server : serverToAttribute.keySet()) {
			HashMap<String, Table> tables = new HashMap<String, Table>();
			for (Attribute attr : serverToAttribute.get(server)) {
				if (tables.get(attr.getTable()) == null) {
					tables.put(attr.getTable(), new Table(attr.getTable()));
				}
				tables.get(attr.getTable()).add(attr);
			}
			for (String tname : tables.keySet()) {
				count++;
				t.add(tname);
			}
		}
		sb = new StatusBar(count);
		for (Server server : serverToAttribute.keySet()) {
			HashMap<String, Table> tables = new HashMap<String, Table>();
			for (Attribute attr : serverToAttribute.get(server)) {
				if (tables.get(attr.getTable()) == null) {
					tables.put(attr.getTable(), new Table(attr.getTable()));
				}
				tables.get(attr.getTable()).add(attr);
			}
			for (String tname : tables.keySet()) {
				executor.submit(new insertThread(new Server(server), suffix, batchsize, tname,
						new HashMap<String, Table>(tables), limit));
			}
		}
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		qr = new QueryRewriter(suffix, dbname, attributeToServer, serverToAttribute, tablenameToTable,
				new Server(orig_owner), new Server(frag_owner));
		for (String tablename : tablenameToTable.keySet()) {
			String query = "CREATE OR REPLACE VIEW " + tablename + " AS ";
			Table table = tablenameToTable.get(tablename);
			List<Attribute> tid = table.getTid();
			List<Attribute> attributes = table.getAttributes();
			String select = "Select ";
			String from = " FROM ";
			String using = " USING(";
			for (Attribute attr : table.getTid()) {
				using += attr.getName() + ",";
			}
			using = using.substring(0, using.length() - 1);
			using += ")";
			List<Server> selectedServers = new ArrayList<Server>();
			HashMap<Attribute, Server> assignment = qr.getAssignment(new ArrayList(attributes));
			boolean first = true;
			for (Attribute attr : attributes) {
				Server server = assignment.get(attr);
				select += server.getName() + "_" + tablename + suffix + "." + attr.getName() + " AS " + attr.getName()
						+ ",";
				if (!selectedServers.contains(server)) {
					selectedServers.add(server);
					if (first) {
						from += server.getName() + "_" + tablename + suffix;
						first = false;
					} else {
						from += " LEFT JOIN " + server.getName() + "_" + tablename + suffix + using;
					}

				}
			}
			select = select.substring(0, select.length() - 1);
			viewCount.put(tablename, new HashSet<Server>(selectedServers).size());
			viewToServer.put(tablename, new HashSet<Server>(selectedServers));
			if (!(selectedServers.size() > 1)) {
			} else {
				HashMap<Server, Integer> counts = new HashMap<Server, Integer>();

				for (Server server : selectedServers) {
					counts.put(server, 0);
				}

				boolean flag = true;
				for (int i = 0; i < tid.size(); i++) {
					for (int j = 1; j < selectedServers.size(); j++) {
						if (flag) {
							flag = false;
						} else {
						}
					}
				}
			}
			frag_owner.executeUpdate(query + select + from);

		}
		executor = Executors.newFixedThreadPool(threads);

		for (Server server : serverToAttribute.keySet()) {
			final Server tmp = new Server(server);
			executor.submit(new Thread() {
				public void run() {
					try {
						tmp.executeUpdate("ANALYZE;");
					} catch (ClassNotFoundException | SQLException e) {
						e.printStackTrace();
					}
				}
			});
		}

		executor.shutdown();

		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			this.save(dbname);
		} catch (IOException e) {
			System.out.println("Could not save the fragmentation!");
			e.printStackTrace();
		}
		return true;
	}

	// the threads that set up table fragments
	private class insertThread implements Runnable {
		Server server;
		String suffix;
		String tname;
		HashMap<String, Table> tables;

		public insertThread(Server server, String suffix, int batchsize, String table, HashMap<String, Table> tables,
				Long limit) {
			this.server = server;
			this.suffix = suffix;
			this.tname = table;
			this.tables = tables;
		}

		public void run() {
			if (frag_owner.getType().equals("psql")) {
				try {
					runPsql();
				} catch (ClassNotFoundException | SQLException e) {
					e.printStackTrace();
				}
			}
		}

		public void runPsql() throws ClassNotFoundException, SQLException {
			server.open(server.getHost(), server.getPort(), dbname + suffix, server.getUsername(),
					server.getPassword());

			final String orgName = Thread.currentThread().getName();
			Thread.currentThread().setName(orgName + "," + tname);
			Table table = tables.get(tname);
			String in = "COPY " + server.getName() + "_" + tname + suffix;
			String createTable = "CREATE TABLE " + server.getName() + "_" + tname + suffix;
			String createForeignTable = "IMPORT FOREIGN SCHEMA public LIMIT TO(" + server.getName() + tname + suffix
					+ " ) FROM SERVER " + server.getName() + " INTO public;";
			String primaryKey = " PRIMARY KEY( ";
			String out = "COPY " + tname;

			List<Attribute> attributes = new ArrayList<Attribute>(table.getAttributes());
			for (int i = 0; i < attributes.size(); i++) {
				if (i == 0) {
					createTable += " ( ";
					createForeignTable += " ( ";
					in += "(";
					out += "( ";
				} else {
					createTable += ", ";
					createForeignTable += ", ";
					in += ",";
					out += ", ";
				}
				Attribute attr = attributes.get(i);
				String typeName = attr.getSQLType();
				if (attr.isTid()) {

					createTable += attr.getName() + " " + typeName;
					createForeignTable += attr.getName() + " " + typeName;
					primaryKey += attr.getName() + ",";
				} else {
					createTable += attr.getName() + " " + typeName;
					createForeignTable += attr.getName() + " " + typeName;
				}

				out += attr.getName();
				in += attr.getName();
			}
			primaryKey = primaryKey.substring(0, primaryKey.length() - 1) + ")";
			createTable += " ," + primaryKey + " )";
			createForeignTable += " )";

			server.executeUpdate("DROP TABLE IF EXISTS " + server.getName() + "_" + tname + suffix);
			server.executeUpdate(createTable);

			in += ") FROM STDIN;";
			out += ")  TO STDOUT;";

			Connection connection = new Server(orig_owner).getConnection();

			byte[] buf = new byte[8192];
			try {
				connection.setAutoCommit(false);
				PGCopyInputStream input = new PGCopyInputStream((PGConnection) connection, out);
				Connection connection2 = null;
				connection2 = server.getConnection();
				connection2.setAutoCommit(false);
				PGCopyOutputStream output = new PGCopyOutputStream((PGConnection) connection2, in);
				int bytesRead = input.read(buf, 0, 8192);
				while (bytesRead > 0) {
					output.writeToCopy(buf, 0, bytesRead);
					bytesRead = input.read(buf, 0, 8192);
				}
				output.endCopy();
				connection2.commit();
				connection2.close();
				connection.commit();
				connection.close();
				orig_owner.close();

				if (!server.isOwner()) {
					connection = new Server(frag_owner).getConnection();
					connection.setAutoCommit(false);
					Statement stmt = connection.createStatement();
					createForeignTable = "IMPORT FOREIGN SCHEMA public LIMIT TO(" + server.getName() + "_" + tname
							+ suffix + " ) FROM SERVER " + server.getName() + " INTO public"+";";
					//System.out.println(createForeignTable);
					stmt.executeUpdate(createForeignTable);
					connection.commit();
				}	
				connection = new Server(frag_owner).getConnection();
				connection.setAutoCommit(false);
				Statement stmt = connection.createStatement();
				String analyze = "ANALYZE public." + server.getName() + "_" + tname + suffix;
				stmt.execute(analyze);

				connection.commit();
				connection.close();
				frag_owner.close();
				synchronized (sb) {
					sb.print();
				}
				synchronized (tablefragments) {
					if (!tablefragments.containsKey(tname)) {
						tablefragments.put(tname, new HashSet<Server>());
					}
					tablefragments.get(tname).add(server);
				}

			} catch (SQLException | IOException e) {
				e.printStackTrace();
				System.exit(0);
			}
		}

	}

	// threads that prepare the table fragment schemas
	class prepareThread implements Runnable {
		Server server;
		String suffix;
		int batchsize;
		Server frag_owner;

		public prepareThread(Server server,Server frag_owner, String suffix, int batchsize) {
			this.server = server;
			this.suffix = suffix;
			this.batchsize = batchsize;
			this.frag_owner=frag_owner;
		}

		public void run() {
			if (orig_owner.getType().equals("psql")) {
				try {
					runPsql();
				} catch (ClassNotFoundException | SQLException e) {
					e.printStackTrace();
				}
			}
		}

		public void runPsql() throws ClassNotFoundException, SQLException {
			server.open(server.getHost(), server.getPort(), server.getUsername(), server.getPassword());
			Statement stmt = server.createStatement();
			try {
				stmt.execute("CREATE EXTENSION postgres_fdw");
				stmt.close();
			} catch (Exception e) {

			}
			try {
				stmt.close();
			} catch (SQLException e2) {
				e2.printStackTrace();
			}
			ResultSet rs = server.executeQuery("SELECT 1 FROM pg_database WHERE datname = '" + dbname + suffix + "'");
			boolean exists = false;
			try {
				while (rs.next()) {
					exists = true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			if (!exists) {
				server.executeUpdate("CREATE DATABASE " + dbname + suffix + ";");
			} else {
				try {
					stmt = frag_owner.createStatement();
					stmt.executeUpdate("DROP SERVER IF EXISTS " + server.getName() + " CASCADE;");
					if (server.isOwner() == false) {
						server.executeQuery("SELECT pg_terminate_backend(pg_stat_activity.pid) "
								+ "FROM pg_stat_activity WHERE pg_stat_activity.datname = '" + dbname + suffix
								+ "'AND pid <> pg_backend_pid();");
						server.executeUpdate("DROP DATABASE IF EXISTS " + dbname + suffix + ";");
						server.executeUpdate("CREATE DATABASE " + dbname + suffix + ";");
					}
					stmt.executeUpdate("CREATE SERVER " + server.getName()
							+ " FOREIGN DATA WRAPPER postgres_fdw OPTIONS(dbname '" + dbname + suffix + "'," + "host '"
							+ server.getHost() + "', use_remote_estimate 'true');");
					stmt.executeUpdate("CREATE USER MAPPING FOR "+frag_owner.getUsername()+" SERVER " + server.getName() + " OPTIONS(user '"
							+ server.getUsername() + "',password '" + server.getPassword() + "');");

					stmt.close();

				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			server.open(server.getHost(), server.getPort(), dbname + suffix, server.getUsername(),
					server.getPassword());

			HashMap<String, String> domain = new HashMap<String, String>();
			rs = server.executeQuery(
					"SELECT t.typname as domain_name, pg_catalog.format_type(t.typbasetype, t.typtypmod) as data_type FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace LEFT JOIN pg_catalog.pg_constraint c ON t.oid = c.contypid WHERE t.typtype = 'd' ");
			try {
				while (rs.next()) {
					domain.put(rs.getString(1), rs.getString(2));

				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			for (Attribute attr : serverToAttribute.get(server)) {
				if (!attr.getDomain().getName().equals(attr.getDomain().getSQLType())) {
					if (!domain.containsKey(attr.getDomain().getName())) {
						server.executeUpdate(
								"CREATE DOMAIN " + attr.getDomain().getName() + " AS " + attr.getDomain().getSQLType());
						domain.put(attr.getDomain().getName(), attr.getDomain().getSQLType());
					}
				}
			}
		}
	}

	// saves the current state
	public void save(String fname) throws IOException {
		fname = fname.toLowerCase();
		File dir = new File(Paths.get(".").toAbsolutePath().normalize().toString() + "/fragmentations/" + fname);
		dir.mkdirs();
		FileOutputStream fout = new FileOutputStream(Paths.get(".").toAbsolutePath().normalize().toString()
				+ "/fragmentations/" + fname + "/" + fname + ".ser", false);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(this);
	}

	// loads a state
	public static DistributedDatabaseClient load(String fname) throws ClassNotFoundException, IOException {
		fname = fname.toLowerCase();
		FileInputStream fileIn = new FileInputStream(Paths.get(".").toAbsolutePath().normalize().toString()
				+ "/fragmentations/" + fname + "/" + fname + ".ser");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		DistributedDatabaseClient frag = (DistributedDatabaseClient) in.readObject();
		return frag;
	}

	// updates rows in a database table
	public void update(Update update, boolean view) throws Exception {
		Table table = tablenameToTable.get(update.getTables().get(0).getName().toLowerCase());
		if (table == null) {
			throw new Exception();
		}
		List<Column> specifiedColumns = update.getColumns();
		List<Attribute> columns = new ArrayList<Attribute>();
		for (Attribute attr : table.getAttributes()) {
			for (int i = 0; i < specifiedColumns.size(); i++) {
				Column column = specifiedColumns.get(i);
				if (attr.getName().equals(column.getColumnName().toLowerCase())) {
					columns.add(attr);
				}
			}
		}
		Set<Server> updateServers = new HashSet<Server>();
		for (Server server : serverToAttribute.keySet()) {
			for (Attribute attr : serverToAttribute.get(server)) {
				if (columns.contains(attr)) {
					updateServers.add(server);
				}
			}
		}
		HashMap<Attribute, Expression> columnToValue = new HashMap<Attribute, Expression>();
		List<Expression> values = update.getExpressions();
		for (int i = 0; i < values.size(); i++) {
			columnToValue.put(columns.get(i), values.get(i));
		}

		boolean hasWhere = true;

		if (update.getWhere() == null) {
			hasWhere = false;
		}

		Connection connection = frag_owner.getConnection();
		connection.setAutoCommit(false);
		Statement statement = connection.createStatement();
		try {
			if (!hasWhere) {
				for (Server server : updateServers) {
					String sqlUpdate = "UPDATE " + server.getName() + "_" + table.getName() + suffix + " ";
					String sqlSet = " SET ";
					for (Attribute attr : columns) {
						if (serverToAttribute.get(server).contains(attr)) {
							sqlSet += attr.getName() + "=" + columnToValue.get(attr) + ",";
						}
					}
					sqlSet = sqlSet.substring(0, sqlSet.length() - 1);
					statement.executeUpdate(sqlUpdate + sqlSet + ";");
				}
			} else {
				String subselect = "SELECT ";
				String attributes = " (";
				for (Attribute attr : table.getTid()) {
					subselect += attr.getName() + ",";
					attributes += attr.getName() + ",";
				}
				attributes = attributes.subSequence(0, attributes.length() - 1) + ") ";
				subselect = subselect.substring(0, subselect.length() - 1) + " FROM " + table.getName() + " WHERE "
						+ update.getWhere();
				String parsedQuery = subselect;
				if (!view) {
					parsedQuery = rewrite(subselect, true);
				}
				subselect = "CREATE TEMP TABLE tmpupdate ON COMMIT DROP AS (" + parsedQuery + ");";
				statement.executeUpdate(subselect + ";");

				subselect = " IN ( SELECT * FROM tmpupdate )";
				for (Server server : updateServers) {
					String sqlUpdate = "UPDATE " + server.getName() + "_" + table.getName() + suffix + " ";
					String sqlSet = " SET ";
					for (Attribute attr : columns) {
						if (serverToAttribute.get(server).contains(attr)) {
							sqlSet += attr.getName() + "=" + columnToValue.get(attr) + ",";
						}
					}
					sqlSet = sqlSet.substring(0, sqlSet.length() - 1);
					String sql = sqlUpdate + sqlSet + " WHERE " + attributes + subselect + ";";
					statement.executeUpdate(sql);
				}
			}
		} catch (Exception e) {
			connection.rollback();
			e.printStackTrace();
		}
		connection.commit();
		connection.close();
	}

	// inserts rows into a database table
	public void insert(Insert insert)
			throws JSQLParserException, InterruptedException, SQLException, ClassNotFoundException {
		Connection connection = frag_owner.getConnection();
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();
		for (Server server : serverToAttribute.keySet()) {
			HashMap<Attribute, Object> columnToValue = new HashMap<Attribute, Object>();
			List<Column> specifiedColumns = insert.getColumns();
			List<Attribute> columns = new ArrayList<Attribute>();
			Table table = tablenameToTable.get(insert.getTable().getName().toString().toLowerCase());
			if (specifiedColumns != null) {
				for (Attribute attr : table.getAttributes()) {
					for (int i = 0; i < specifiedColumns.size(); i++) {
						Column column = specifiedColumns.get(i);
						if (attr.getName().equals(column.getColumnName().toLowerCase())) {
							columns.add(attr);
						}
					}
				}
			} else {
				columns = table.getAttributes();
			}
			ItemsList il = insert.getItemsList();
			List<Expression> values = null;
			if (il instanceof ExpressionList) {
				values = ((ExpressionList) il).getExpressions();
			}
			if (values == null || values.size() != columns.size()) {
				return;
			}
			for (int i = 0; i < values.size(); i++) {
				columnToValue.put(columns.get(i), values.get(i));
			}

			boolean nothingToDo = true;
			for (Attribute attr : serverToAttribute.get(server)) {
				for (Attribute attr2 : columns) {
					if (attr2.equals(attr)) {
						nothingToDo = false;
					}
				}
			}
			if (nothingToDo) {
				return;
			}
			String sqlInsert = "INSERT INTO " + server.getName() + "_" + table.getName() + suffix + " ";
			String sqlColumns = "(";
			String sqlValues = " VALUES (";
			for (Attribute attr : columns) {
				if (serverToAttribute.get(server).contains(attr)) {
					sqlColumns += attr.getName() + ",";
					sqlValues += columnToValue.get(attr) + ",";
				}
			}
			sqlColumns = sqlColumns.substring(0, sqlColumns.length() - 1);
			sqlColumns += ") ";
			sqlValues = sqlValues.substring(0, sqlValues.length() - 1);
			sqlValues += ");";
			String sql = sqlInsert + sqlColumns + sqlValues;
			st.executeUpdate(sql);

		}
		connection.commit();
		connection.close();
	}

	// deletes rows from a database table
	public void delete(Delete delete, boolean view) throws Exception {
		Table table = tablenameToTable.get(delete.getTable().getName().toLowerCase());
		boolean hasWhere = true;
		if (table == null) {
			throw new Exception();
		}
		Set<Server> deleteServers = new HashSet<Server>();
		for (Server server : serverToAttribute.keySet()) {
			for (Attribute attr : serverToAttribute.get(server)) {
				if (attr.getTable().toLowerCase().equals(table.getName())) {
					deleteServers.add(server);
				}
			}
		}
		if (delete.getWhere() == null) {
			hasWhere = false;
		}
		Connection connection = frag_owner.getConnection();
		connection.setAutoCommit(false);
		Statement statement = connection.createStatement();
		try {
			if (!hasWhere) {
				for (Server server : deleteServers) {
					String sql = "DELETE FROM " + server.getName() + "_" + table.getName() + suffix + "; ";
					statement.executeUpdate(sql);
				}
			} else {
				String subselect = "SELECT ";
				String attributes = " (";
				for (Attribute attr : table.getTid()) {
					subselect += attr.getName() + ",";
					attributes += attr.getName() + ",";
				}
				attributes = attributes.subSequence(0, attributes.length() - 1) + ") ";
				subselect = subselect.substring(0, subselect.length() - 1) + " FROM " + table.getName() + " WHERE "
						+ delete.getWhere();
				String parsedQuery = subselect;
				if (!view) {
					parsedQuery = rewrite(subselect, true);
				}
				subselect = "CREATE TEMP TABLE tmpdelete ON COMMIT DROP AS (" + parsedQuery + ");";
				statement.executeUpdate(subselect + ";");
				subselect = " IN ( SELECT * FROM tmpdelete )";
				for (Server server : deleteServers) {
					String sql = "DELETE FROM " + server.getName() + "_" + table.getName() + suffix + " WHERE "
							+ attributes + subselect;
					statement.executeUpdate(sql + ";");
				}
			}
		} catch (Exception e) {
			connection.rollback();
			e.printStackTrace();
		}
		connection.commit();
		connection.close();
	}
}
