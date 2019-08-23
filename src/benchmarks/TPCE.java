package benchmarks;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;

import constraints.Constraint;
import constraints.Dependency;
import fragmentation.DistributedDatabaseClient;
import metadata.Server;
import metadata.Table;
import util.ModelSolver;
import util.StatusBar;
import util.Utility;

//The class to set up the TPC-E database and to execute the benchmark
public class TPCE {
	List<String> files;
	StatusBar sb=null;
	List<String> inProgress=null;

	public TPCE() {}
	
	//Executes the benchmark
	public static void tpce(List<Server> servers,boolean load,double timeout) throws Exception{
		ModelSolver.setVerbose(0);
		List<Server> serversCopy=new ArrayList<Server>(servers);
		Server server=new Server(servers.get(0));
		server.setName("Dummy");
		server.setOwner(false);
		serversCopy.add(server);
		server=new Server(servers.get(0));
		server.setName("Dummy2");
		server.setOwner(false);
		serversCopy.add(server);
		String dbname="tpce";
		Server owner=new Server(Utility.getOwner(servers));
		owner.open(dbname);
		List<Double> sf1s=new ArrayList<Double>(Arrays.asList(1.0, 2.0,4.0,8.0,16.0));
		List<Double> sf2s=new ArrayList<Double>(Arrays.asList(0.25,0.5,1.0,2.0));
		List<Double> sf3s=new ArrayList<Double>(Arrays.asList(0.5,1.0,2.0,4.0));
		
		for(Double sf1:sf1s){
			tpce(serversCopy,load,sf1,0,0,false,timeout,false);
		}
		for(Double sf1:sf1s){
			tpce(serversCopy,load,sf1,0,0,true,timeout,false);
		}
		for(Double sf2:sf2s){
			tpce(serversCopy,load,4.0,sf2,0,true,timeout,true);
		}
		for(Double sf3:sf3s){
			tpce(serversCopy,load,4.0,1.0,sf3,true,timeout,true);
		}	
	}
	
	//Class to execute a single run of the TPC-E benchmark with fixed scale factors
	public static void tpce(List<Server> servers,boolean load,double sf1,double sf2,double sf3,boolean closeness,double timeout,boolean preSolve) throws Exception{
		String dbname="tpce";
		Server owner=new Server(Utility.getOwner(servers));
		owner.open(dbname);
		DistributedDatabaseClient frag=null;
	
		Utility.seed((int)(sf1*1000.0));
		System.out.println(sf1+" "+sf2+" "+sf3);
		List<Table> tables=Utility.getTables(owner.getHost(), owner.getPort(), owner.getUsername(), owner.getPassword(), dbname);
			
		List<Constraint> C=Utility.randomConstraints(tables,sf1, 4, true);
		C=Utility.wellDefined(tables, C,true);
		
		Utility.seed((int)(sf2*1000.0));
		List<Constraint> V=new ArrayList<Constraint>();
		V=Utility.randomConstraints(tables, sf2, 4, false);
			
		List<Constraint> CC=new ArrayList<Constraint>();
		CC=Utility.getClosenessConstraintsFromTables(tables);
		if(!closeness) {
			CC=new ArrayList<Constraint>();
		}
		
		Utility.seed((int)(sf3*1000.0));
		List<Dependency> D=new ArrayList<Dependency>();
		D=Utility.randomDependencies(tables, sf3, 2,4,1,4);	
			
		double ownerCapacity=Utility.suggestOwnerCapacity(tables, C, D);
		Utility.getOwner(servers).setCapacity(ownerCapacity);
			
		double[] a=Utility.suggestWeights(servers.size(), V, CC);
		
		frag=ModelSolver.solve(tables,servers,C,V,CC,D,a,1.0e-10,timeout,preSolve);
		System.out.println("Privacy Preserving: "+Utility.checkFragmentation(frag, C,D));
		
	}
	
	//Function to set up the TPC-E schema in PostgreSQL
	public void setUpSchema(String host, int port, String username,
			String password, String dbname) throws ClassNotFoundException, SQLException{
		Server server = new Server("server0", host, port,
				username, password, Double.MAX_VALUE, 1);
		ResultSet rs = server.executeQuery("SELECT 1 FROM pg_database WHERE datname = '" + dbname + "'");
		boolean exists = false;
		try {
			while (rs.next()) {
				exists = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (exists) {
			server.executeQuery("SELECT pg_terminate_backend(pg_stat_activity.pid) " +
					"FROM pg_stat_activity WHERE pg_stat_activity.datname = '"+dbname+ "'AND pid <> pg_backend_pid();");
			server.executeUpdate("DROP DATABASE IF EXISTS " + dbname  + ";");
		}
		server.executeUpdate("VACUUM;");
		server.executeUpdate("CREATE DATABASE " + dbname  + ";");
		server.open(dbname);		
	}

	//Function load data from .csv files into the database
	public void readCSV(String host, int port, String username,
			String password, String dbname, String directory, String delimiter) throws ClassNotFoundException, SQLException {
		String dir = directory;
		File folder = new File(dir);
		files = listFilesForFolder(folder);
		Collections.shuffle(files, new Random(2));
		Server server = new Server("server0", host, port,
				username, password, dbname, Double.MAX_VALUE, 1);
		System.out.println("Deleting");
		ResultSet rs = server.executeQuery("SELECT table_name "
				+ "FROM information_schema.tables "
				+ "WHERE table_schema='public' "
				+ "AND table_type='BASE TABLE';");
		List<String> tablenames = new ArrayList<String>();
		try {
			while (rs.next()) {
				tablenames.add(rs.getString(1));
			}
		} catch (Exception e) {

		}
		boolean flag2 = true;
		int j = 0;
		while (flag2) {
			flag2 = false;
			j++;
			Collections.shuffle(tablenames, new Random());
			for (String tablename : tablenames) {
				flag2 = false;
				try {
					server.executeUpdate("Alter table " + tablename
							+ " disable trigger all; Delete  from " + tablename
							+ ";Alter table " + tablename
							+ " enable trigger all");
				} catch (Exception e) {
					System.out.println(e.getMessage());
					flag2 = true;
					break;
				}
			}

		}
		System.out.println("Finished deletion");
		Collections.shuffle(files, new Random());
		rs = server.executeQuery("select R.TABLE_NAME, U.TABLE_NAME "
				+ "from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE U "
				+ "inner join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS FK "
				+ "on U.CONSTRAINT_CATALOG = FK.UNIQUE_CONSTRAINT_CATALOG "
				+ "and U.CONSTRAINT_SCHEMA = FK.UNIQUE_CONSTRAINT_SCHEMA "
				+ "and U.CONSTRAINT_NAME = FK.UNIQUE_CONSTRAINT_NAME "
				+ "inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE R "
				+ "ON R.CONSTRAINT_CATALOG = FK.CONSTRAINT_CATALOG "
				+ "AND R.CONSTRAINT_SCHEMA = FK.CONSTRAINT_SCHEMA "
				+ "AND R.CONSTRAINT_NAME = FK.CONSTRAINT_NAME ;");
		List<String[]> foreignkey = new ArrayList<String[]>();
		try {
			while (rs.next()) {
				foreignkey.add(new String[] { rs.getString(1), rs.getString(2) });
			}
		} catch (Exception e) {

		}
		boolean flag = true;
		while (flag) {
			flag = false;
			for (String[] fk : foreignkey) {
				String tmp1 = fk[0];
				String tmp2 = fk[1];
				if (files.indexOf(tmp1 + ".txt") < 0) {
					files.remove(tmp1 + ".txt");
					files.add(tmp1 + ".txt");
					flag = true;
					continue;
				}
				if (files.indexOf(tmp2 + ".txt") < 0) {
					files.remove(tmp2 + ".txt");
					files.add(tmp2 + ".txt");
					flag = true;
					continue;
				}
				if (files.indexOf(tmp1 + ".txt") < files.indexOf(tmp2 + ".txt")) {
					Collections.swap(files, files.indexOf(tmp1 + ".txt"),
							files.indexOf(tmp2 + ".txt"));
					flag = true;
				}
			}

		}
		System.out.println(files);
		sb=new StatusBar(files.size());

		ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		inProgress = new ArrayList<String>();
		while (!files.isEmpty()) {
			List<String> files2=new ArrayList<String>();
			synchronized(files){
				files2 = new ArrayList<String>(files);
				files2.removeAll(inProgress);
				
			}
			for (String file : files2) {
				String tablename = file.substring(0, file.lastIndexOf('.'));
				if (inProgress.contains(file)) {
					continue;
				}
				boolean cont=false;
				synchronized(inProgress){
					for (String file2 : inProgress) {
						for (String[] fk : foreignkey) {
							String tmp1 = fk[0]+".txt";
							String tmp2 = fk[1]+".txt";;
							if(tmp1.equals(file) && tmp2.equals(file2)){
								cont=true;
							}
						}
					}	
				}
				if(cont){
					continue;
				}
				cont = false;
				for (String[] fk : foreignkey) {
					if (tablename.equals(fk[0]) && files2.contains(fk[1]+".txt")) {
						cont = true;
					}
				}
				if (cont) {
					continue;
				}
				synchronized(inProgress){
					inProgress.add(file);
				}
				exec.submit(new WorkerThread(dir, file, delimiter, new Server(
								server)));
				
				
			}
		}
		exec.shutdown();
		try {
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	//Thread to fill the TPC-E database with data
	private class WorkerThread implements Runnable {
		Server server;
		String dir;
		String file;
		String delimiter;

		public WorkerThread(String dir, String file, String delimiter,
				Server server) {
			this.server = server;
			this.dir = dir;
			this.file = file;
			this.delimiter = delimiter;
		}

		public void run() {
			synchronized (files) {
				if(!files.contains(file)){
					return;
				}
			}
			Connection c=null;
			try {
				c = server.getConnection();
			} catch (ClassNotFoundException | SQLException e1) {
				e1.printStackTrace();
			}
			String tablename = file.substring(0, file.lastIndexOf('.'));
			try {
				c.setAutoCommit(false);
				FileInputStream fis = new FileInputStream(dir + "/" + file);
				byte[] buf = new byte[4096 * 4096];
				PGCopyOutputStream output = new PGCopyOutputStream(
						(PGConnection) c, "COPY " + tablename
								+ " FROM STDIN DELIMITER '" + delimiter
								+ "' CSV;");
				int bytesRead = fis.read(buf, 0, 4096 * 4096);
				while (bytesRead > 0) {
					output.writeToCopy(buf, 0, bytesRead);
					bytesRead = fis.read(buf, 0, 4096 * 4096);
				}
				fis.close();
				output.endCopy();
				c.commit();
				c.close();

			} catch (Exception e) {
				synchronized (files) {
					int i1 = files.indexOf(file);
					if(file.equals("trade_request.txt")){
						sb.print();
						files.remove(file);
					}
					else{
						files.remove(file);
						files.add(file);
					}
				}
				return;
			}
			
			synchronized (files) {
				//System.out.println(file+", ");
				sb.print();
				files.remove(file);
			}
			synchronized(inProgress){
				inProgress.remove(file);
			}
		}
	}


	private static List<String> listFilesForFolder(final File folder) {
		List<String> filenames = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry);
			} else {
				if (fileEntry.getName().contains(".txt"))
					filenames.add(fileEntry.getName());
			}
		}
		return filenames;
	}
	
	//Creates the TPC-E schema in a PostgreSQL database
	private final static String schema="\r\n" + 
			"\r\n" + 
			"SET statement_timeout = 0;\r\n" + 
			"SET lock_timeout = 0;\r\n" + 
			"SET idle_in_transaction_session_timeout = 0;\r\n" + 
			"SET client_encoding = 'UTF8';\r\n" + 
			"SET standard_conforming_strings = on;\r\n" + 
			"SET check_function_bodies = false;\r\n" + 
			"SET client_min_messages = warning;\r\n" + 
			"SET row_security = off;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE EXTENSION IF NOT EXISTS dblink WITH SCHEMA public;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"COMMENT ON EXTENSION dblink IS 'connect to other PostgreSQL databases from within a database';\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"SET search_path = public, pg_catalog;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE DOMAIN balance_t AS numeric(12,2);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER DOMAIN balance_t OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE DOMAIN fin_agg_t AS numeric(15,2) NOT NULL;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER DOMAIN fin_agg_t OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE DOMAIN ident_t AS bigint;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER DOMAIN ident_t OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE DOMAIN s_count_t AS bigint;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER DOMAIN s_count_t OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE DOMAIN s_price_t AS numeric(8,2);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER DOMAIN s_price_t OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE DOMAIN s_qty_t AS integer;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER DOMAIN s_qty_t OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE DOMAIN trade_t AS bigint;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER DOMAIN trade_t OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE DOMAIN value_t AS numeric(10,2);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER DOMAIN value_t OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"SET default_tablespace = '';\r\n" + 
			"\r\n" + 
			"SET default_with_oids = false;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE account_permission (\r\n" + 
			"    ap_ca_id ident_t NOT NULL,\r\n" + 
			"    ap_acl character varying(4) NOT NULL,\r\n" + 
			"    ap_tax_id character varying(20) NOT NULL,\r\n" + 
			"    ap_l_name character varying(30) NOT NULL,\r\n" + 
			"    ap_f_name character varying(30) NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE account_permission OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE address (\r\n" + 
			"    ad_id ident_t NOT NULL,\r\n" + 
			"    ad_line1 character varying(80),\r\n" + 
			"    ad_line2 character varying(80),\r\n" + 
			"    ad_zc_code character varying(12) NOT NULL,\r\n" + 
			"    ad_ctry character varying(80)\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE address OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE broker (\r\n" + 
			"    b_id ident_t NOT NULL,\r\n" + 
			"    b_st_id character varying(4) NOT NULL,\r\n" + 
			"    b_name character varying(100) NOT NULL,\r\n" + 
			"    b_num_trades integer NOT NULL,\r\n" + 
			"    b_comm_total balance_t NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE broker OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE cash_transaction (\r\n" + 
			"    ct_t_id trade_t NOT NULL,\r\n" + 
			"    ct_dts timestamp without time zone NOT NULL,\r\n" + 
			"    ct_amt value_t NOT NULL,\r\n" + 
			"    ct_name character varying(100)\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE cash_transaction OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE charge (\r\n" + 
			"    ch_tt_id character varying(3) NOT NULL,\r\n" + 
			"    ch_c_tier smallint NOT NULL,\r\n" + 
			"    ch_chrg value_t,\r\n" + 
			"    CONSTRAINT charge_ch_chrg_check CHECK (((ch_chrg)::numeric > (0)::numeric))\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE charge OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE commission_rate (\r\n" + 
			"    cr_c_tier smallint NOT NULL,\r\n" + 
			"    cr_tt_id character varying(3) NOT NULL,\r\n" + 
			"    cr_ex_id character varying(6) NOT NULL,\r\n" + 
			"    cr_from_qty s_qty_t NOT NULL,\r\n" + 
			"    cr_to_qty s_qty_t NOT NULL,\r\n" + 
			"    cr_rate numeric(5,2) NOT NULL,\r\n" + 
			"    CONSTRAINT commission_rate_check CHECK (((cr_to_qty)::integer > (cr_from_qty)::integer)),\r\n" + 
			"    CONSTRAINT commission_rate_cr_from_qty_check CHECK (((cr_from_qty)::integer >= 0)),\r\n" + 
			"    CONSTRAINT commission_rate_cr_rate_check CHECK ((cr_rate >= (0)::numeric))\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE commission_rate OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE company (\r\n" + 
			"    co_id ident_t NOT NULL,\r\n" + 
			"    co_st_id character varying(4) NOT NULL,\r\n" + 
			"    co_name character varying(60) NOT NULL,\r\n" + 
			"    co_in_id character varying(2) NOT NULL,\r\n" + 
			"    co_sp_rate character varying(4) NOT NULL,\r\n" + 
			"    co_ceo character varying(100) NOT NULL,\r\n" + 
			"    co_ad_id ident_t NOT NULL,\r\n" + 
			"    co_desc character varying(150) NOT NULL,\r\n" + 
			"    co_open_date date NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE company OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE company_competitor (\r\n" + 
			"    cp_co_id ident_t NOT NULL,\r\n" + 
			"    cp_comp_co_id ident_t NOT NULL,\r\n" + 
			"    cp_in_id character varying(2) NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE company_competitor OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE customer (\r\n" + 
			"    c_id ident_t NOT NULL,\r\n" + 
			"    c_tax_id character varying(20) NOT NULL,\r\n" + 
			"    c_st_id character varying(4) NOT NULL,\r\n" + 
			"    c_l_name character varying(30) NOT NULL,\r\n" + 
			"    c_f_name character varying(30) NOT NULL,\r\n" + 
			"    c_m_name character(1),\r\n" + 
			"    c_gndr character(1),\r\n" + 
			"    c_tier smallint NOT NULL,\r\n" + 
			"    c_dob date NOT NULL,\r\n" + 
			"    c_ad_id ident_t NOT NULL,\r\n" + 
			"    c_ctry_1 character varying(3),\r\n" + 
			"    c_area_1 character varying(3),\r\n" + 
			"    c_local_1 character varying(10),\r\n" + 
			"    c_ext_1 character varying(5),\r\n" + 
			"    c_ctry_2 character varying(3),\r\n" + 
			"    c_area_2 character varying(3),\r\n" + 
			"    c_local_2 character varying(10),\r\n" + 
			"    c_ext_2 character varying(5),\r\n" + 
			"    c_ctry_3 character varying(3),\r\n" + 
			"    c_area_3 character varying(3),\r\n" + 
			"    c_local_3 character varying(10),\r\n" + 
			"    c_ext_3 character varying(5),\r\n" + 
			"    c_email_1 character varying(50),\r\n" + 
			"    c_email_2 character varying(50)\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE customer OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE customer_account (\r\n" + 
			"    ca_id ident_t NOT NULL,\r\n" + 
			"    ca_b_id ident_t NOT NULL,\r\n" + 
			"    ca_c_id ident_t NOT NULL,\r\n" + 
			"    ca_name character varying(50),\r\n" + 
			"    ca_tax_st smallint NOT NULL,\r\n" + 
			"    ca_bal balance_t NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE customer_account OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE customer_taxrate (\r\n" + 
			"    cx_tx_id character varying(4) NOT NULL,\r\n" + 
			"    cx_c_id ident_t NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE customer_taxrate OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE daily_market (\r\n" + 
			"    dm_date date NOT NULL,\r\n" + 
			"    dm_s_symb character varying(15) NOT NULL,\r\n" + 
			"    dm_close s_price_t NOT NULL,\r\n" + 
			"    dm_high s_price_t NOT NULL,\r\n" + 
			"    dm_low s_price_t NOT NULL,\r\n" + 
			"    dm_vol s_count_t NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE daily_market OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE exchange (\r\n" + 
			"    ex_id character varying(6) NOT NULL,\r\n" + 
			"    ex_name character varying(100) NOT NULL,\r\n" + 
			"    ex_num_symb integer NOT NULL,\r\n" + 
			"    ex_open integer NOT NULL,\r\n" + 
			"    ex_close integer NOT NULL,\r\n" + 
			"    ex_desc character varying(150),\r\n" + 
			"    ex_ad_id ident_t NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE exchange OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE financial (\r\n" + 
			"    fi_co_id ident_t NOT NULL,\r\n" + 
			"    fi_year integer NOT NULL,\r\n" + 
			"    fi_qtr smallint NOT NULL,\r\n" + 
			"    fi_qtr_start_date date NOT NULL,\r\n" + 
			"    fi_revenue fin_agg_t NOT NULL,\r\n" + 
			"    fi_net_earn fin_agg_t NOT NULL,\r\n" + 
			"    fi_basic_eps value_t NOT NULL,\r\n" + 
			"    fi_dilut_eps value_t NOT NULL,\r\n" + 
			"    fi_margin value_t NOT NULL,\r\n" + 
			"    fi_inventory fin_agg_t NOT NULL,\r\n" + 
			"    fi_assets fin_agg_t NOT NULL,\r\n" + 
			"    fi_liability fin_agg_t NOT NULL,\r\n" + 
			"    fi_out_basic s_count_t NOT NULL,\r\n" + 
			"    fi_out_dilut s_count_t NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE financial OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE holding (\r\n" + 
			"    h_t_id trade_t NOT NULL,\r\n" + 
			"    h_ca_id ident_t NOT NULL,\r\n" + 
			"    h_s_symb character varying(15) NOT NULL,\r\n" + 
			"    h_dts timestamp without time zone NOT NULL,\r\n" + 
			"    h_price s_price_t NOT NULL,\r\n" + 
			"    h_qty s_qty_t NOT NULL,\r\n" + 
			"    CONSTRAINT holding_h_price_check CHECK (((h_price)::numeric > (0)::numeric))\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE holding OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE holding_history (\r\n" + 
			"    hh_h_t_id trade_t NOT NULL,\r\n" + 
			"    hh_t_id trade_t NOT NULL,\r\n" + 
			"    hh_before_qty s_qty_t NOT NULL,\r\n" + 
			"    hh_after_qty s_qty_t NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE holding_history OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE holding_summary (\r\n" + 
			"    hs_ca_id ident_t NOT NULL,\r\n" + 
			"    hs_s_symb character varying(15) NOT NULL,\r\n" + 
			"    hs_qty s_qty_t NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE holding_summary OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE industry (\r\n" + 
			"    in_id character varying(2) NOT NULL,\r\n" + 
			"    in_name character varying(50) NOT NULL,\r\n" + 
			"    in_sc_id character varying(2) NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE industry OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE last_trade (\r\n" + 
			"    lt_s_symb character varying(15) NOT NULL,\r\n" + 
			"    lt_dts timestamp without time zone NOT NULL,\r\n" + 
			"    lt_price s_price_t NOT NULL,\r\n" + 
			"    lt_open_price s_price_t NOT NULL,\r\n" + 
			"    lt_vol s_count_t\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE last_trade OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE news_item (\r\n" + 
			"    ni_id ident_t NOT NULL,\r\n" + 
			"    ni_headline character varying(80) NOT NULL,\r\n" + 
			"    ni_summary character varying(255) NOT NULL,\r\n" + 
			"    ni_item bytea NOT NULL,\r\n" + 
			"    ni_dts timestamp without time zone NOT NULL,\r\n" + 
			"    ni_source character varying(30) NOT NULL,\r\n" + 
			"    ni_author character varying(30)\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE news_item OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE news_xref (\r\n" + 
			"    nx_ni_id ident_t NOT NULL,\r\n" + 
			"    nx_co_id ident_t NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE news_xref OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE sector (\r\n" + 
			"    sc_id character varying(2) NOT NULL,\r\n" + 
			"    sc_name character varying(30) NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE sector OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE security (\r\n" + 
			"    s_symb character varying(15) NOT NULL,\r\n" + 
			"    s_issue character varying(6) NOT NULL,\r\n" + 
			"    s_st_id character varying(4) NOT NULL,\r\n" + 
			"    s_name character varying(70) NOT NULL,\r\n" + 
			"    s_ex_id character varying(6) NOT NULL,\r\n" + 
			"    s_co_id ident_t NOT NULL,\r\n" + 
			"    s_num_out s_count_t NOT NULL,\r\n" + 
			"    s_start_date date NOT NULL,\r\n" + 
			"    s_exch_date date NOT NULL,\r\n" + 
			"    s_pe value_t NOT NULL,\r\n" + 
			"    s_52wk_high s_price_t NOT NULL,\r\n" + 
			"    s_52wk_high_date date NOT NULL,\r\n" + 
			"    s_52wk_low s_price_t NOT NULL,\r\n" + 
			"    s_52wk_low_date date NOT NULL,\r\n" + 
			"    s_dividend value_t NOT NULL,\r\n" + 
			"    s_yield numeric(5,2) NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE security OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE SEQUENCE seq_trade_id\r\n" + 
			"    START WITH 1\r\n" + 
			"    INCREMENT BY 1\r\n" + 
			"    NO MINVALUE\r\n" + 
			"    NO MAXVALUE\r\n" + 
			"    CACHE 1;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE seq_trade_id OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE settlement (\r\n" + 
			"    se_t_id trade_t NOT NULL,\r\n" + 
			"    se_cash_type character varying(40) NOT NULL,\r\n" + 
			"    se_cash_due_date date NOT NULL,\r\n" + 
			"    se_amt value_t NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE settlement OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE status_type (\r\n" + 
			"    st_id character varying(4) NOT NULL,\r\n" + 
			"    st_name character varying(10) NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE status_type OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE taxrate (\r\n" + 
			"    tx_id character varying(4) NOT NULL,\r\n" + 
			"    tx_name character varying(50) NOT NULL,\r\n" + 
			"    tx_rate numeric(6,5) NOT NULL,\r\n" + 
			"    CONSTRAINT taxrate_tx_rate_check CHECK ((tx_rate >= (0)::numeric))\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE taxrate OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE trade (\r\n" + 
			"    t_id trade_t NOT NULL,\r\n" + 
			"    t_dts timestamp without time zone NOT NULL,\r\n" + 
			"    t_st_id character varying(4) NOT NULL,\r\n" + 
			"    t_tt_id character varying(3) NOT NULL,\r\n" + 
			"    t_is_cash boolean NOT NULL,\r\n" + 
			"    t_s_symb character varying(15) NOT NULL,\r\n" + 
			"    t_qty s_qty_t NOT NULL,\r\n" + 
			"    t_bid_price s_price_t NOT NULL,\r\n" + 
			"    t_ca_id ident_t NOT NULL,\r\n" + 
			"    t_exec_name character varying(64) NOT NULL,\r\n" + 
			"    t_trade_price s_price_t,\r\n" + 
			"    t_chrg value_t NOT NULL,\r\n" + 
			"    t_comm value_t NOT NULL,\r\n" + 
			"    t_tax value_t NOT NULL,\r\n" + 
			"    t_lifo boolean NOT NULL,\r\n" + 
			"    CONSTRAINT trade_t_bid_price_check CHECK (((t_bid_price)::numeric > (0)::numeric)),\r\n" + 
			"    CONSTRAINT trade_t_chrg_check CHECK (((t_chrg)::numeric >= (0)::numeric)),\r\n" + 
			"    CONSTRAINT trade_t_comm_check CHECK (((t_comm)::numeric >= (0)::numeric)),\r\n" + 
			"    CONSTRAINT trade_t_qty_check CHECK (((t_qty)::integer > 0)),\r\n" + 
			"    CONSTRAINT trade_t_tax_check CHECK (((t_tax)::numeric >= (0)::numeric))\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE trade OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE trade_history (\r\n" + 
			"    th_t_id trade_t NOT NULL,\r\n" + 
			"    th_dts timestamp without time zone NOT NULL,\r\n" + 
			"    th_st_id character varying(4) NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE trade_history OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE trade_request (\r\n" + 
			"    tr_t_id trade_t NOT NULL,\r\n" + 
			"    tr_tt_id character varying(3) NOT NULL,\r\n" + 
			"    tr_s_symb character varying(15) NOT NULL,\r\n" + 
			"    tr_qty s_qty_t NOT NULL,\r\n" + 
			"    tr_bid_price s_price_t NOT NULL,\r\n" + 
			"    tr_b_id ident_t NOT NULL,\r\n" + 
			"    CONSTRAINT trade_request_tr_bid_price_check CHECK (((tr_bid_price)::numeric > (0)::numeric)),\r\n" + 
			"    CONSTRAINT trade_request_tr_qty_check CHECK (((tr_qty)::integer > 0))\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE trade_request OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE trade_type (\r\n" + 
			"    tt_id character varying(3) NOT NULL,\r\n" + 
			"    tt_name character varying(12) NOT NULL,\r\n" + 
			"    tt_is_sell boolean NOT NULL,\r\n" + 
			"    tt_is_mrkt boolean NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE trade_type OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE watch_item (\r\n" + 
			"    wi_wl_id ident_t NOT NULL,\r\n" + 
			"    wi_s_symb character varying(15) NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE watch_item OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE watch_list (\r\n" + 
			"    wl_id ident_t NOT NULL,\r\n" + 
			"    wl_c_id ident_t NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE watch_list OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE TABLE zip_code (\r\n" + 
			"    zc_code character varying(12) NOT NULL,\r\n" + 
			"    zc_town character varying(80) NOT NULL,\r\n" + 
			"    zc_div character varying(80) NOT NULL\r\n" + 
			");\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE zip_code OWNER TO postgres;\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY account_permission\r\n" + 
			"    ADD CONSTRAINT pk_account_permission PRIMARY KEY (ap_ca_id, ap_tax_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY address\r\n" + 
			"    ADD CONSTRAINT pk_address PRIMARY KEY (ad_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY broker\r\n" + 
			"    ADD CONSTRAINT pk_broker PRIMARY KEY (b_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY cash_transaction\r\n" + 
			"    ADD CONSTRAINT pk_cash_transaction PRIMARY KEY (ct_t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY charge\r\n" + 
			"    ADD CONSTRAINT pk_charge PRIMARY KEY (ch_tt_id, ch_c_tier);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY commission_rate\r\n" + 
			"    ADD CONSTRAINT pk_commission_rate PRIMARY KEY (cr_c_tier, cr_tt_id, cr_ex_id, cr_from_qty);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY company\r\n" + 
			"    ADD CONSTRAINT pk_company PRIMARY KEY (co_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY company_competitor\r\n" + 
			"    ADD CONSTRAINT pk_company_competitor PRIMARY KEY (cp_co_id, cp_comp_co_id, cp_in_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY customer\r\n" + 
			"    ADD CONSTRAINT pk_customer PRIMARY KEY (c_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY customer_account\r\n" + 
			"    ADD CONSTRAINT pk_customer_account PRIMARY KEY (ca_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY customer_taxrate\r\n" + 
			"    ADD CONSTRAINT pk_customer_taxrate PRIMARY KEY (cx_tx_id, cx_c_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY daily_market\r\n" + 
			"    ADD CONSTRAINT pk_daily_market PRIMARY KEY (dm_date, dm_s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY exchange\r\n" + 
			"    ADD CONSTRAINT pk_exchange PRIMARY KEY (ex_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY financial\r\n" + 
			"    ADD CONSTRAINT pk_financial PRIMARY KEY (fi_co_id, fi_year, fi_qtr);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY holding\r\n" + 
			"    ADD CONSTRAINT pk_holding PRIMARY KEY (h_t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY holding_history\r\n" + 
			"    ADD CONSTRAINT pk_holding_history PRIMARY KEY (hh_h_t_id, hh_t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY holding_summary\r\n" + 
			"    ADD CONSTRAINT pk_holding_summary PRIMARY KEY (hs_ca_id, hs_s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY industry\r\n" + 
			"    ADD CONSTRAINT pk_industry PRIMARY KEY (in_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY last_trade\r\n" + 
			"    ADD CONSTRAINT pk_last_trade PRIMARY KEY (lt_s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY news_item\r\n" + 
			"    ADD CONSTRAINT pk_news_item PRIMARY KEY (ni_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY news_xref\r\n" + 
			"    ADD CONSTRAINT pk_news_xref PRIMARY KEY (nx_ni_id, nx_co_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY sector\r\n" + 
			"    ADD CONSTRAINT pk_sector PRIMARY KEY (sc_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY security\r\n" + 
			"    ADD CONSTRAINT pk_security PRIMARY KEY (s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY settlement\r\n" + 
			"    ADD CONSTRAINT pk_settlement PRIMARY KEY (se_t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY status_type\r\n" + 
			"    ADD CONSTRAINT pk_status_type PRIMARY KEY (st_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY taxrate\r\n" + 
			"    ADD CONSTRAINT pk_taxrate PRIMARY KEY (tx_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade\r\n" + 
			"    ADD CONSTRAINT pk_trade PRIMARY KEY (t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade_history\r\n" + 
			"    ADD CONSTRAINT pk_trade_history PRIMARY KEY (th_t_id, th_st_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade_request\r\n" + 
			"    ADD CONSTRAINT pk_trade_request PRIMARY KEY (tr_t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade_type\r\n" + 
			"    ADD CONSTRAINT pk_trade_type PRIMARY KEY (tt_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY watch_item\r\n" + 
			"    ADD CONSTRAINT pk_watch_item PRIMARY KEY (wi_wl_id, wi_s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY watch_list\r\n" + 
			"    ADD CONSTRAINT pk_watch_list PRIMARY KEY (wl_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY zip_code\r\n" + 
			"    ADD CONSTRAINT pk_zip_code PRIMARY KEY (zc_code);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_c_tax_id ON customer USING btree (c_tax_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_ca_c_id ON customer_account USING btree (ca_c_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_co_name ON company USING btree (co_name);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_dm_s_symb ON daily_market USING btree (dm_s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_hh_t_id ON holding_history USING btree (hh_t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_holding ON holding USING btree (h_ca_id, h_s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_security ON security USING btree (s_co_id, s_issue);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_t_ca_id ON trade USING btree (t_ca_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_t_s_symb ON trade USING btree (t_s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_t_st_id ON trade USING btree (t_st_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_tr_s_symb ON trade_request USING btree (tr_s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"CREATE INDEX i_wl_c_id ON watch_list USING btree (wl_c_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY account_permission\r\n" + 
			"    ADD CONSTRAINT fk_account_permission_ca FOREIGN KEY (ap_ca_id) REFERENCES customer_account(ca_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY address\r\n" + 
			"    ADD CONSTRAINT fk_address FOREIGN KEY (ad_zc_code) REFERENCES zip_code(zc_code);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY broker\r\n" + 
			"    ADD CONSTRAINT fk_broker FOREIGN KEY (b_st_id) REFERENCES status_type(st_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY cash_transaction\r\n" + 
			"    ADD CONSTRAINT fk_cash_transaction FOREIGN KEY (ct_t_id) REFERENCES trade(t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY charge\r\n" + 
			"    ADD CONSTRAINT fk_charge FOREIGN KEY (ch_tt_id) REFERENCES trade_type(tt_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY commission_rate\r\n" + 
			"    ADD CONSTRAINT fk_commission_rate_ex FOREIGN KEY (cr_ex_id) REFERENCES exchange(ex_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY commission_rate\r\n" + 
			"    ADD CONSTRAINT fk_commission_rate_tt FOREIGN KEY (cr_tt_id) REFERENCES trade_type(tt_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY company\r\n" + 
			"    ADD CONSTRAINT fk_company_ad FOREIGN KEY (co_ad_id) REFERENCES address(ad_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY company_competitor\r\n" + 
			"    ADD CONSTRAINT fk_company_competitor_co FOREIGN KEY (cp_co_id) REFERENCES company(co_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY company_competitor\r\n" + 
			"    ADD CONSTRAINT fk_company_competitor_co2 FOREIGN KEY (cp_comp_co_id) REFERENCES company(co_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY company_competitor\r\n" + 
			"    ADD CONSTRAINT fk_company_competitor_in FOREIGN KEY (cp_in_id) REFERENCES industry(in_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY company\r\n" + 
			"    ADD CONSTRAINT fk_company_in FOREIGN KEY (co_in_id) REFERENCES industry(in_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY company\r\n" + 
			"    ADD CONSTRAINT fk_company_st FOREIGN KEY (co_st_id) REFERENCES status_type(st_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY customer_account\r\n" + 
			"    ADD CONSTRAINT fk_customer_account_b FOREIGN KEY (ca_b_id) REFERENCES broker(b_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY customer_account\r\n" + 
			"    ADD CONSTRAINT fk_customer_account_c FOREIGN KEY (ca_c_id) REFERENCES customer(c_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY customer\r\n" + 
			"    ADD CONSTRAINT fk_customer_ad FOREIGN KEY (c_ad_id) REFERENCES address(ad_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY customer\r\n" + 
			"    ADD CONSTRAINT fk_customer_st FOREIGN KEY (c_st_id) REFERENCES status_type(st_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY customer_taxrate\r\n" + 
			"    ADD CONSTRAINT fk_customer_taxrate_c FOREIGN KEY (cx_c_id) REFERENCES customer(c_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY customer_taxrate\r\n" + 
			"    ADD CONSTRAINT fk_customer_taxrate_tx FOREIGN KEY (cx_tx_id) REFERENCES taxrate(tx_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY daily_market\r\n" + 
			"    ADD CONSTRAINT fk_daily_market FOREIGN KEY (dm_s_symb) REFERENCES security(s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY exchange\r\n" + 
			"    ADD CONSTRAINT fk_exchange FOREIGN KEY (ex_ad_id) REFERENCES address(ad_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY financial\r\n" + 
			"    ADD CONSTRAINT fk_financial FOREIGN KEY (fi_co_id) REFERENCES company(co_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY holding_history\r\n" + 
			"    ADD CONSTRAINT fk_holding_history_t1 FOREIGN KEY (hh_h_t_id) REFERENCES trade(t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY holding_history\r\n" + 
			"    ADD CONSTRAINT fk_holding_history_t2 FOREIGN KEY (hh_t_id) REFERENCES trade(t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY holding\r\n" + 
			"    ADD CONSTRAINT fk_holding_hs FOREIGN KEY (h_ca_id, h_s_symb) REFERENCES holding_summary(hs_ca_id, hs_s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY holding_summary\r\n" + 
			"    ADD CONSTRAINT fk_holding_summary_ca FOREIGN KEY (hs_ca_id) REFERENCES customer_account(ca_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY holding_summary\r\n" + 
			"    ADD CONSTRAINT fk_holding_summary_s FOREIGN KEY (hs_s_symb) REFERENCES security(s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY holding\r\n" + 
			"    ADD CONSTRAINT fk_holding_t FOREIGN KEY (h_t_id) REFERENCES trade(t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY industry\r\n" + 
			"    ADD CONSTRAINT fk_industry FOREIGN KEY (in_sc_id) REFERENCES sector(sc_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY last_trade\r\n" + 
			"    ADD CONSTRAINT fk_last_trade FOREIGN KEY (lt_s_symb) REFERENCES security(s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY news_xref\r\n" + 
			"    ADD CONSTRAINT fk_news_xref_co FOREIGN KEY (nx_co_id) REFERENCES company(co_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY news_xref\r\n" + 
			"    ADD CONSTRAINT fk_news_xref_ni FOREIGN KEY (nx_ni_id) REFERENCES news_item(ni_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY security\r\n" + 
			"    ADD CONSTRAINT fk_security_co FOREIGN KEY (s_co_id) REFERENCES company(co_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY security\r\n" + 
			"    ADD CONSTRAINT fk_security_ex FOREIGN KEY (s_ex_id) REFERENCES exchange(ex_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY security\r\n" + 
			"    ADD CONSTRAINT fk_security_st FOREIGN KEY (s_st_id) REFERENCES status_type(st_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY settlement\r\n" + 
			"    ADD CONSTRAINT fk_settlement FOREIGN KEY (se_t_id) REFERENCES trade(t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade\r\n" + 
			"    ADD CONSTRAINT fk_trade_ca FOREIGN KEY (t_ca_id) REFERENCES customer_account(ca_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade_history\r\n" + 
			"    ADD CONSTRAINT fk_trade_history_st FOREIGN KEY (th_st_id) REFERENCES status_type(st_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade_history\r\n" + 
			"    ADD CONSTRAINT fk_trade_history_t FOREIGN KEY (th_t_id) REFERENCES trade(t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade_request\r\n" + 
			"    ADD CONSTRAINT fk_trade_request_b FOREIGN KEY (tr_b_id) REFERENCES broker(b_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade_request\r\n" + 
			"    ADD CONSTRAINT fk_trade_request_s FOREIGN KEY (tr_s_symb) REFERENCES security(s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade_request\r\n" + 
			"    ADD CONSTRAINT fk_trade_request_t FOREIGN KEY (tr_t_id) REFERENCES trade(t_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade_request\r\n" + 
			"    ADD CONSTRAINT fk_trade_request_tt FOREIGN KEY (tr_tt_id) REFERENCES trade_type(tt_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade\r\n" + 
			"    ADD CONSTRAINT fk_trade_s FOREIGN KEY (t_s_symb) REFERENCES security(s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade\r\n" + 
			"    ADD CONSTRAINT fk_trade_st FOREIGN KEY (t_st_id) REFERENCES status_type(st_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY trade\r\n" + 
			"    ADD CONSTRAINT fk_trade_tt FOREIGN KEY (t_tt_id) REFERENCES trade_type(tt_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY watch_item\r\n" + 
			"    ADD CONSTRAINT fk_watch_item_s FOREIGN KEY (wi_s_symb) REFERENCES security(s_symb);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY watch_item\r\n" + 
			"    ADD CONSTRAINT fk_watch_item_wl FOREIGN KEY (wi_wl_id) REFERENCES watch_list(wl_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"ALTER TABLE ONLY watch_list\r\n" + 
			"    ADD CONSTRAINT fk_watch_list FOREIGN KEY (wl_c_id) REFERENCES customer(c_id);\r\n" + 
			"\r\n" + 
			"\r\n" + 
			"";
}
