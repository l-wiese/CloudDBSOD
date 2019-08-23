package util;
import java.io.Console;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

import metadata.Server;
import metadata.Table;
import constraints.Constraint;
import constraints.Dependency;

import benchmarks.BenchmarkResultSet;

import fragmentation.DistributedDatabaseClient;

//class for the command line utility
public class CommandLineUtility {
	Console c = null;
	DistributedDatabaseClient frag=null;
	Scanner sc=null;
	List<Server> servers=null;
	Server owner=null;
	public CommandLineUtility(List<Server> servers) {
		super();
		this.c = System.console();
		this.sc=new Scanner(System.in);
		this.servers=servers;
		owner=new Server(Utility.getOwner(servers));
		
	}
	
	public CommandLineUtility(DistributedDatabaseClient frag) {
		super();
		this.c = System.console();
		this.frag=frag;
		this.sc=new Scanner(System.in);
		//sc.useDelimiter(";");
	}
	
	//Runs the command line utility
	public boolean executeCommand() throws JSQLParserException{
		boolean methodSet=false;
		while(true){
			if(frag==null){
				System.out.println("Which database do you want to use?");
				String query="";
				System.out.print("fragDB> ");
				query=sc.nextLine()+";";
				if(query.toLowerCase().equals(new String("EXIT;").toLowerCase())){
					System.exit(0);
				}
				String dbname=query.substring(0, query.length()-1);
				try{
					DistributedDatabaseClient.load(dbname);
					System.out.println("Existing fragmentation found, do you want to load it?[y|n]");
					System.out.print("fragDB> ");
					String tmp=sc.nextLine();
					while(!tmp.toLowerCase().equals("y") && !tmp.toLowerCase().equals("n")){
						System.out.println("Existing fragmentation found, do you want to load it?[y|n]");
						System.out.print("fragDB> ");
						tmp=sc.nextLine();
					}
					if(tmp.toLowerCase().equals("y")){
						frag=DistributedDatabaseClient.load(dbname);
						continue;
					}
					else if(tmp.toLowerCase().equals("n")){
						
					}
					else if(query.toLowerCase().replaceAll("\\s", "").equals(new String("exit;").toLowerCase())){
						System.exit(0);
					}
					else if(query.toLowerCase().replaceAll("\\s", "").equals(new String("exit").toLowerCase())){
						System.exit(0);
					}
					
					
				}
				catch(Exception e){
					
				}
				List<Table> tables=new ArrayList<Table>();
				try {
					owner.open(dbname);
					tables = Utility.getTables(owner.getHost(), owner.getPort(), owner.getUsername(), owner.getPassword(), dbname);
				} catch (SQLException | ClassNotFoundException e1) {
					System.out.println("Database \""+dbname+"\" does not exist!");
					continue;
				}
				
				if(!Files.isDirectory(Paths.get(Paths.get(".").toAbsolutePath().normalize().toString() + "/fragmentations/"+dbname))){
					System.out.println("No configuration for \""+dbname+"\" exists!");
					continue;
				}
				
				List<Constraint> C=new ArrayList<Constraint>();
				try {
					String filename=Paths.get(".").toAbsolutePath().normalize().toString() + "/fragmentations/"+dbname+"/confidentiality.txt";
					C = Utility.readConstraints(filename, tables);
				} catch (FileNotFoundException e) {
					System.out.println("No confidentiality constraints specified, are you sure you want to fragment this database? [y|n]");
					System.out.print("fragDB> ");
					String tmp=sc.nextLine();
					while(!tmp.toLowerCase().equals("y") && !tmp.toLowerCase().equals("n")){
						if(tmp.toLowerCase().replaceAll("\\s", "").equals(new String("exit;").toLowerCase())){
							System.exit(0);
						}
						else if(tmp.toLowerCase().replaceAll("\\s", "").equals(new String("exit").toLowerCase())){
							System.exit(0);
						}
						System.out.println("No confidentiality constraints specified, are you sure you want to fragment this database? [y|n]");
						System.out.print("fragDB> ");
						tmp=sc.nextLine();
					}
					if(tmp.toLowerCase().equals("y")){
					}
					else if(tmp.toLowerCase().equals("n")){
						continue;
					}
					
					
				}
				List<Constraint> C2=new ArrayList<Constraint>(C);
				C=Utility.wellDefined(tables, C,true);
				if(C2.size()!=C.size()){
					System.out.println("Set of confidentiality constraints is not well-defined. I fixed that for you!");
				}
				
				List<Constraint> V=new ArrayList<Constraint>();
				try {
					String filename=Paths.get(".").toAbsolutePath().normalize().toString() + "/fragmentations/"+dbname+"/visibility.txt";
					V=Utility.readConstraints(filename, tables);
				} catch (FileNotFoundException e) {
					System.out.println("No visibility constraints found");
				}
				
				List<Constraint> CC=new ArrayList<Constraint>();
				try {
					String filename=Paths.get(".").toAbsolutePath().normalize().toString() + "/fragmentations/"+dbname+"/closeness.txt";
					CC=Utility.readConstraints(filename, tables);
				} catch (FileNotFoundException e) {
					System.out.println("No closeness constraints found. Should I create one for each table?[y|n]");
					System.out.print("fragDB> ");
					String tmp=sc.nextLine();
					while(!tmp.toLowerCase().equals("y") && !tmp.toLowerCase().equals("n")){
						if(tmp.toLowerCase().replaceAll("\\s", "").equals(new String("exit;").toLowerCase())){
							System.exit(0);
						}
						else if(tmp.toLowerCase().replaceAll("\\s", "").equals(new String("exit").toLowerCase())){
							System.exit(0);
						}
						System.out.println("No closeness constraints found. Should I create one for each table?[y|n]");
						System.out.print("fragDB> ");
						tmp=sc.nextLine();
					}
					if(tmp.toLowerCase().equals("n")){
					}
					else if(tmp.toLowerCase().equals("y")){
						V=Utility.getClosenessConstraintsFromTables(tables);
						System.out.println("Done!");
					}
				}
				
				List<Dependency> D=new ArrayList<Dependency>();
				try {
					String filename=Paths.get(".").toAbsolutePath().normalize().toString() + "/fragmentations/"+dbname+"/dependency.txt";
					D=Utility.readDependencies(filename, tables);
				} catch (FileNotFoundException e) {
					System.out.println("No dependencies found");
				}

				
				System.out.println("Use suggested owner capacity?[y|n]");
				System.out.print("fragDB> ");
				String tmp=sc.nextLine();
				while(!tmp.toLowerCase().equals("y") && !tmp.toLowerCase().equals("n")){
					if(tmp.toLowerCase().replaceAll("\\s", "").equals(new String("exit;").toLowerCase())){
						System.exit(0);
					}
					else if(tmp.toLowerCase().replaceAll("\\s", "").equals(new String("exit").toLowerCase())){
						System.exit(0);
					}
					System.out.println("Use suggested owner capacity?[y|n]");
					System.out.print("fragDB> ");
					tmp=sc.nextLine();
				}
				if(tmp.toLowerCase().equals("n")){
				}
				else if(tmp.toLowerCase().equals("y")){
					double ownerCapacity=Utility.suggestOwnerCapacity(tables, C, D);
					Utility.getOwner(servers).setCapacity(ownerCapacity);
				}
				else if(query.toLowerCase().replaceAll("\\s", "").equals(new String("exit;").toLowerCase())){
					System.exit(0);
				}
				else if(query.toLowerCase().replaceAll("\\s", "").equals(new String("exit").toLowerCase())){
					System.exit(0);
				}
				
				double[] a= {1.0,1.0,1.0};
				System.out.println("Use suggested weights?[y|n]");
				System.out.print("fragDB> ");
				tmp=sc.nextLine();
				while(!tmp.toLowerCase().equals("y") && !tmp.toLowerCase().equals("n")){
					if(tmp.toLowerCase().replaceAll("\\s", "").equals(new String("exit;").toLowerCase())){
						System.exit(0);
					}
					else if(tmp.toLowerCase().replaceAll("\\s", "").equals(new String("exit").toLowerCase())){
						System.exit(0);
					}
					System.out.println("Use suggested weights?[y|n]");
					System.out.print("fragDB> ");
					tmp=sc.nextLine();
				}
				if(tmp.toLowerCase().equals("n")){
					boolean flag =true;
					while(flag){
					System.out.println("How would you like to choose the weights?[a1 a2 a3]");
					System.out.print("fragDB> ");
					tmp=sc.nextLine();
						try{
							String[] as=tmp.split(" ");
							a[0]=Double.parseDouble(as[0]);
							a[1]=Double.parseDouble(as[1]);
							a[2]=Double.parseDouble(as[2]);
							if(a[1]<0 | a[2]<0 | a[0]<0){
								System.out.println("Weights must be positive!");
								continue;
							}
							flag=false;
						}catch(Exception e){
							continue;
						}
					}
					
				}
				else if(tmp.toLowerCase().equals("y")){
					a=Utility.suggestWeights(servers.size(), V, CC);
					
				}
				System.out.println("Solving ILP!");
				frag=ModelSolver.solve(tables,servers,C,V,CC,D,a,true);
				if(frag==null){
					System.out.println("No solution exists!");
					continue;
				}
				System.out.println("Setting up the distributed database!");
				int threads=Runtime.getRuntime().availableProcessors();
				try {
					frag.create(dbname, "_frag",1000000,threads,null);
				} catch (ClassNotFoundException | SQLException e) {
					System.out.println("Error occured!");
					System.out.println(e.getMessage());
					System.exit(0);
				}
				
			}
			else{
				boolean view=false;
				String method="fdw";
				String query="";
				if(!methodSet){
					System.out.println("Should queries be rewritten[1] or based on views[2]?");
					System.out.print("fragDB> ");
					query="";
					String tmp=sc.nextLine();
					view=false;
					method="fdw";
					while(!tmp.toLowerCase().equals("1") && !tmp.toLowerCase().equals("2")){
						if(tmp.toLowerCase().replaceAll("\\s", "").equals(new String("exit;").toLowerCase())){
							System.exit(0);
						}
						else if(tmp.toLowerCase().replaceAll("\\s", "").equals(new String("exit").toLowerCase())){
							System.exit(0);
						}
						System.out.println("Should queries be rewritten[1] or be based on views[2]?");
						System.out.print("fragDB> ");
						tmp=sc.nextLine();
					}
					if(tmp.toLowerCase().equals("2")){
						method="view";
						view=true;
					}
					else if(tmp.toLowerCase().equals("1")){
						
					}
					methodSet=true;
				}
				
				sc.useDelimiter(";");
				query="";
				System.out.print("fragDB> ");
				while(!query.endsWith(";")) {
					query+=sc.nextLine().trim();
					if(query.toLowerCase().replaceAll("\\s", "").equals(new String("EXIT;").toLowerCase())){
						System.exit(0);
					}
					else if(query.toLowerCase().replaceAll("\\s", "").equals(new String("EXIT").toLowerCase())){
						System.exit(0);
					}
				}
				query=query.replaceAll("\\s+", " ");
				List<String> queries = Arrays.asList(query.split(";"));
				for(String query2:queries) {
					query2=query2.trim();
					if(query.equals("")) {
						continue;
					}
					if(query2.toLowerCase().replaceAll("\\s", "").equals(new String("EXIT;").toLowerCase())){
						System.exit(0);
					}
					boolean cont=false;
					try{
						CCJSqlParserManager sqlparser = new CCJSqlParserManager();
						Statement st=sqlparser.parse(new StringReader(query2));
						
						if(st instanceof Insert){
							frag.insert((Insert)st);
							cont=true;
						}
						if(!cont && st instanceof Delete){
							frag.delete((Delete) st,view);
							cont=true;
						}
						if(!cont && st instanceof Update){
							frag.update((Update) st,view);
							cont=true;
						}
						
					}catch(Exception e){
	
					}
					if(cont){
						continue;
					}
					
					try {
						if(query.toLowerCase().contains("create view")) {
							BenchmarkResultSet rs=frag.executeUpdate(query2,method);
							rs.print();
						}
						else if(query.toLowerCase().contains("drop view")) {
							BenchmarkResultSet rs=frag.executeUpdate(query2,method);
							rs.print();
						}
						else {
							BenchmarkResultSet rs=frag.executeQuery(query2,method,true);
							rs.print();
						}
					} catch (Exception e) {
						System.out.println("Could not execute query!");
					}
				}
			}
		}
	}
}
