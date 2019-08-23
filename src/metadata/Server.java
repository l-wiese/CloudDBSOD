package metadata;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

//A class for the servers
public class Server implements Serializable{
	private static final long serialVersionUID = 6270053747699733786L;
	transient Connection connection=null;
	String host;
	int port;
	String username;
	String password;
	String dbname="";
	String name;
	String type="psql";
	double capacity;
	double importance=1;

	boolean owner=false;
	List<Table> originalTables=new ArrayList<Table>();
	
	public boolean isOwner() {
		return owner;
	}

	public void setOwner(boolean owner) {
		this.owner = owner;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getOriginalTableNames() {
		List<String> tablenames= new ArrayList<String>();
		for(Table t:originalTables){
			tablenames.add(t.getName());
		}
		return tablenames;
	}

	public void setOriginalTables(List<Table> originalTables) {
		this.originalTables = originalTables;
	}
	
	public Server(Server s){
		this.name=s.name;
		this.host = s.host;
		this.port = s.port;
		this.username=s.username;
		this.password=s.password;
		this.capacity=s.capacity;
		this.dbname=s.dbname;
		this.importance=s.importance;
		this.type=s.type;
		this.owner=s.isOwner();
		this.originalTables=new ArrayList(s.getOriginalTableNames());
	}

	public Server(String name,String host, int port,String username,String password,double capacity,double importance) throws ClassNotFoundException, SQLException {
		super();
		this.name=name;
		this.host = host;
		this.port = port;
		this.username=username;
		this.password=password;
		this.capacity=capacity;
		this.importance=importance;
	}
	
	public Server(String name,String host, int port,String username,String password,String dbname,double capacity,double importance) throws ClassNotFoundException, SQLException {
		super();
		this.name=name;
		this.host = host;
		this.port = port;
		this.username=username;
		this.password=password;
		this.dbname=dbname;
		this.capacity=capacity;
		this.importance=importance;
	}

	public Connection getConnection() throws ClassNotFoundException, SQLException {
		Connection connection=null;
		if(this.type.equals("psql")){
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://"+host+":"+port+"/"+dbname,username, password);
		}
		else if(this.type.equals("mariadb")){
			Class.forName("org.mariadb.jdbc.Driver");
			connection = DriverManager.getConnection("jdbc:mariadb://"+host+":"+port+"/"+dbname+"?allowMultiQueries=true",username, password);
		}
		
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDbname() {
		return dbname;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public double getCapacity() {
		return capacity;
	}

	public void setCapacity(double capacity) {
		this.capacity = capacity;
	}

	public double getImportance() {
		return importance;
	}

	public void setImportance(double importance) {
		this.importance = importance;
	}
	
	
	public ResultSet executeQuery(String query) throws ClassNotFoundException, SQLException{
		ResultSet res=null;
		if(connection!=null && !connection.isClosed()) {
			connection.close();
		}
			if(connection==null || connection.isClosed()){
				open();
			}
			Statement stmt=connection.createStatement();
			stmt.setFetchSize(200);
			res=stmt.executeQuery(query);
		return res;
	}
	
	public ResultSet executeQuery(String query,boolean nested_loop) throws ClassNotFoundException{
		ResultSet res=null;
		try {
			if(connection.isClosed()){
				open();
			}
			Statement stmt=connection.createStatement();
			try{
			if(!nested_loop){
				stmt.executeUpdate("Set enable_nestloop to false;");
			}
			}catch(Exception e){
				e.printStackTrace();
			}
			stmt.setFetchSize(200);
			res=stmt.executeQuery(query);
			
		} catch (SQLException e) {
			System.out.println("Could not execute query: "+query);
		}
		return res;
	}
	
	synchronized public void executeUpdate(String sql) throws ClassNotFoundException, SQLException{
			if(connection==null||connection.isClosed()){
				open();
			}
			Statement stmt=connection.createStatement();
			stmt.executeUpdate(sql);
	}
	
	public Statement createStatement() throws ClassNotFoundException{
		try {
			if(connection==null||connection.isClosed()){
				open();
			}
			return connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void close(){
		try {
			connection.close();
		} catch (SQLException e) {

		}
	}
	public void open(String host, int port,String dbname,String username,String password) throws ClassNotFoundException, SQLException{
		if(!(connection==null) && !connection.isClosed()){
				connection.close();	
		}
		this.host = host;
		this.port = port;
		this.username=username;
		this.password=password;
		this.dbname=dbname;
		open();
	}
	
	
	public void open(String dbname) throws ClassNotFoundException, SQLException{
		try {
			if(!(connection==null) && !connection.isClosed()){
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.dbname=dbname;
		open();
	}
	
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void open(String host, int port,String username,String password) throws SQLException, ClassNotFoundException{
		if(!(connection==null) && !connection.isClosed()){
				connection.close();	
		}
		this.host = host;
		this.port = port;
		this.username=username;
		this.password=password;
		this.dbname="";
		open();
	}
	
	public void open() throws ClassNotFoundException, SQLException{
		if(connection!=null) {
			try {
				connection.close();
			}
			catch(Exception e) {
				
			}
		}
			if(this.type.equals("psql")){
				Class.forName("org.postgresql.Driver");
				connection = DriverManager.getConnection("jdbc:postgresql://"+host+":"+port+"/"+dbname,username, password);
			}
	}
	
	public DatabaseMetaData getMetaData() throws SQLException{
		return connection.getMetaData();
	}
	
	public String toString(){
		return this.name+"("+this.host+")";
	}
	
	public void createDBlink() throws SQLException, ClassNotFoundException{
		Connection connection=this.getConnection();
		Statement statement=connection.createStatement();
		statement.execute("CREATE EXTENSION dblink;");
		statement.close();
		connection.close();
		
	}

	public boolean equals(Object other){
		if(other instanceof Server){
			if(!this.host.equals(((Server) other).getHost())){
				return false;
			}
			if(!this.name.equals(((Server) other).getName())){
				return false;
			}
			return true;
		}
		else{
			return false;
		}
	}
	
}
