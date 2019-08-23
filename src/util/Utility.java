package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

import constraints.Constraint;
import constraints.Dependency;
import fragmentation.DistributedDatabaseClient;
import metadata.Attribute;
import metadata.Domain;
import metadata.Server;
import metadata.Table;

//class that provides basic uncategorized functionalities
public class Utility {
	private static Random random=new Random(1404);
	public static void print(ResultSet rs){
		if(rs==null){
			return;
		}
		List<Integer> lengths=new ArrayList<Integer>();
		//int length=15;
		List<String> formats=new ArrayList<String>();
		List<String> mformats=new ArrayList<String>();
//		String format="| %-"+length+"s";
//		String mformat="| %"+length+"s";
		lengths.add(0);
		formats.add("");
		mformats.add("");
		int totalLength=0;
		try {
			for(int i=1;i<=rs.getMetaData().getColumnCount();i++){
				String cn=rs.getMetaData().getColumnLabel(i);
				int length=rs.getMetaData().getColumnDisplaySize(i);
				if(length>15) {
					length=15;
				}
				totalLength+=length+1;
				lengths.add(length);
				formats.add("| %-"+length+"s");
				mformats.add("| %-"+length+"s");
			}

			String separator="+";
			for(int i=1;i<lengths.size();i++) {
				//separator+="+";
				for(int j=0; j<=lengths.get(i);j++) {
					separator+="-";
				}
				separator+="+";
			}
			System.out.println(separator);
			for(int i=1;i<=rs.getMetaData().getColumnCount();i++){
				String cn=rs.getMetaData().getColumnLabel(i);
				int length=lengths.get(i);
				if(length>15) {
					length=15;
				}
				if(cn.length()>length){
					cn=cn.substring(0,length);
				}
				System.out.printf(formats.get(i),cn.toUpperCase());
			}
			System.out.print("|");
			System.out.println();
			System.out.println(separator);
			
			while(rs.next()){
				for(int i=1;i<=rs.getMetaData().getColumnCount();i++){
					int length=lengths.get(i);
					String value="null";
					if(rs.getObject(i)!=null){
						value=rs.getObject(i).toString();
					}
					if(value.equals("null")){
						value=Long.toString(rs.getLong(i));
					}
					if(value.length()>length){
						value=value.substring(0,length);
					}
					System.out.printf(mformats.get(i),value);
				}
				System.out.print("|");
				System.out.println();
			}
			System.out.println(separator);
			System.out.println();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static List<Table> getTables(String host,int port,String username,String password,String dbname) throws SQLException, ClassNotFoundException{
		Server server=new Server("util", host, port, username, password, 0, 0);
		server.open(host, port, dbname, username, "postgres");
		DatabaseMetaData meta=server.getMetaData();
		List<String> tablenames=new ArrayList<String>();
		ResultSet rs=meta.getTables(null, null, "%", new String[] { "TABLE" });
		while(rs.next()){
			tablenames.add(rs.getString(3));
		}

		HashMap<String,String> domains=new HashMap<String,String>();
		rs=server.executeQuery("SELECT t.typname as domain_name, pg_catalog.format_type(t.typbasetype, t.typtypmod) as data_type FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace LEFT JOIN pg_catalog.pg_constraint c ON t.oid = c.contypid WHERE t.typtype = 'd' ");
	
		while(rs.next()){
			domains.put(rs.getString(1), rs.getString(2));
			
		}
		List<Attribute> allAttrs=new ArrayList<Attribute>();
		List<Attribute> allNonTidAttrs=new ArrayList<Attribute>();
		List<Table> tables=new ArrayList<Table>();
		for(String name:tablenames){
			Table table=new Table(name);
			tables.add(table);
			meta=server.getMetaData();
			rs=meta.getPrimaryKeys(null, null, name);
			List<String> pk=new ArrayList<String>();
			while(rs.next()){
				pk.add(rs.getString("COLUMN_NAME"));
			}
			rs=meta.getColumns(null, null, name, null);
			while(rs.next()){
				String colname=rs.getString(4).toLowerCase();
				Domain domain=null;
				if(domains.containsKey(rs.getString(6))){
					domain=new Domain(rs.getString(6),domains.get(rs.getString(6)));
				}
				else{
					domain=new Domain(rs.getString(6),rs.getString(6));
				}
				double size=1.25e-6;
				String sizequery="select sum(sizes)*9.53674e-7 from (Select pg_column_size("+colname+"::"+domain.getSQLType()+") as sizes from "+name+ ") as columnsize;";
				ResultSet sizeRs=server.executeQuery(sizequery);
				sizeRs.next();
				size=sizeRs.getDouble(1);
				if(size<=0){
					size=1.25e-6;
				}
				if(pk.contains(colname)){
					Attribute tmpa=new Attribute(colname,domain,size,name,true);
					table.add(tmpa);
					allAttrs.add(tmpa);
					
				}
				else{
					Attribute tmpa=new Attribute(colname,domain,size,name,false);
					table.add(tmpa);
					allAttrs.add(tmpa);
					allNonTidAttrs.add(tmpa);
				}
			}
		}
		server.close();
		return tables;
		
	}
	
	public static List<Server> readConfig(String filename) throws FileNotFoundException, ClassNotFoundException, SQLException{
		List<Server> servers=new ArrayList<Server>();
		File file=new File(filename);
		Scanner sc=new Scanner(file);
		sc.nextLine();
		sc.useDelimiter(";");
		boolean owner=false;
		while(sc.hasNext()){
			String config=sc.next();
			config=config.replaceAll("\\s+", "");
			if(config.equals("")){
				continue;
			}
			if(config.charAt(0)=='#'){
				continue;
			}
			String[] tmp=config.split(",");
			
			String name=tmp[0];
			String address=tmp[1];
			int port=5432;
			try{
				port=Integer.parseInt(tmp[2]);
			}catch(Exception e){
					
			}
			String username=tmp[3];
			String password=tmp[4];
			double capacity=Double.MAX_VALUE;
			if(tmp[2].toLowerCase().equals(new String("INF").toLowerCase())){
				capacity=Double.MAX_VALUE;
			}
			try{
				capacity=Double.parseDouble(tmp[2]);
			}
			catch (Exception e){
				
			}
			Server server=new Server(name,address,port,username,password,capacity,1);
			if(tmp.length==7 && (!owner) && tmp[6].toLowerCase().equals(new String("owner"))){
				server.setOwner(true);
				owner=true;
			}
			servers.add(server);	
		}
		return servers;
		
	}
	
	public static List<Constraint> getClosenessConstraintsFromTables(List<Table> tables){
		List<Constraint> constraints=new ArrayList<Constraint>();
		for(Table table:tables){
			constraints.add(new Constraint(table.getAttributes(),1.0));
		}
		return constraints;
	}
	
	public static List<Constraint> readConstraints(String filename,List<Table> tables) throws FileNotFoundException{
		List<Constraint> constraints=new ArrayList<Constraint>();
		Set<Attribute> specifiedAttributes=new HashSet<Attribute>();
		for(Table table : tables){
			specifiedAttributes.addAll(table.getAttributes());
		}
		
		File file = new File(filename);
		Scanner sc=new Scanner(file);
		sc.useDelimiter(";");
		while(sc.hasNext()){
			List<Attribute> attributes=new ArrayList<Attribute>();
			int priority=1;
			String constraint=sc.next();
			if(constraint.contains("#")){
				continue;
			}
			Scanner sc2=new Scanner(constraint);
			sc2.useDelimiter(",");
			while(sc2.hasNext()){
				String attributeString=sc2.next();
				attributeString=attributeString.replaceAll("\\s+","");
				if(!attributeString.contains(".")){
					try{
						priority=Integer.parseInt(attributeString);
					}catch(Exception e){
					}
					continue;
				}
				String table=null;
				String attributename=null;
				String[] tmp=attributeString.split("\\.");
				table=tmp[0];
				attributename=tmp[1];
				Attribute attribute=new Attribute(attributename,table);
				if(!specifiedAttributes.contains(attribute)){
					System.out.println("Attribute "+attribute+ " doesn't exist");
					continue;
				}
				for(Iterator<Attribute> it=specifiedAttributes.iterator();it.hasNext();){
					Attribute attribute2=it.next();
					if(attribute2.equals(attribute)){
						attribute=attribute2;
					}
				}
				attributes.add(attribute);
			}
			sc2.close();
			if(attributes.size()>0){
				Constraint c=new Constraint(attributes,priority);
				constraints.add(c);
			}
		}
		sc.close();
		return constraints;
	}
	
	public static List<Dependency> readDependencies(String filename,List<Table> tables) throws FileNotFoundException{
		List<Dependency> dependencies=new ArrayList<Dependency>();
		HashSet<Attribute> specifiedAttributes=new HashSet<Attribute>();
		for(Table table : tables){
			specifiedAttributes.addAll(table.getAttributes());
		}
		
		File file = new File(filename);
		Scanner sc=new Scanner(file);
		sc.useDelimiter(";");
		while(sc.hasNext()){
			List<Attribute> premise=new ArrayList<Attribute>();
			List<Attribute> consequence=new ArrayList<Attribute>();
			List<Attribute> premise2=new ArrayList<Attribute>();
			List<Attribute> consequence2=new ArrayList<Attribute>();
			boolean bidirectional=false;
			String delimiter=">";
			String dependency=sc.next();
			if(dependency.contains("#")){
				continue;
			}
			if(dependency.contains(">")){
				delimiter=">";
			}
			else if(dependency.contains("~")){
				bidirectional=true;
				delimiter="\\~";
			}
			else{
				continue;
			}
			String[] tmp1=dependency.split(delimiter);
			for(int i=0;i<2;i++){
				String attributesString=tmp1[i];
				Scanner sc2=new Scanner(attributesString);
				sc2.useDelimiter(",");
				while(sc2.hasNext()){
					String attributeString=sc2.next();
					attributeString=attributeString.replaceAll("\\s+","");
					String table=null;
					String attributename=null;
					String[] tmp2=attributeString.split("\\.");
					table=tmp2[0];
					attributename=tmp2[1];
					Attribute attribute=new Attribute(attributename,table);
					if(!specifiedAttributes.contains(attribute)){
						System.out.println("Attribute "+attribute+ " doesn't exist");
						continue;
					}
					for(Iterator<Attribute> it=specifiedAttributes.iterator();it.hasNext();){
						Attribute attribute2=it.next();
						if(attribute2.equals(attribute)){
							attribute=attribute2;
						}
					}
					if(i==0){
						premise.add(attribute);
						if(bidirectional){
							consequence2.add(attribute);
						}
					}
					else{
						consequence.add(attribute);
						if(bidirectional){
							premise2.add(attribute);
						}
					}
				}
				sc2.close();
			}
			if(premise.size()>0 && consequence.size()>0){
				dependencies.add(new Dependency(premise,consequence));
				if(bidirectional){
					dependencies.add(new Dependency(premise2,consequence2));
				}
			}
		}
		sc.close();
		return dependencies;
	}
	public static List<Constraint> randomConstraints(List<Table> tables, int[] cardinalities,boolean conf){
		List<Constraint> constraints=new ArrayList<Constraint>();
		List<Attribute> specifiedAttributes=new ArrayList<Attribute>();
		for(Table table : tables){
			specifiedAttributes.addAll(table.getAttributes());
			specifiedAttributes.removeAll(table.getTid());
		}
		for(int i=0;i<cardinalities.length;i++){
			for(int k=0;k<cardinalities[i];k++){
				int card=i+2;
				Constraint c=new Constraint(1,String.valueOf(i+k));
				for(int j=0;j<card;j++){
					Attribute attr=specifiedAttributes.get(random.nextInt(specifiedAttributes.size()));
					while(c.contains(attr)){
						attr=specifiedAttributes.get(random.nextInt(specifiedAttributes.size()));
					}
					c.add(attr);
				}

				if(conf && wellDefined(tables,c,constraints,conf)){
					constraints.add(c);
				}
				else if(conf){
					k--;
				}
				else{
					constraints.add(c);
				}
			}
		}
		
		return constraints;
	}
	
	public static List<Dependency> randomDependencies(List<Table> tables, double fraction,int p_minCard,int p_maxCard,int c_minCard,int c_maxCard){
		List<Dependency> dependencies=new ArrayList<Dependency>();
		List<Attribute> specifiedAttributes=new ArrayList<Attribute>();
		int n=0;
		for(Table table : tables){
			specifiedAttributes.addAll(table.getAttributes());
		}
		for(Table table : tables){
			specifiedAttributes.removeAll(table.getTid());
		}
		n=(int)(Math.ceil(((double)specifiedAttributes.size())*fraction));
			for(int k=0;k<n;k++){
				List<Attribute> p=new ArrayList<Attribute>();
				List<Attribute> c=new ArrayList<Attribute>();
				int pcard=0;
				while(pcard==0 || pcard<p_minCard){
					pcard=random.nextInt(p_maxCard);
				}
				int ccard=0;
				while(ccard==0|| pcard<c_minCard){
					ccard=random.nextInt(c_maxCard);
				}
				for(int j=0;j<pcard;j++){
					Attribute attr=specifiedAttributes.get(random.nextInt(specifiedAttributes.size()));
					while(p.contains(attr)){
						attr=specifiedAttributes.get(random.nextInt(specifiedAttributes.size()));
					}
					p.add(attr);
				}
				for(int j=0;j<ccard;j++){
					Attribute attr=specifiedAttributes.get(random.nextInt(specifiedAttributes.size()));
					while(c.contains(attr)){
						attr=specifiedAttributes.get(random.nextInt(specifiedAttributes.size()));
					}
					c.add(attr);
				}

				dependencies.add(new Dependency(p,c));
			}
		return dependencies;
	}
	
	public static Set<Attribute> inflate(List<Attribute> A,List<Dependency> D){
		Set<Attribute> res=new HashSet<Attribute>(A);
		boolean flag=true;
		while(flag){
			flag=false;
			for(Dependency d:D){
				if(res.containsAll(d.getPremise())){
					if(!res.containsAll(d.getConsequence())){
						res.addAll(d.getConsequence());
						flag=true;
					}
				}
			}
			
		}
		return res;
	}
	
	public static List<Constraint> randomConstraints(List<Table> tables, double fraction, int size,boolean conf){
		List<Constraint> constraints=new ArrayList<Constraint>();
		List<Attribute> specifiedAttributes=new ArrayList<Attribute>();
		int n=0;
		for(Table table : tables){
			specifiedAttributes.addAll(table.getAttributes());
		}
		fraction=fraction/4.0;
		n=(int)((double)specifiedAttributes.size()*fraction);
		for(Table table : tables){
			specifiedAttributes.removeAll(table.getTid());
		}
		n=(int)Math.ceil(specifiedAttributes.size()*fraction);
		for(int i=0;i<size;i++){
			for(int k=0;k<n;k++){
				int card=i+2;
				Constraint c=new Constraint(1,String.valueOf(i+k));
				for(int j=0;j<card;j++){
					Attribute attr=specifiedAttributes.get(random.nextInt(specifiedAttributes.size()));
					while(c.contains(attr)){
						attr=specifiedAttributes.get(random.nextInt(specifiedAttributes.size()));
					}
					c.add(attr);
				}

				if(conf && wellDefined(tables,c,constraints,conf)){
					constraints.add(c);
				}
				else if(conf){
					k--;
				}
				else{
					constraints.add(c);
				}
			}
		}
		
		return constraints;
	}
	
	public static double suggestOwnerCapacity(List<Table> tables,List<Constraint> C,List<Dependency> D){
		double capacity=0.0;
		Set<Attribute> ownerAttributes=new HashSet<Attribute>();
		for(Constraint c:C){
			if(c.getAttributes().size()==1){
				ownerAttributes.add(c.getAttributes().get(0));
				String tablename=c.getAttributes().get(0).getTable();
				for(Table table:tables){
					if(table.getName().toLowerCase().equals(tablename.toLowerCase())){
						ownerAttributes.addAll(table.getTid());
					}
				}
			}
		}
		for(Dependency d:D){
			if(d.getPremise().size()==1){
				Set<Attribute> attributes=new HashSet<Attribute>();
				attributes.addAll(d.getPremise());
				attributes.addAll(d.getConsequence());
				while(true){
					int size=attributes.size();
					for(Dependency d2:D){
						if(attributes.containsAll(d2.getPremise())){
							attributes.addAll(d2.getConsequence());
						}
					}
					if(attributes.size()==size){
						break;
					}
				}
				for(Constraint c:C){
					boolean flag=false;
					if(attributes.containsAll(c.getAttributes())){
						flag=true;
					}
					if(flag){
						ownerAttributes.add(d.getPremise().get(0));
						String tablename=d.getPremise().get(0).getTable();
						for(Table table:tables){
							if(table.getName().toLowerCase().equals(tablename.toLowerCase())){
								ownerAttributes.addAll(table.getTid());
							}
						}
					}
					
				}
			}
		}
		Set<Attribute> ownerAttributes2=new HashSet<Attribute>(ownerAttributes);
		for(Table table:tables) {
			for(Attribute attr: ownerAttributes2) {
				if(attr.getTable().toLowerCase().equals(table.getName())) {
					ownerAttributes.addAll(table.getTid());
				}
			}
		}
		for(Attribute attr:ownerAttributes){
			capacity+=attr.getWeight();
		}
		
		return capacity;
	}
	
	public static double[] suggestWeights(int k,List<Constraint> V,List<Constraint> CC){
		if(CC==null){
			CC=new ArrayList<Constraint>();
		}
		if(V==null){
			V=new ArrayList<Constraint>();
		}
		double[] weights={1,1,1};
		double cardCC=0;
		double cardV=0;
		for(Constraint cc:CC){
			cardCC+=cc.getWeight();
		}
		for(Constraint v:V){
			cardV+=v.getWeight();
		}
		if(cardCC==0 && cardV==0){
			return weights;
		}
		else if(cardV>0 && cardCC==0){
			weights[0]=(2*cardV);
			weights[1]=0.9;
		}
		else if(cardV==0 && cardCC>0){
			weights[0]=(2*k*1*cardCC);
			weights[2]=0.87;
		
		}
		else{
			weights[0]=(2*k*cardV*cardCC);
			weights[1]=0.9*(k*cardCC);
			weights[2]=0.87;	
		}
		return weights;
	}
	
	public static boolean wellDefined(List<Table> tables,Constraint c1,List<Constraint> C,boolean conf){
		for(int j=0;j<C.size();j++){
			Constraint c2=C.get(j);
			if(c1.getAttributes().containsAll(c2.getAttributes())){
				return false;
			}
			if(c2.getAttributes().containsAll(c1.getAttributes())){
				return false;
			}
		}
		if(conf){
			for(Table table : tables){
				if(table.getAttributes().containsAll(c1.getAttributes())){
					for(Attribute attr:c1.getAttributes()){
						if(table.getTid().contains(attr)){
							return false;
						}
					}
				}
			}
		}
		return true;
	}
	
	public static List<Constraint> wellDefined(List<Table> tables,List<Constraint> C,boolean conf){
		List<Constraint> wellDefined=new ArrayList<Constraint>();
		for(int i=0;i<C.size();i++){
			Constraint c1=C.get(i);
			boolean flag=true;
			for(int j=0;j<wellDefined.size();j++){
				Constraint c2=wellDefined.get(j);
				if(c1.getAttributes().containsAll(c2.getAttributes())){
					flag=false;
				}
				else if(c2.getAttributes().containsAll(c1.getAttributes())){
					wellDefined.remove(c2);
					
				}
				
			}
			if(flag){
				wellDefined.add(c1);
			}
		}
		List<Constraint> wellDefined2=new ArrayList<Constraint>(wellDefined);
		for(Constraint c1:wellDefined2){
			for(Table table : tables){
				if(table.getAttributes().containsAll(c1.getAttributes())){
					for(Attribute attr:c1.getAttributes()){
						if(table.getTid().contains(attr)){
							wellDefined.remove(c1);
						}
					}
				}
			}
		}
		return wellDefined;
	}
	
	
	public static boolean checkFragmentation(DistributedDatabaseClient frag,List<Constraint> C,List<Dependency>D){
		for(Server server: frag.getServerToAttribute().keySet()){
			if(server.isOwner()){
				continue;
			}
			for(Constraint c:C){
				if(frag.getServerToAttribute().get(server).containsAll(c.getAttributes())){
					return false;
				}
			}
		}
		for(Server server: frag.getServerToAttribute().keySet()){
			if(server.isOwner()){
				continue;
			}
			for(Dependency d:D){
				if(frag.getServerToAttribute().get(server).containsAll(d.getPremise())){
					if(!frag.getServerToAttribute().get(server).contains(d.getConsequence())){
						return false;
					}
				}
			}
		}
		
		return true;
	}
	
	public static Server getOwner(List<Server> servers){
		Server owner=null;
		for(Server s:servers){
			if(s.isOwner()){
				return s;
			}
		}
		return owner;
	}
	
	public static void seed(int seed){
		random=new Random(seed);
	}
}
