package metadata;

import java.io.Serializable;
import java.sql.Types;

//class implementing an attribute
public class Attribute implements Serializable{
	private static final long serialVersionUID = -1632585032124682441L;
	protected Domain domain;
	protected String name;
	protected String table="default";
	protected double weight;
	boolean tid=false;
	int size=-1;
	
	
	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public Domain getDomain() {
		return domain;
	}
	
	public String getSQLType() {
		return domain.getSQLType();
	}

	public void setDomain(Domain domain) {
		this.domain = domain;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	

	public Attribute(String name,Domain domain,double weight ,String table) {
		super();
		this.domain = domain;
		this.name = name.toLowerCase();
		this.table = table.toLowerCase();
		this.weight=weight;
	}
	public Attribute(String name,Domain domain,double weight ,String table,boolean tid) {
		super();
		this.domain = domain;
		this.name = name.toLowerCase();
		this.table = table.toLowerCase();
		this.weight=weight;
		this.tid=tid;
	}
	
	public Attribute(String name ,String table) {
		super();
		this.name = name;
		this.table = table;
	}
	
	public boolean isTid() {
		return tid;
	}

	public void setTid(boolean tid) {
		this.tid = tid;
	}

	public String toString(){
		return table+"."+name;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}
	
	
	@Override
	public boolean equals(Object obj){
		if(!(obj instanceof Attribute)){
			return false;
		}
		Attribute rhs=(Attribute) obj;
		if(rhs.getDomain()!=(domain)){
		}
		if(!rhs.getName().toLowerCase().equals(name.toLowerCase())){
			return false;
		}
		if(!rhs.getTable().toLowerCase().equals(table.toLowerCase())){
			return false;
		}
		return true;
	}
	
	public int hashCode(){
		return table.toLowerCase().hashCode()*name.toLowerCase().hashCode();
	}
}
