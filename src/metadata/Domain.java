package metadata;

import java.io.Serializable;

//class implementing a domain of an attribute
public class Domain implements Serializable{
	private static final long serialVersionUID = 3897544979538451904L;
	String name="";
	String sqlType="";
	public Domain(String name,String domain){
		this.name=name;
		this.sqlType=domain;
	}
	public Domain(String domain){
		this.name=domain;
		this.sqlType=domain;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSQLType() {
		return sqlType;
	}
	public void setSQLType(String domain) {
		this.sqlType = domain;
	}
	
	public String toString(){
		return "["+name+","+sqlType+"]";
	}
}
