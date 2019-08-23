package metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

//class storing metadata of a table
public class Table implements Serializable{
	private static final long serialVersionUID = 3153652535158056718L;
	String name;
	List<Attribute> attributes=new ArrayList<Attribute>();
	List<Attribute> tid=new ArrayList<Attribute>();
	double weight=1;
	
	
	public Table(String name) {
		this.name=name.toLowerCase();
	}
	
	public Table(String name,List<Attribute> attributes) {
		super();
		this.name=name.toLowerCase();
		this.attributes = attributes;
		for(Attribute attr : attributes){
			if(attr.isTid()){
				tid.add(attr);
			}
		}
	}
	
	public List<Attribute> getAttributes(){
		return attributes;
	}
	
	public void add(Attribute attr){
		attributes.add(attr);
		if(attr.isTid()){
			tid.add(attr);
		}
	}
	public String toString(){
		return attributes.toString();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Attribute> getTid() {
		return tid;
	}

	public void setTid(List<Attribute> tid) {
		this.tid = tid;
	}

	public void setAttributes(List<Attribute> attributes) {
		this.attributes = attributes;
	}
	
	@Override
	public boolean equals(Object obj){
		if(!(obj instanceof Table)){
			return false;
		}
		if(!((Table)obj).getName().equals(name)){
			return false;
		}
		return true;
	}
}
