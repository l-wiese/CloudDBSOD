package constraints;
import java.util.ArrayList;
import java.util.List;

import metadata.Attribute;

//Class to implement general constraints
public class Constraint {
	List<Attribute> attributes=new ArrayList<Attribute>();
	double weight=1;
	String name="";
	
	public void add(Attribute attr){
		attributes.add(attr);
	}
	
	public boolean contains(Attribute attr){
		return attributes.contains(attr);
	}

	public List<Attribute> getAttributes() {
		return attributes;
	}
	
	public int size(){
		return attributes.size();
	}
	public Attribute get(int index){
		return attributes.get(index);
	}

	public void setAttributes(List<Attribute> attributes) {
		this.attributes = attributes;
	}

	public Constraint(List<Attribute> attributes, double weight) {
		super();
		this.attributes = attributes;
		this.weight = weight;
	}

	public Constraint(double weight) {
		super();
		this.weight = weight;
	}
	public Constraint(double weight,String name) {
		super();
		this.weight = weight;
		this.name=name;
	}

	public Constraint() {
		super();
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}
	public String toString(){
		return "{"+weight+": "+attributes.toString().substring(1,attributes.toString().length()-1)+"}";
	}
	@Override
	public boolean equals(Object o){
		if(!this.getClass().isAssignableFrom(o.getClass())){
			return false;
		}
		try{
			Constraint c2=(Constraint) o;
			if(c2.getAttributes().containsAll(this.getAttributes()) && this.getAttributes().containsAll(c2.getAttributes())){
				return true;
			}
		}catch(Exception e){
			return false;
		}
		return false;
	}
	
}
