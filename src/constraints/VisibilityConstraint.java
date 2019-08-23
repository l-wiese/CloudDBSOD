package constraints;
import java.util.ArrayList;
import java.util.List;

import metadata.Attribute;

//Class to implement visibility constraints
public class VisibilityConstraint extends Constraint{
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

	public void setAttributes(List<Attribute> attributes) {
		this.attributes = attributes;
	}
	public int size(){
		return attributes.size();
	}
	
	public Attribute get(int index){
		return attributes.get(index);
	}
	
	public VisibilityConstraint(List<Attribute> attributes, double weight,String name) {
		this.attributes = attributes;
		this.weight = weight;
		this.name=name;
	}

	public VisibilityConstraint(List<Attribute> attributes, double weight) {
		super();
		this.attributes = attributes;
		this.weight = weight;
	}

	public VisibilityConstraint(double weight) {
		super();
		this.weight = weight;
	}
	

	
	public VisibilityConstraint(double weight,String name) {
		super();
		this.weight = weight;
		this.name=name;
	}

	public VisibilityConstraint() {
		super();
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}
	
	@Override
	public boolean equals(Object obj){
		if(!(obj instanceof VisibilityConstraint)){
			return false;
		}
		VisibilityConstraint rhs=(VisibilityConstraint) obj;
		for(Attribute attr:attributes){
			if(!rhs.getAttributes().contains(attr)){
				return false;
			}
		}
		for(Attribute attr:rhs.getAttributes()){
			if(!attributes.contains(attr)){
				return false;
			}
		}
		
		return true;
	}
	public String toString(){
		return this.name;
	}
}
