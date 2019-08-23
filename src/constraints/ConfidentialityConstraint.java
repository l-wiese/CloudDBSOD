package constraints;
import java.util.ArrayList;
import java.util.List;

import metadata.Attribute;

//Class to implement confidentiality constraints
public class ConfidentialityConstraint extends Constraint {
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

	public ConfidentialityConstraint(List<Attribute> attributes, double weight) {
		super();
		this.attributes = attributes;
		this.weight = weight;
	}

	public ConfidentialityConstraint(double weight) {
		super();
		this.weight = weight;
	}
	public ConfidentialityConstraint(double weight,String name) {
		super();
		this.weight = weight;
		this.name=name;
	}

	public ConfidentialityConstraint() {
		super();
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}
	
}