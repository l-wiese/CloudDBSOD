package constraints;

import java.util.ArrayList;
import java.util.List;

import metadata.Attribute;

//Class to implement confidentiality dependencies
public class Dependency {
	private List<Attribute> premise=new ArrayList<Attribute>();
	private List<Attribute> consequence=new ArrayList<Attribute>();

	public Dependency(List<Attribute> premise, List<Attribute> consequence) {
		super();
		this.premise = premise;
		this.consequence = consequence;
	}

	public List<Attribute> getPremise() {
		return premise;
	}
	public void setPremise(List<Attribute> premise) {
		this.premise = premise;
	}
	public List<Attribute> getConsequence() {
		return consequence;
	}
	public void setConsequence(List<Attribute> consequence) {
		this.consequence = consequence;
	}
	public String toString(){
		return  premise.toString()+" ~> "+consequence.toString();
	}
	
}
