package util;

import java.io.Serializable;

//simple class implementing a status bar
public class StatusBar implements Serializable{
	private static final long serialVersionUID = 9139002532752404611L;
	int length=0;
	int status=0;
	boolean init=false;
	

	public StatusBar(int length) {
		super();
		this.length = length;
	};
	public void print(){
		if(status<length){
			String progress="[";
			int tmp=status;
			for(int i=0;i<=status;i++){
				progress+="=";
			}
			for(int i=status+1;i<length;i++){
				progress+=" ";
			}
			System.out.print("\r"+progress+"]");
			if(status==length-1) {
				System.out.println();
			}
		}
		status++;
	}
	public void reset(){
		status=0;
	}
	public void reset(int length){
		this.length=length;
		status=0;
	}

}
