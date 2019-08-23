package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import metadata.Server;

//class to load the configuration
public class Configuration {
	List<Server> servers;
	public Configuration(String path) throws FileNotFoundException {
		servers=new ArrayList<Server>();
		File file=new File(path);
		Scanner sc=new Scanner(file);
		sc.useDelimiter(";");
		while(sc.hasNext()){
			String config=sc.next();
			config=config.replaceAll("\\s+", "");
			
		}
		sc.close();
	}
	
}
