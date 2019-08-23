package main;
import java.io.File;
import java.util.List;
import java.util.Scanner;

import benchmarks.TPCE;
import benchmarks.TPCH;
import fragmentation.DistributedDatabaseClient;
import metadata.Server;
import util.CommandLineUtility;
import util.Utility;
public class SeparationOfDuties {
	private static String options="-b tpch\n -b tpce\n -b tpce sf1 sf2 sfe timeout";
	//Starts the command line utility, the TPC-H or the TPC-E benchmark 
	public static void main(String[] args){
		try {
			String configFile="";
			if(System.getProperty("os.name").toLowerCase().indexOf("win")>=0){
				configFile=".\\config\\config.txt";
			}
			else if(System.getProperty("os.name").toLowerCase().indexOf("nux")>=0){
				configFile="./config/config.txt";
			}
			
			
			List<Server> servers=Utility.readConfig(configFile);
			if(args.length==0){
				CommandLineUtility cl=new CommandLineUtility(servers);
				while(cl.executeCommand());
			}
			else if(getArgument("-b",args)!=null){
				if(getArgument("-b",args).equals("tpch")){
					System.out.println("TPC-H");
					String dbname="tpch";
					try{
						Scanner sc=new Scanner(System.in);
						DistributedDatabaseClient.load(dbname);
						System.out.println("Load existing fragmentation?[y|n]");
						System.out.print("fragDB> ");
						String tmp=sc.next();
						while(!tmp.toLowerCase().equals("y") && !tmp.toLowerCase().equals("n") && !tmp.toLowerCase().equals("exit") && !tmp.toLowerCase().equals("exit;")){
							System.out.println("Load existing fragmentation?[y|n]");
							System.out.print("fragDB> ");
							tmp=sc.next();
						}
						if(tmp.toLowerCase().equals("y")){
							TPCH.tpch(servers, true);
						}
						else if(tmp.toLowerCase().equals("n")){
							TPCH.tpch(servers, false);
							
						}
						else if(tmp.toLowerCase().equals("exit")){
							System.exit(0);
						}
						else if(tmp.toLowerCase().equals("exit;")){
							System.exit(0);
						}
						sc.close();
						
					}
					catch(Exception e){
						//e.printStackTrace();
						TPCH.tpch(servers, false);
					}
					
				}
				else if(getArgument("-b",args).equals("tpce")){
					System.out.println("TPC-E");		
					double[] sfs=getTPCEArguments(args);
					if(sfs!=null){
						TPCE.tpce(servers, false,sfs[0],sfs[1],sfs[2],true,sfs[4],true);
					}else{
						TPCE.tpce(servers, false,1800);
					}
				}
				else{
					System.out.println("Possible parameters are:");
					System.out.println(options);
				}
			}
			else{
				System.out.println("Possible parameters are:");
				System.out.println(options);
			}
			
		} catch (Exception e) {
			System.out.println("Exception raised: "+e.getMessage());
			System.exit(0);
		}
	      
	}
	
	private static String getArgument(String option,String[] args){
		for(int i=0;i<args.length;i=i++){
			if(args[i].toLowerCase().equals(option.toLowerCase())){
				try{
					return args[i+1].toLowerCase();
				}
				catch(Exception e){
					System.out.println("Parameter for \""+option+"\" missing!");
					return null;
				}
			}
		}
		return null;	
	}
	
	private static double[] getTPCEArguments(String[] args){
		String option="-b";
		for(int i=0;i<args.length;i=i++){
			if(args[i].toLowerCase().equals(option.toLowerCase())){
				try{
					if(args[i+1].toLowerCase().equals("tpce")){
						double sf1=Double.parseDouble(args[i+2]);
						double sf2=Double.parseDouble(args[i+3]);
						double sf3=Double.parseDouble(args[i+4]);
						double timeout=-1.0;
						try{
							timeout=Double.parseDouble(args[i+5]);
						}catch(Exception e){
							
						}
						return new double[]{sf1,sf2,sf3,timeout};
					}
					
				}
				catch(Exception e){
					System.out.println("No scale factors provided!");
					return null;
				}
			}
		}
		return null;
	}
	
	private static void loadLibrary() {
		File f=null;
		String os = System.getProperty("os.name").toLowerCase();
		if(os.indexOf("win") >= 0) {
			f=new File("lib/ILOG.CPLEX.dll");
			try {
				System.load(f.getAbsolutePath());
			}catch(Exception e) {
				
			}
		}
		else if(os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 || os.indexOf("aix") > 0 ){
			f=new File("lib/libcplex1270.so");
			try {
				System.load(f.getAbsolutePath());
			}catch(Exception e) {
				
			}
		}
	}
	
}