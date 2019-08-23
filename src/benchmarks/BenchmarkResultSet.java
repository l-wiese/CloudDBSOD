package benchmarks;

import java.sql.ResultSet;

import util.Utility;

//A simple wrapper class for ResultSets that additionally holds information about the executed query
public class BenchmarkResultSet {
	private ResultSet rs = null;
	private long analyzeAndRewriteTime = 0;
	private long executionTime = 0;
	private int involvedServers = 0;
	private int involvedTableFragments = 0;

	public BenchmarkResultSet(ResultSet rs) {
		super();
		this.rs = rs;
	}

	public BenchmarkResultSet(ResultSet rs, long analyzeAndRewriteTime, long executionTime) {
		super();
		this.rs = rs;
		this.analyzeAndRewriteTime = analyzeAndRewriteTime;
		this.executionTime = executionTime;
	}

	public BenchmarkResultSet(ResultSet rs, long analyzeAndRewriteTime, long executionTime, int involvedServers) {
		super();
		this.rs = rs;
		this.analyzeAndRewriteTime = analyzeAndRewriteTime;
		this.executionTime = executionTime;
		this.involvedServers = involvedServers;
	}

	public BenchmarkResultSet(ResultSet rs, long analyzeAndRewriteTime, long executionTime, int involvedServers,
			int involvedTableFragments) {
		super();
		this.rs = rs;
		this.analyzeAndRewriteTime = analyzeAndRewriteTime;
		this.executionTime = executionTime;
		this.involvedServers = involvedServers;
		this.involvedTableFragments = involvedTableFragments;
	}

	public ResultSet getRs() {
		return rs;
	}

	public void setRs(ResultSet rs) {
		this.rs = rs;
	}

	public long getAnalyzeAndRewriteTime() {
		return analyzeAndRewriteTime;
	}

	public void setAnalyzeAndRewriteTime(long analyzeAndRewriteTime) {
		this.analyzeAndRewriteTime = analyzeAndRewriteTime;
	}

	public long getExecutionTime() {
		return executionTime;
	}

	public void setExecutionTime(long executionTime) {
		this.executionTime = executionTime;
	}

	public void print() {
		System.out.println();
		System.out.println("Analysis & Rewritting: " + analyzeAndRewriteTime + " Execution: " + executionTime
				+ " #Servers: " + involvedServers);
		Utility.print(rs);
	}

	public void printStats() {
		if (involvedTableFragments != 0) {
			System.out.println("Analysis & Rewritting: " + analyzeAndRewriteTime + " Execution: " + executionTime
					+ " #Servers: " + involvedServers + " #Fragments: " + involvedTableFragments);
		} else {
			System.out.println("Analysis & Rewritting: " + analyzeAndRewriteTime + " Execution: " + executionTime
					+ " #Servers: " + involvedServers);
		}
	}

	public int getInvolvedServers() {
		return involvedServers;
	}

	public void setInvolvedServers(int involvedServers) {
		this.involvedServers = involvedServers;
	}

	public int getInvolvedTableFragments() {
		return involvedTableFragments;
	}

	public void setInvolvedTableFragments(int involvedTableFragments) {
		this.involvedTableFragments = involvedTableFragments;
	}

}
