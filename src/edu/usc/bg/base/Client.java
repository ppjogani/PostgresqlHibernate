/**                                                                                                                                                                                
 * Copyright (c) 2012 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package edu.usc.bg.base;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import edu.usc.bg.KillThread;
import edu.usc.bg.MonitoringThread;
import edu.usc.bg.generator.Fragmentation;
import edu.usc.bg.validator.ValidationMainClass;
import edu.usc.bg.workloads.CoreWorkload;
import edu.usc.bg.workloads.loadActiveThread;
import edu.usc.bg.measurements.MyMeasurement;
import edu.usc.bg.measurements.StatsPrinter;


/**
 * This class keeps a track of the frequency of access for each member during the workload benchmark execution.
 * @author sumita barahmand
 */
class FreqElm{
	int uid = 0;
	int frequency = 0;
	public FreqElm(int userid, int userfrequency){
		uid = userid;
		frequency = userfrequency;
	}

	public int getFrequency(){
		return frequency;
	}

	public int getUserId(){
		return uid;
	}

}

/**
 * This class is needed for sorting the frequency of access to members for computing interested percentage frequencies.
 * @author sumita barahmand
 */
class FreqComparator implements Comparator<FreqElm> {
	public int compare(FreqElm o1, FreqElm o2) {
		if(o1.frequency>o2.frequency)
			return 1;
		return 0;
	}
}



/**
 * A thread to periodically show the status of the experiment, to reassure you
 * that progress is being made.
 * 
 * @author cooperb
 * 
 */

class StatusThread extends Thread {
	Vector<Thread> _threads;
	Workload _workload;

	/**
	 * The interval for reporting status.
	 */
	public static final long sleeptime = 10000;

	public StatusThread(Vector<Thread> threads, Workload workload) {
		_threads = threads;
		_workload = workload;
	}

	/**
	 * Run and periodically report status.
	 */
	public void run() {
		long st = System.currentTimeMillis();

		long lasten = st;
		long lasttotalops = 0;
		long lasttotalacts = 0;

		boolean alldone;

		do {
			alldone = true;

			int totalops = 0;
			int totalacts = 0;

			// terminate this thread when all the worker threads are done
			for (Thread t : _threads) {
				if (t.getState() != Thread.State.TERMINATED) {
					alldone = false;
				}

				ClientThread ct = (ClientThread) t;
				totalops += ct.getOpsDone();
				totalacts += ct.getActsDone();
			}

			long en = System.currentTimeMillis();

			long interval = 0;
			interval = en - st;

			double curthroughput = 1000.0 * (((double) (totalops - lasttotalops)) / ((double) (en - lasten)));
			double curactthroughput = 1000.0 * (((double) (totalacts - lasttotalacts)) / ((double) (en - lasten)));
			lasttotalops = totalops;
			lasttotalacts = totalacts;
			lasten = en;

			DecimalFormat d = new DecimalFormat("#.##");
	

		if (totalacts == 0) {
			System.out.print( " " + (interval / 1000) + " sec: "
					+ totalacts + " actions; ");
		} else {
			System.out.print(" " + (interval / 1000) + " sec: "
					+ totalacts + " actions; " + d.format(curactthroughput)
					+ " current acts/sec; ");
		}

		if (totalops == 0) {
				System.out.println( " " + (interval / 1000) + " sec: "
						+ totalops + " operations; "
						+ MyMeasurement.getSummary());
		} else {
				System.out.println(" " + (interval / 1000) + " sec: "
						+ totalops + " operations; " + d.format(curthroughput)
						+ " current ops/sec; "
						+ MyMeasurement.getSummary());
		}
		try {
			sleep(sleeptime);
		} catch (InterruptedException e) {
			// do nothing
		}

		} while (!alldone && !_workload.isStopRequested());
	}
}



/**
 * Main class for executing BG.
 */
public class Client {

	public static final String OPERATION_COUNT_PROPERTY = "operationcount";
	public static final String OPERATION_COUNT_PROPERTY_DEFAULT = "0";
	public static final String NUM_LOAD_THREAD_PROPERTY = "numloadthreads";
	public static final String USER_COUNT_PROPERTY = "usercount";
	public static final String USER_COUNT_PROPERTY_DEFAULT = "0";
	public static final String USER_OFFSET_PROPERTY = "useroffset";
	public static final String USER_OFFSET_PROPERTY_DEFAULT = "0";
	public static final String RESOURCE_COUNT_PROPERTY = "resourcecountperuser";
	public static final String RESOURCE_COUNT_PROPERTY_DEFAULT = "0";
	public static final String FRIENDSHIP_COUNT_PROPERTY = "friendcountperuser";
	public static final String FRIENDSHIP_COUNT_PROPERTY_DEFAULT = "0";
	public static final String CONFPERC_COUNT_PROPERTY = "confperc";
	public static final String RATING_MODE_PROPERTY ="ratingmode";
	public static final String RATING_MODE_PROPERTY_DEFAULT = "false";
	//needed when rating is happening
	public static final String EXPECTED_LATENCY_PROPERTY ="expectedlatency";
	public static final String EXPECTED_LATENCY_PROPERTY_DEFAULT ="1.3";
	public static final String EXPORT_FILE_PROPERTY ="exportfile";

	public static final String WORKLOAD_PROPERTY = "workload";
	public static final String USER_WORKLOAD_PROPERTY = "userworkload";
	public static final String FRIENDSHIP_WORKLOAD_PROPERTY = "friendshipworkload";
	public static final String RESOURCE_WORKLOAD_PROPERTY = "resourceworkload";


	//in the benchmark phase, these params will be queried from the data store initially and will be set
	public static final String INIT_STATS_REQ_APPROACH_PROPERTY = "initapproach";
	//can be QUERYDATA, CATALOGUQ, QUERYBITMAP 
	public static final String INIT_USER_COUNT_PROPERTY = "initialmembercount";
	public static final String INIT_USER_COUNT_PROPERTY_DEFAULT =  "0";
	public static final String INIT_FRND_COUNT_PROPERTY = "initialfriendsperuser";
	public static final String INIT_PEND_COUNT_PROPERTY = "initialpendingsperuser";
	public static final String INIT_FRND_COUNT_PROPERTY_DEFAULT =  "0";
	public static final String INIT_RES_COUNT_PROPERTY = "initialresourcesperuser";
	public static final String INIT_RES_COUNT_PROPERTY_DEFAULT =  "0";


	public static final String INSERT_IMAGE_PROPERTY = "insertimage";
	public static final String INSERT_IMAGE_PROPERTY_DEFAULT = "false";
	public static final String IMAGE_SIZE_PROPERTY = "imagesize";
	public static final String IMAGE_SIZE_PROPERTY_DEFAULT = "2";
	public static final String THREAD_CNT_PROPERTY = "threadcount";
	public static final String THREAD_CNT_PROPERTY_DEFAULT = "1";
	public static final String THINK_TIME_PROPERTY = "thinktime";
	public static final String THINK_TIME_PROPERTY_DEFAULT = "0";
	public static final String INTERARRIVAL_TIME_PROPERTY = "interarrivaltime";
	public static final String INTERARRIVAL_TIME_PROPERTY_DEFAULT = "0";
	public static final String WARMUP_OP_PROPERTY = "warmup";
	public static final String WARMUP_OP_PROPERTY_DEFAULT = "0";
	public static final String WARMUP_THREADS_PROPERTY = "warmupthreads";
	public static final String WARMUP_THREADS_PROPERTY_DEFAULT = "10";
	public static final String PORT_PROPERTY= "port";
	public static final String PORT_PROPERTY_DEFAULT = "10655";
	public static final String DB_CLIENT_PROPERTY = "db";
	public static final String DB_CLIENT_PROPERTY_DEFAULT = "fake.TestClient";
	public static final String MACHINE_ID_PROPERTY = "machineid";
	public static final String MACHINE_ID_PROPERTY_DEFAULT = "0";
	public static final String NUM_BG_PROPERTY = "numclients";
	public static final String NUM_BG_PROPERTY_DEFAULT = "1";
	public static final String MONITOR_DURATION_PROPERTY = "monitor";
	public static final String MONITOR_DURATION_PROPERTY_DEFAULT = "0";
	public static final String TARGET__PROPERTY = "target";
	public static final String TARGET_PROPERTY_DEFAULT = "0";
	public static final String LOG_DIR_PROPERTY= "logdir";
	public static final String LOG_DIR_PROPERTY_DEFAULT = ".";
	public static final String PROBS_PROPERTY= "probs";
	public static final String PROBS_PROPERTY_DEFAULT = "";
	
	/**
	 * The maximum amount of time (in seconds) for which the benchmark will be
	 * run.
	 */
	public static final String MAX_EXECUTION_TIME = "maxexecutiontime";
	public static final String MAX_EXECUTION_TIME_DEFAULT = "0";


	public static int machineid = 0;	
	public static int numBGClients = 1;

	public static void usageMessage() {
		System.out.println("Usage: java BG [options]");
		System.out.println("Options:");
		System.out
		.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n"
				+ "             be specified as the \"target\" property using -p");
		System.out.println("  -load:  run the loading phase of the workload");
		System.out.println("  -loadindex:  run the loading phase of the workload and create index structures once loadig completed");
		System.out.println("  -schema:  Create the schema used for the load phase of the workload");
		System.out.println("  -testdb:  Tests the database by trying to connect to it");
		System.out.println("  -stats:  Queries the database stats such as usercount, avgfriendcountpermember and etc.");
		System.out
		.println("  -t:  run the benchmark phase of the workload (default)");
		System.out
		.println("  -db dbname: specify the name of the DB to use  - \n"
				+ "              can also be specified as the \"db\" property using -p");
		System.out
		.println("  -P propertyfile: load properties from the given file. Multiple files can");
		System.out
		.println("                   be specified, and will be processed in the order specified");
		System.out
		.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
		System.out
		.println("                  multiple properties can be specified, and override any");
		System.out.println("                  values in the propertyfile");
		System.out
		.println("  -s:  show status during run (default: no status)");
		System.out.println("");
		System.out.println("Required properties:");
		System.out
		.println("  "
				+ USER_WORKLOAD_PROPERTY
				+ ", "
				+ FRIENDSHIP_WORKLOAD_PROPERTY
				+ " ,"
				+ RESOURCE_WORKLOAD_PROPERTY
				+ ": the name of the workload class to use for -load or -loadindex (e.g. edu.usc.bg.workloads.CoreWorkload)");
		System.out
		.println("  "
				+ WORKLOAD_PROPERTY
				+ ": the name of the workload class to use for -t (e.g. edu.usc.bg.workloads.CoreWorkload)");
		System.out.println("");
		System.out
		.println("To run the transaction phase from multiple servers, start a separate client on each.");
		System.out
		.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
		System.out
		.println("use the \"usercount\" and \"useroffset\" properties to divide up the records to be inserted");
		System.out
		.println("You can also load data store using the dzipfian fragments by using \"requestdistribution=dzipfian\" ");
		System.out
		.println("and \"zipfianmean=0.27\", for this approach you need to specify the number of bgclients, the current bgclient machineid and the rates of all the involved machienids");
	}

	/** 
	 * check if the workload parameters exist
	 */
	public static boolean checkRequiredProperties(Properties props,
			boolean dotransactions, boolean doSchema, boolean doTestDB, boolean doStats) {
		if (dotransactions) { 
			//benchmark phase
			if (props.getProperty(WORKLOAD_PROPERTY) == null) {
				System.out.println("Missing property: " + WORKLOAD_PROPERTY);
				return false;
			}
		} else {
			if(!doSchema && !doTestDB && !doStats){
				//if schema is already created and are in load phase
				if (props.getProperty(USER_WORKLOAD_PROPERTY) == null
						|| props.getProperty(FRIENDSHIP_WORKLOAD_PROPERTY) == null
						|| props.getProperty(RESOURCE_WORKLOAD_PROPERTY) == null) {
					System.out.println("Missing property: "
							+ USER_WORKLOAD_PROPERTY + ", "
							+ FRIENDSHIP_WORKLOAD_PROPERTY + " ,"
							+ RESOURCE_WORKLOAD_PROPERTY);
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Exports the measurements to either console or a file using the printer.
	 * It also loads the statistics to a file and sends them to the coordinator if BG is in rating mode.
	 * @param opcount The total session count.
	 * @param actcount The total action count.
	 * @param runtime The duration of the benchmark.
	 * @param outpS Used when the statistics are written on a socket for the coordinator.
	 * @param props The properties of the benchmark.
	 * @param benchmarkStats Contains the statistics of the benchmark.
	 * @throws IOException
	 *             Either failed to write to output stream or failed to close
	 *             it.
	 */
	private static void printFinalStats(Properties props, int opcount, int actcount,
			long runtime, HashMap<String, Integer> benchmarkStats, PrintWriter outpS) throws IOException {
		StatsPrinter printer = null;
		try {
			//compute stats
			double sessionthroughput = 1000.0 * ((double) opcount)
					/ ((double) runtime);
			double actthroughput = 1000.0 * ((double) actcount)
					/ ((double) runtime);

			//not considering the ramp down
			double sessionthroughputTillFirstDeath = 0;
			if(benchmarkStats.get("TimeTillFirstDeath") != null && benchmarkStats.get("OpsTillFirstDeath") != null ){
				sessionthroughputTillFirstDeath = 1000.0 * ((double)benchmarkStats.get("OpsTillFirstDeath") )
						/ ((double) benchmarkStats.get("TimeTillFirstDeath"));	
			}
			double actthroughputTillFirstDeath =0;
			if(benchmarkStats.get("TimeTillFirstDeath") != null && benchmarkStats.get("ActsTillFirstDeath") != null ){
				actthroughputTillFirstDeath = 1000.0 * ((double)benchmarkStats.get("ActsTillFirstDeath") )
						/ ((double) benchmarkStats.get("TimeTillFirstDeath"));
			}
		
			//write to socket
			//only when BG is in rating mode is true and it writes to the socket communicating with the coordinator
			if(props.getProperty(RATING_MODE_PROPERTY, RATING_MODE_PROPERTY_DEFAULT).equals("true")){
				outpS.print(" OVERALLRUNTIME(ms):"+runtime+" ");
				outpS.flush();
				outpS.print(" OVERALLOPCOUNT(SESSIONS):"+opcount+" ");	
				outpS.flush();
				outpS.print(" OVERALLTHROUGHPUT(SESSIONS/SECS):"+sessionthroughput+" ");
				outpS.flush();
				outpS.print(" OVERALLOPCOUNT(ACTIONS):"+actcount);	
				outpS.flush();
				outpS.print(" OVERALLTHROUGHPUT(ACTIONS/SECS):"+actthroughput+" ");
				outpS.flush();
				if (benchmarkStats != null) {
					//the run time and throughput right when the first thread dies
					if(benchmarkStats.get("TimeTillFirstDeath") != null){
						outpS.print(" RAMPEDRUNTIME(ms):"+benchmarkStats.get("TimeTillFirstDeath")+" ");
						outpS.flush();
					}
					if(benchmarkStats.get("OpsTillFirstDeath") != null){
						outpS.print(" RAMPEDOPCOUNT(SESSIONS):"+benchmarkStats.get("OpsTillFirstDeath")+" ");
						outpS.flush();
					}
					if(benchmarkStats.get("TimeTillFirstDeath") != null && benchmarkStats.get("OpsTillFirstDeath") != null ){
						outpS.print(" RAMPEDTHROUGHPUT(SESSIONS/SECS):"+sessionthroughputTillFirstDeath +" ");
						outpS.flush();
					}
					if(benchmarkStats.get("ActsTillFirstDeath") != null){
						outpS.print(" RAMPEDOPCOUNT(ACTIONS):"+benchmarkStats.get("ActsTillFirstDeath")+" ");
						outpS.flush();
					}
					if(benchmarkStats.get("TimeTillFirstDeath") != null && benchmarkStats.get("ActsTillFirstDeath") != null ){
						outpS.print(" RAMPEDTHROUGHPUT(ACTIONS/SECS):"+actthroughputTillFirstDeath +" ");
						outpS.flush();
					}
					if(benchmarkStats.get("NumReadOps") != null){
						outpS.print(" READ(OPS):"+benchmarkStats.get("NumReadOps")+" ");
						outpS.flush();
					}

					if(benchmarkStats.get("NumPruned") != null){
						outpS.print(" PRUNED(OPS):"+benchmarkStats.get("NumPruned")+" ");
						outpS.flush();
					}

					if(benchmarkStats.get("NumProcessed") != null){
						outpS.print(" PROCESSED(OPS):"+benchmarkStats.get("NumProcessed")+" ");
						outpS.flush();
					}

					if(benchmarkStats.get("NumWriteOps") != null){
						outpS.print(" WRITE(OPS):"+benchmarkStats.get("NumWriteOps")+" ");
						outpS.flush();
					}

					if(benchmarkStats.get("ValidationTime") != null){
						outpS.print(" VALIDATIONTIME(MS):"+benchmarkStats.get("ValidationTime")+" ");
						outpS.flush();
					}

					if(benchmarkStats.get("NumReadOps") != null && benchmarkStats.get("NumStaleOps") != null){
						if(benchmarkStats.get("NumReadOps") == 0)
							outpS.print(" STALENESS(OPS):0 ");
						else
							outpS.print(" STALENESS(OPS):"+(((double) benchmarkStats.get("NumStaleOps")) / benchmarkStats.get("NumReadOps"))+" ");
						outpS.flush();
					}else{
						outpS.print(" STALENESS(OPS):0 ");
						outpS.flush();
					}

					double satisfyingPerc = MyMeasurement.getSatisfyingPerc();
					outpS.print(" SATISFYINGOPS(%):"+satisfyingPerc+" ");
					outpS.flush();
					outpS.print(" THEEND. ");
					outpS.flush();
				}
			}

			//write to file	
			// if no destination file is provided the results will be written to stdout.
			OutputStream out;
			String exportFile = props.getProperty(EXPORT_FILE_PROPERTY );
			if (exportFile == null) {
				out = System.out;
			} else {
				out = new FileOutputStream(exportFile);
			}
			try {
				printer = new StatsPrinter(out);
			} catch (Exception e) {
				e.printStackTrace(System.out);
				
			}

			// write the date to the beginning of the output file
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			printer.write("DATE: " + dateFormat.format(date).toString());
			// write the workload details to the beginning of the file
			String params = "Runtime configuration parameters (those missing from the list have default values):\n";
			@SuppressWarnings("unchecked")
			Enumeration<Object> em = (Enumeration<Object>) props.propertyNames();
			while(em.hasMoreElements()){
				String str = (String)em.nextElement();
			  	params+=("\n"+str + ": " + props.get(str));
			}
			
			params+="\nSanity Init parameters:\n";
			params += "sanityMemberCount="
					+ props.getProperty(INIT_USER_COUNT_PROPERTY, INIT_USER_COUNT_PROPERTY_DEFAULT)
					+" ,sanityAvgFriendsPerUserCount="
					+props.getProperty(INIT_FRND_COUNT_PROPERTY,INIT_FRND_COUNT_PROPERTY_DEFAULT)
					+" ,sanityAvgPendingsPerUserCount="
					+props.getProperty(INIT_PEND_COUNT_PROPERTY,INIT_PEND_COUNT_PROPERTY)
					+" ,sanityResourcePerUserCount="
					+ props.getProperty(INIT_RES_COUNT_PROPERTY, INIT_RES_COUNT_PROPERTY_DEFAULT);

			printer.write(params);
			printer.write("OVERALL", "RunTime(ms)", runtime);
			printer.write("OVERALL", "opcount(sessions)", opcount);
			printer.write("OVERALL", "Throughput(sessions/sec)", sessionthroughput);
			printer.write("OVERALL", "actcount(actions)", actcount);

			printer.write("OVERALL", "Throughput(actions/sec)", actthroughput);
			System.out.println("OVERALLOPCOUNT(SESSIONS):"+opcount);
			System.out.println("OVERALLTHROUGHPUT(SESSIONS/SECS):"+sessionthroughput);
			System.out.println("OVERALLOPCOUNT(ACTIONS):"+actcount);
			System.out.println("OVERALLTHROUGHPUT(ACTIONS/SECS):"+actthroughput);



			if (benchmarkStats != null) {
				//the run time and throughput right when the first thread dies
				if(benchmarkStats.get("TimeTillFirstDeath") != null){
					printer.write("UntilFirstThreadsDeath", "RunTime(ms)", benchmarkStats.get("TimeTillFirstDeath"));
				}
				if(benchmarkStats.get("OpsTillFirstDeath") != null){
					printer.write("UntilFirstThreadsDeath", "opcount(sessions)", benchmarkStats.get("OpsTillFirstDeath"));
					System.out.println("RAMPEDOVERALLOPCOUNT(SESSIONS):"+benchmarkStats.get("OpsTillFirstDeath"));
				}
				if(benchmarkStats.get("TimeTillFirstDeath") != null && benchmarkStats.get("OpsTillFirstDeath") != null ){
					printer.write("UntilFirstThreadsDeath", "Throughput(sessions/sec)", sessionthroughputTillFirstDeath  );
					System.out.println("RAMPEDOVERALLTHROUGHPUT(SESSIONS/SECS):"+sessionthroughputTillFirstDeath );
				}
				if(benchmarkStats.get("ActsTillFirstDeath") != null){
					printer.write("UntilFirstThreadsDeath", "opcount(actions)", benchmarkStats.get("ActsTillFirstDeath"));
					System.out.println("RAMPEDOVERALLOPCOUNT(ACTIONS):"+benchmarkStats.get("ActsTillFirstDeath"));
				}
				if(benchmarkStats.get("TimeTillFirstDeath") != null && benchmarkStats.get("ActsTillFirstDeath") != null ){
					printer.write("UntilFirstThreadsDeath", "Throughput(actions/sec)", actthroughputTillFirstDeath  );
					System.out.println("RAMPEDOVERALLTHROUGHPUT(ACTIONS/SECS):"+actthroughputTillFirstDeath );		
				}				

				if(benchmarkStats.get("NumReadOps") != null)
					printer.write("OVERALL", "Read(ops)", benchmarkStats.get("NumReadOps"));
				if(benchmarkStats.get("NumStaleOps") != null)
					printer.write("OVERALL", "StaleRead(ops)", benchmarkStats.get("NumStaleOps"));
				if(benchmarkStats.get("NumReadOps") != null && benchmarkStats.get("NumStaleOps") != null){
					printer.write("OVERALL", "Staleness(staleReads/totalReads)",
							((double) benchmarkStats.get("NumStaleOps")) / benchmarkStats.get("NumReadOps"));
				}if(benchmarkStats.get("NumReadSessions") != null)
					printer.write("OVERALL", "Read(sessions)", benchmarkStats.get("NumReadSessions"));
				if(benchmarkStats.get("NumStaleSessions") != null)
					printer.write("OVERALL", "StaleRead(sessions)",
							benchmarkStats.get("NumStaleSessions"));
				if(benchmarkStats.get("NumStaleSessions") != null &&  benchmarkStats.get("NumReadSessions") != null)
					printer.write("OVERALL",
							"Staleness(staleSessions/totalSessions)",
							((double) benchmarkStats.get("NumStaleSessions")) / benchmarkStats.get("NumReadSessions"));
				if(benchmarkStats.get("NumPruned") != null)
					printer.write("OVERALL", "Pruned(ops)", benchmarkStats.get("NumPruned"));
				if(benchmarkStats.get("ValidationTime") != null)
					printer.write("OVERALL", "Validationtime(ms)",
							benchmarkStats.get("ValidationTime"));
				if(benchmarkStats.get("DumpAndValidateTime") != null)
					printer.write("OVERALL", "DumpAndValidationtime(ms)",
							benchmarkStats.get("DumpAndValidateTime"));
				if(benchmarkStats.get("DumpTime") != null)
					printer.write("OVERALL", "dumpFilesToDB (ms)",
							benchmarkStats.get("DumpTime"));
			}//if benchmarkStats not null

			printer.write(MyMeasurement.getFinalResults());
			//Needed in case you want to print out frequency related stats
			printer.write(CoreWorkload.getFrequecyStats());
		} finally {
			if (printer != null) {
				printer.close();
			}
		}
	}
	
	public static void main(String[] args) {
		String dbname;
		Properties props = new Properties();
		Properties fileprops = new Properties();
		boolean dotransactions = true;
		boolean doSchema = false;
		boolean doTestDB = false;
		boolean doStats = false;
		boolean doIndex = false;
		int threadcount = 1;
		int target = 0;
		boolean status = false;

		// parse arguments
		int argindex = 0;

		if (args.length == 0) {
			usageMessage();
			System.exit(0);
		}

		while (args[argindex].startsWith("-")) {
			if (args[argindex].compareTo("-threads") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				int tcount = Integer.parseInt(args[argindex]);
				props.setProperty(THREAD_CNT_PROPERTY, tcount + "");
				argindex++;
			} else if (args[argindex].compareTo("-target") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				int ttarget = Integer.parseInt(args[argindex]);
				props.setProperty("target", ttarget + "");
				argindex++;
			} else if (args[argindex].compareTo("-load") == 0) {
				dotransactions = false;
				argindex++;
			}else if (args[argindex].compareTo("-loadindex") == 0) {
				dotransactions = false;
				doIndex = true;
				argindex++; 
			}else if (args[argindex].compareTo("-t") == 0) {
				dotransactions = true;
				argindex++;
			}else if (args[argindex].compareTo("-schema") == 0) {
				dotransactions = false;
				doSchema = true;
				argindex++;
			}else if (args[argindex].compareTo("-testdb") == 0) {
				dotransactions = false;
				doTestDB = true;
				argindex++;
			}else if (args[argindex].compareTo("-stats") == 0) {
				dotransactions = false;
				doStats = true;
				argindex++;
			}
			else if (args[argindex].compareTo("-s") == 0) {
				status = true;
				argindex++;
			} else if (args[argindex].compareTo("-db") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				props.setProperty("db", args[argindex]);
				argindex++;
			} else if (args[argindex].compareTo("-P") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				String propfile = args[argindex];
				argindex++;

				Properties myfileprops = new Properties();
				try {
					myfileprops.load(new FileInputStream(propfile));
				} catch (IOException e) {
					System.out.println(e.getMessage());
					System.exit(0);
				}

				for (Enumeration e = myfileprops.propertyNames(); e
						.hasMoreElements();) {
					String prop = (String) e.nextElement();

					fileprops.setProperty(prop, myfileprops.getProperty(prop));
				}

			} else if (args[argindex].compareTo("-p") == 0) {
				argindex++;
				if (argindex >= args.length) {
					usageMessage();
					System.exit(0);
				}
				int eq = args[argindex].indexOf('=');
				if (eq < 0) {
					usageMessage();
					System.exit(0);
				}

				String name = args[argindex].substring(0, eq);
				String value = args[argindex].substring(eq + 1);
				props.put(name, value);
				argindex++;
			} else {
				System.out.println("Unknown option " + args[argindex]);
				usageMessage();
				System.exit(0);
			}

			if (argindex >= args.length) {
				break;
			}
		}// done reading command args


		ServerSocket BGSocket;
		Socket connection = null;
		InputStream inS;
		OutputStream outS;
		PrintWriter outpS = null;
		Scanner inSS = null;
		//doing distributed rating using the coordinator
		if(Boolean.parseBoolean(props.getProperty(RATING_MODE_PROPERTY, RATING_MODE_PROPERTY_DEFAULT)) == true && dotransactions){
			int port = Integer.parseInt(props.getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT));
			System.out.println("Trying to do rating with the specified thread count , creating socket on "+port);
			try {
				System.out.println("Started");
				BGSocket = new ServerSocket(port,10);
				System.out.println("BGClient: started and Waiting for connection on "+port);
				connection = BGSocket.accept();
				System.out.println("BGClient: Connection received from " + connection.getInetAddress().getHostName());
				inS = connection.getInputStream();
				outS = connection.getOutputStream();
				outpS = new PrintWriter(outS);
				inSS = new Scanner(inS);
				//send connected message to the rater thread
				outpS.print("Initiated ");
				outpS.flush();
				System.out.println("BGClient: SENT initiation message to " + connection.getInetAddress().getHostName());
				
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

		// overwrite file properties with properties from the command line
		for (Enumeration e = props.propertyNames(); e.hasMoreElements();) {
			String prop = (String) e.nextElement();

			fileprops.setProperty(prop, props.getProperty(prop));
		}

		props = fileprops;

		if (!checkRequiredProperties(props, dotransactions, doSchema, doTestDB, doStats)) {
			System.exit(0);
		}


		machineid = Integer.parseInt(props.getProperty(MACHINE_ID_PROPERTY,MACHINE_ID_PROPERTY_DEFAULT));
		numBGClients = Integer.parseInt(props.getProperty(NUM_BG_PROPERTY,NUM_BG_PROPERTY_DEFAULT));

		//verify threadcount 
		//get number of threads, target and db
		threadcount = Integer.parseInt(props.getProperty(THREAD_CNT_PROPERTY, THREAD_CNT_PROPERTY_DEFAULT));		
		if(dotransactions){
			//needed for the activate user array
			if(Integer.parseInt(props.getProperty(USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT)) <  threadcount){
				props.setProperty(THREAD_CNT_PROPERTY,"5" );
				threadcount = 5;
			}
			//so no thread will get 0 ops
			if(Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY, OPERATION_COUNT_PROPERTY_DEFAULT)) != 0 && Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY, OPERATION_COUNT_PROPERTY_DEFAULT)) % threadcount != 0){
				threadcount--;
				while(Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY, OPERATION_COUNT_PROPERTY_DEFAULT))%threadcount != 0)
					threadcount--; 
				props.setProperty(THREAD_CNT_PROPERTY,Integer.toString(threadcount) );
			}
		}else{
			if(!doSchema && !doTestDB && !doStats){ //when creating schema the number of threads does not matter
				//so no thread will get 0 users
				if(Integer.parseInt(props.getProperty(USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT)) <  threadcount){
					props.setProperty(THREAD_CNT_PROPERTY,"5" );
					threadcount = 5;
				}
				if(Integer.parseInt(props.getProperty(USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT)) % threadcount != 0){
					while(Integer.parseInt(props.getProperty(USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT))%threadcount != 0)
						threadcount--; 
					props.setProperty(THREAD_CNT_PROPERTY,Integer.toString(threadcount) );
				}
				//ensure the friendship creation within clusters for each thread makes sense
				if(Integer.parseInt(props.getProperty(FRIENDSHIP_COUNT_PROPERTY, FRIENDSHIP_COUNT_PROPERTY_DEFAULT)) != 0){
					int tmp = Integer.parseInt(props.getProperty(USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT))/threadcount;
					while(tmp <= Integer.parseInt(props.getProperty(FRIENDSHIP_COUNT_PROPERTY, FRIENDSHIP_COUNT_PROPERTY_DEFAULT))){
						threadcount--;
						while(Integer.parseInt(props.getProperty(USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT))%threadcount != 0)
							threadcount--;
						tmp = Integer.parseInt(props.getProperty(USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT))/threadcount;					
					} 
					props.setProperty(THREAD_CNT_PROPERTY,Integer.toString(threadcount) );
				}
			}
		}

		int monitoringTime = Integer.parseInt(props.getProperty(MONITOR_DURATION_PROPERTY, MONITOR_DURATION_PROPERTY_DEFAULT));

		long maxExecutionTime = Integer.parseInt(props.getProperty(
				MAX_EXECUTION_TIME, MAX_EXECUTION_TIME_DEFAULT));

		System.out.println("*****max execution time specified : "+maxExecutionTime);

		dbname = props.getProperty(DB_CLIENT_PROPERTY, DB_CLIENT_PROPERTY_DEFAULT);
		target = Integer.parseInt(props.getProperty(TARGET__PROPERTY, TARGET_PROPERTY_DEFAULT));

		// compute the target throughput
		double targetperthreadperms = -1;
		if (target > 0) {
			double targetperthread = ((double) target) / ((double) threadcount);
			targetperthreadperms = targetperthread / 1000.0;
		}
		System.out.println("BG Client: ThreadCount ="+threadcount);
		
		// show a warning message that creating the workload is taking a while
		// but only do so if it is taking longer than 2 seconds
		// (showing the message right away if the setup wasn't taking very long
		// was confusing people).
		Thread warningthread = new Thread() {
			public void run() {
				try {
					sleep(2000);
				} catch (InterruptedException e) {
					return;
				}
				System.out
				.println(" (might take a few minutes for large data sets)");
			}
		};

		warningthread.start();
		System.out.println();
		System.out.println("Loading workload...");

		// load the workload
		ClassLoader classLoader = Client.class.getClassLoader();

		Workload workload = null;
		Workload userWorkload = null;
		Workload friendshipWorkload = null;
		Workload resourceWorkload = null;

		try {
			if (dotransactions) {
				//check to see if the sum of all activity and action proportions are 1
				double totalProb = 0;
				System.out.println(props.getProperty(CoreWorkload.DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY, CoreWorkload.DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT));
				totalProb = Double.parseDouble(props.getProperty(CoreWorkload.GETOWNPROFILE_PROPORTION_PROPERTY, CoreWorkload.GETOWNPROFILE_PROPORTION_PROPERTY_DEFAULT))+Double.parseDouble(props.getProperty(CoreWorkload.GETFRIENDPROFILE_PROPORTION_PROPERTY, CoreWorkload.GETFRIENDPROFILE_PROPORTION_PROPERTY_DEFAULT))
						+Double.parseDouble(props.getProperty(CoreWorkload.POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY,CoreWorkload.POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY_DEFAULT))+
						Double.parseDouble(props.getProperty(CoreWorkload.DELCOMMENTONRESOURCE_PROPORTION_PROPERTY,CoreWorkload.DELCOMMENTONRESOURCE_PROPORTION_PROPERTY_DEFAULT))
						+Double.parseDouble(props.getProperty(CoreWorkload.GENERATEFRIENDSHIP_PROPORTION_PROPERTY, CoreWorkload.GENERATEFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT))
						+Double.parseDouble(props.getProperty(CoreWorkload.ACCEPTFRIENDSHIP_PROPORTION_PROPERTY, CoreWorkload.ACCEPTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT))+Double.parseDouble(props.getProperty(CoreWorkload.REJECTFRIENDSHIP_PROPORTION_PROPERTY, CoreWorkload.REJECTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT))
						+Double.parseDouble(props.getProperty(CoreWorkload.UNFRIEND_PROPORTION_PROPERTY,CoreWorkload.UNFRIEND_PROPORTION_PROPERTY_DEFAULT))+Double.parseDouble(props.getProperty(CoreWorkload.GETRANDOMPROFILEACTION_PROPORTION_PROPERTY, CoreWorkload.GETRANDOMPROFILEACTION_PROPORTION_PROPERTY_DEFAULT))
						+Double.parseDouble(props.getProperty(CoreWorkload.GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY, CoreWorkload.GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT))+ Double.parseDouble(props.getProperty(CoreWorkload.GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY, CoreWorkload.GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY_DEFAULT))
						+Double.parseDouble(props.getProperty(CoreWorkload.INVITEFRIENDSACTION_PROPORTION_PROPERTY, CoreWorkload.INVITEFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT))+Double.parseDouble(props.getProperty(CoreWorkload.ACCEPTFRIENDSACTION_PROPORTION_PROPERTY, CoreWorkload.ACCEPTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT))
						+Double.parseDouble(props.getProperty(CoreWorkload.REJECTFRIENDSACTION_PROPORTION_PROPERTY, CoreWorkload.REJECTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT)) + Double.parseDouble(props.getProperty(CoreWorkload.UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY, CoreWorkload.UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT))
						+Double.parseDouble(props.getProperty(CoreWorkload.GETTOPRESOURCEACTION_PROPORTION_PROPERTY, CoreWorkload.GETTOPRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT))+Double.parseDouble(props.getProperty(CoreWorkload.GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY, CoreWorkload.GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT))
						+Double.parseDouble(props.getProperty(CoreWorkload.POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY, CoreWorkload.POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT))
						+Double.parseDouble(props.getProperty(CoreWorkload.DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY, CoreWorkload.DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT));

				if( ((totalProb-1) >0.1) || ((1-totalProb) > 0.1)){
					System.out.println("The sum of the probabilities assigned to the actions and activities is not 1. Total Prob = "+totalProb);
					System.exit(0);
				}

				DB db = DBFactory.newDB(dbname, props);
				db.init();
				Class workloadclass = classLoader.loadClass(props
						.getProperty(WORKLOAD_PROPERTY));
				workload = (Workload) workloadclass.newInstance();
				//before starting the benchmark get the database statistics : member count, resource per member and avg friend per member 
				workload.init(props, null);
				props.setProperty(INIT_USER_COUNT_PROPERTY, workload.getDBInitialStats(db).get("usercount"));	
				props.setProperty(INIT_RES_COUNT_PROPERTY,workload.getDBInitialStats(db).get("resourcesperuser"));
				props.setProperty(INIT_FRND_COUNT_PROPERTY, workload.getDBInitialStats(db).get("avgfriendsperuser"));	
				props.setProperty(INIT_PEND_COUNT_PROPERTY, workload.getDBInitialStats(db).get("avgpendingperuser"));
				db.cleanup(true);
				MyMeasurement.resetMeasurement();
				
				System.out.println("\nAfter init: " + new Date());
			} 
		} catch (Exception e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}

		warningthread.interrupt();


		int numWarmpup =0;
		if((numWarmpup=Integer.parseInt(props.getProperty(WARMUP_OP_PROPERTY,WARMUP_OP_PROPERTY_DEFAULT))) != 0 && dotransactions){
			//do the warmup phase
			Vector<Thread> warmupThreads = new Vector<Thread>();
			int numWarmpThread = Integer.parseInt(props.getProperty(WARMUP_THREADS_PROPERTY, WARMUP_THREADS_PROPERTY_DEFAULT));
			System.out.println("Starting warmup with "+numWarmpThread+" threads and "+numWarmpup+" operations.");
			long wst = System.currentTimeMillis();
			if (dotransactions) {
				for (int threadid = 0; threadid < numWarmpThread; threadid++) {
					DB db = null;
					try {
						db = DBFactory.newDB(dbname, props);
					} catch (UnknownDBException e) {
						System.out.println("Unknown DB " + dbname);
						System.exit(0);
					}

					Thread t = new ClientThread(db, dotransactions, workload,
							threadid, threadcount, props, numWarmpup / numWarmpThread,
							targetperthreadperms, true);
					warmupThreads.add(t);

				}
				// initialize all threads before they start issuing requests - ramp up
				for (Thread t1 : warmupThreads) {
					boolean started = false;
					started = ((ClientThread) t1).initThread();
					while (!started) {
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
							e.printStackTrace(System.out);
						}
					}
				}
				// start all threads
				for (Thread t : warmupThreads) {
					t.start();
				}

				int warmupOpsDone =0;
				int warmupActsDone = 0;
				//wait for all warmup threads to end
				for (Thread t : warmupThreads) {
					try {
						t.join();
						warmupOpsDone += ((ClientThread) t).getOpsDone();
						warmupActsDone += ((ClientThread) t).getActsDone();
					} catch (InterruptedException e) {
						e.printStackTrace(System.out);
					}
				}
				System.out.println("num warmup Ops done (sessions=: "+warmupOpsDone+", actions="+warmupActsDone+")");
			}
			long wed = System.currentTimeMillis();
			System.out.println("End warmup. elapsedTime = "+(wed - wst));
			//we do not want the measurement for warmup to be counted in the overall measurements
			MyMeasurement.resetMeasurement();
			
			System.out.println("\nAfter warmup: " + new Date());
		}//end warmup
		
		
		System.out.println("Connected");
		
		
		int opcount;
		//keep a thread of the worker client threads
		Vector<Thread> threads = new Vector<Thread>();
		if (dotransactions) {
			String line="";
			//wait till you get the start message from the coordinator
			if(Boolean.parseBoolean(props.getProperty(RATING_MODE_PROPERTY, RATING_MODE_PROPERTY_DEFAULT)) ){
				if(inSS != null){
					while(inSS.hasNext()){
						line = inSS.next();
						if(line.equals("StartSimulation")){
							break;
						}
					} 
					System.out.println("BGCLient: Received start simulation msg and will run benchmark");
				}
				
				if(threadcount == 0 && outpS != null){
					//need to exit
					System.out.println("DONE");
					outpS.print(" OVERALLRUNTIME(ms):"+0);			
					outpS.print(" OVERALLOPCOUNT(SESSIONS):"+0);			
					outpS.print(" OVERALLTHROUGHPUT(SESSIONS/SECS):"+0);
					outpS.print(" OVERALLOPCOUNT(ACTIONS):"+0);			
					outpS.print(" OVERALLTHROUGHPUT(ACTIONS/SECS):"+0);
					outpS.print(" RAMPEDRUNTIME(ms):"+0);
					outpS.print(" RAMPEDOPCOUNT(SESSIONS):"+0);
					outpS.print(" RAMPEDTHROUGHPUT(SESSIONS/SECS):"+0 );
					outpS.print(" RAMPEDOPCOUNT(ACTIONS):"+0);
					outpS.print(" RAMPEDTHROUGHPUT(ACTIONS/SECS):"+0 );
					outpS.print(" STALENESS(OPS):"+0);
					outpS.print(" SATISFYINGOPS(%):"+100);
					outpS.print(" THEEND. ");
					outpS.flush();
					System.exit(0);
				}
			}else if(threadcount == 0){
				System.out.println("Invalid thread count: 0, system exiting");
				System.exit(0);
			}
		
			
			
		
			// run the workload
			System.out.println("Starting benchmark.");


			opcount = Integer.parseInt(props.getProperty(
					OPERATION_COUNT_PROPERTY, OPERATION_COUNT_PROPERTY_DEFAULT));

			for (int threadid = 0; threadid < threadcount; threadid++) {
				DB db = null;
				try {
					db = DBFactory.newDB(dbname, props);
				} catch (UnknownDBException e) {
					System.out.println("Unknown DB " + dbname);
					System.exit(0);
				}

				Thread t = new ClientThread(db, dotransactions, workload,
						threadid, threadcount, props, opcount / threadcount,
						targetperthreadperms, false);

				threads.add(t);
			}

			long st = System.currentTimeMillis();

			// initialize all threads before they start issuing requests - ramp up
			for (Thread t : threads) {
				boolean started = false;
				started = ((ClientThread) t).initThread();
				while (!started) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace(System.out);
					}
				}
			}

			StatusThread statusthread = null;

			if (status) {
				statusthread = new StatusThread(threads, workload);
				statusthread.start();
			}

			// start all threads
			for (Thread t : threads) {
				t.start();
			}

			Thread terminator = null;

			if (maxExecutionTime > 0) {
				terminator = new TerminatorThread(maxExecutionTime, threads,
						workload);
				terminator.start();
			}

			Thread killThread = null;
			Thread monitorThread = null;
			if(monitoringTime > 0){
				System.out.println("creating the kill thread which waits for kill msg and once received one it kills the BG client");
				killThread = new KillThread(inSS, threads, workload);
				killThread.start();
				System.out.println("creating monitoring thread to monitor the performance of the BGClient");
				monitorThread = new MonitoringThread(monitoringTime, threads, props, outpS, workload);
				monitorThread.start();
			}

			int opsDone = 0;  //keeps a track of total number of operations till all threads complete
			int actsDone = 0;
			//needed to stop capturing throughput once the first thread is dead
			int allOpsDone = 0; //keeps a track of total number of actions till the first thread completes
			int allActsDone = 0;
			boolean anyDied = false;
			long firstEn =0;


			for (Thread t : threads) {
				try {
					t.join();
					//System.out.println(((ClientThread)t)._threadid+" died");
					opsDone += ((ClientThread) t).getOpsDone();
					actsDone += ((ClientThread) t).getActsDone();
					//now that one thread has died for fair throughput computation we stop counting the rest of the completed operations
					if(!anyDied){
						//this is the first thread that is completing work
						firstEn = System.currentTimeMillis();
						for(Thread t1: threads){
							allOpsDone +=((ClientThread) t1).getOpsDone();
							allActsDone += ((ClientThread) t1).getActsDone();
						}
						anyDied = true;
					}

				} catch (InterruptedException e) {
				}
			}

			long en = System.currentTimeMillis();
//			if (terminator != null && !terminator.isInterrupted()) {
//				try {
//					terminator.join(2000);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//				if(terminator.isAlive())
//					terminator.interrupt();
//			}
//			System.out.println("TerminatorThread died");

			if (killThread != null && !killThread.isInterrupted()) {
				killThread.interrupt();
			}
			System.out.println("killerThread died");

			if (monitorThread != null && !monitorThread.isInterrupted()) {
				monitorThread.interrupt();
			}
			System.out.println("monitoringThread died");


			if (statusthread != null) {
				statusthread.interrupt();
			}

			System.out.println("statusThread died");

			System.out.println("\nAfter workload done: " + new Date());

			try {
				workload.cleanup();
			} catch (WorkloadException e) {
				e.printStackTrace(System.out);
				System.exit(0);
			}

			HashMap<String, Integer> expStat = new HashMap<String,Integer>();
			// if no updates or no reads have taken place , the validation phase does not happen
			if (CoreWorkload.updatesExist && CoreWorkload.readsExist) {
				// threadid,listOfSeqs seen  by that threadid
				HashMap<Integer,Integer>[] seqTracker = new HashMap[threadcount];
				HashMap<Integer,Integer>[] staleSeqTracker = new HashMap[threadcount];
				long dumpVTimeE=0, dumpVTimeS=0;

				System.out.println("--Discarding, dumping and validation starting.");
				dumpVTimeS = System.currentTimeMillis();
				ValidationMainClass.dumpFilesAndValidate(props, seqTracker, staleSeqTracker, expStat, outpS, props.getProperty(LOG_DIR_PROPERTY, LOG_DIR_PROPERTY_DEFAULT));
				dumpVTimeE = System.currentTimeMillis();
				System.out.println("******* Discrading, dumping and validation is done."+(dumpVTimeE-dumpVTimeS));
				expStat.put("DumpAndValidateTime",(int) (dumpVTimeE - dumpVTimeS)); //the dumptime
			}
			System.out.println("DONE");
			expStat.put("OpsTillFirstDeath",allOpsDone);
			expStat.put("ActsTillFirstDeath",allActsDone);
			expStat.put("TimeTillFirstDeath",(int)(firstEn-st)); //time it took till first thread dies
			try {
				printFinalStats(props, opsDone,actsDone ,en - st, expStat, outpS);
			} catch (IOException e) {
				System.out.println("Could not export measurements, error: "
						+ e.getMessage());
				e.printStackTrace(System.out);
				System.exit(-1);
			}

			System.out.println("Executing benchmark is completed.");

		} else {

			DB db = null;
			if(doStats){
				try {
					System.out.println("Querying statistics for the data store...");
					db = DBFactory.newDB(dbname, props);
					db.init();
					HashMap<String, String> initStats= db.getInitialStats();
					Set<String> keys = initStats.keySet();
					Iterator<String> it = keys.iterator();
					String str = "Stats:{";
					while(it.hasNext()){
						String tmpKey = it.next();
						str+="["+tmpKey+", "+initStats.get(tmpKey)+"]";
					}
					str+="}\n";
					System.out.println(str);
					db.cleanup(true);
					System.out.println("Query stats completed");
				} catch (UnknownDBException e) {
					e.printStackTrace(System.out);
				} catch (DBException e) {
					e.printStackTrace(System.out);
				}
				
			}else if (doTestDB){
				try {
					System.out.println("Creating connection to the data store...");
					db = DBFactory.newDB(dbname, props);
					boolean connState = db.init();
					db.cleanup(true);
					if(connState)
						System.out.println("connection was successful");
					else
						System.out.println("There was an error creating connection to the data store server.");
				} catch (UnknownDBException e) {
					e.printStackTrace(System.out);
				} catch (DBException e) {
					e.printStackTrace(System.out);
				}
			}else if(doSchema){
				// create schema for RDBMS
				try {
					System.out.println("Creating data store schema...");
					db = DBFactory.newDB(dbname, props);
					db.init();
					db.createSchema(props);
					db.cleanup(false);
					System.out.println("Schema creation was successful");
				} catch (UnknownDBException e) {
					e.printStackTrace(System.out);
				} catch (DBException e) {
					e.printStackTrace(System.out);
				}

			}else{			
				long loadStart, loadEnd;
				loadStart = System.currentTimeMillis();

				try {
					/*//if you want create schema and load to happen together uncomment this
						// create schema for RDBMS
						db = DBFactory.newDB(dbname, props);
						db.init();
						db.createSchema(props);
						db.cleanup(true);
					 */
					int useropcount = 0;
					int useroffset = 0;
					Vector<Integer> allMembers = new Vector<Integer>();
					//creating the memberids for this BGClient
					long loadst = System.currentTimeMillis();
					if(props.getProperty(CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY, CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY_DEFAULT).equals("dzipfian")){
						Fragmentation createFrags = new Fragmentation(Integer.parseInt(props.getProperty(USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT)), Integer.parseInt(props.getProperty(NUM_BG_PROPERTY,NUM_BG_PROPERTY_DEFAULT)),Integer.parseInt(props.getProperty(MACHINE_ID_PROPERTY,MACHINE_ID_PROPERTY_DEFAULT)),props.getProperty(PROBS_PROPERTY,PROBS_PROPERTY_DEFAULT), Double.parseDouble(props.getProperty(CoreWorkload.ZIPF_MEAN_PROPERTY,CoreWorkload.ZIPF_MEAN_PROPERTY_DEFAULT)));
						int[] myMembers = createFrags.getMyMembers();
						useropcount = myMembers.length;
						useroffset = Integer.parseInt(props.getProperty(
								USER_OFFSET_PROPERTY, USER_OFFSET_PROPERTY_DEFAULT));
						for(int j=0; j<useropcount; j++)
							allMembers.add(myMembers[j]+useroffset);					
					}else{
						useropcount = Integer.parseInt(props.getProperty(
								USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT));
						useroffset = Integer.parseInt(props.getProperty(
								USER_OFFSET_PROPERTY, USER_OFFSET_PROPERTY_DEFAULT));
						for(int j=0; j<useropcount; j++)
							allMembers.add(j+useroffset);	
					}
					System.out.println("Time to create fragments :"+(System.currentTimeMillis()-loadst)+" msecs");
					
					System.out.println("Done dividing users");
					loadActiveThread stateThread = new loadActiveThread();
					stateThread.start();

					int friendshipopcount = Integer.parseInt(props.getProperty(
									FRIENDSHIP_COUNT_PROPERTY, FRIENDSHIP_COUNT_PROPERTY_DEFAULT));
					int resourceopcount = Integer.parseInt(props.getProperty(
									RESOURCE_COUNT_PROPERTY, RESOURCE_COUNT_PROPERTY_DEFAULT));
					
					//verify thread count
					int numLoadThreads = Integer.parseInt(props.getProperty(THREAD_CNT_PROPERTY, THREAD_CNT_PROPERTY_DEFAULT));
					Vector<ClientThread> loadThreads = new Vector<ClientThread>();
					
					//verify fragment size related to friendships
					if(useropcount < friendshipopcount+1){
						System.out.println("Fragment size is too small, can't create appropriate friendships. exiting.");
						System.exit(0);
					}
					//verify load thread count and fragment size
					if(friendshipopcount >= useropcount/numLoadThreads){
						System.out.println("Can't load with "+numLoadThreads+" so loading with 1 thread");
						numLoadThreads = 1;
					}
					
					int numUserThreadOps = 0;
					numUserThreadOps = useropcount /numLoadThreads;	
					int remainingUsers = useropcount - (numLoadThreads*numUserThreadOps);
					int addUserCnt = 0;
					for(int j=0; j<numLoadThreads; j++){
						Properties tprop = new Properties();
						db = DBFactory.newDB(dbname, props);
						tprop = (Properties) props.clone();
						if(j==numLoadThreads-1)
							addUserCnt = remainingUsers;
						tprop.setProperty(USER_COUNT_PROPERTY, (Integer.toString(numUserThreadOps+addUserCnt)));
						Class userWorkloadclass = classLoader.loadClass(tprop
								.getProperty(USER_WORKLOAD_PROPERTY));
						userWorkload = (Workload) userWorkloadclass.newInstance();
						Vector<Integer> threadMembers = new Vector<Integer>();
						for(int u=j*numUserThreadOps; u< numUserThreadOps*j+numUserThreadOps+addUserCnt; u++){
							threadMembers.add(allMembers.get(u));
						}
						userWorkload.init(tprop, threadMembers);
						Thread t = new ClientThread(db, dotransactions,
								userWorkload, j, 1, tprop, numUserThreadOps+addUserCnt,
								targetperthreadperms, true);
						loadThreads.add((ClientThread) t);
						((ClientThread) t).initThread();
						t.start();
					}
					for(int j=0; j<numLoadThreads; j++)
						loadThreads.get(j).join();
					System.out.println("Done loading users");
					loadThreads = new Vector<ClientThread>();
					addUserCnt = 0;
					if (friendshipopcount != 0) {
						for(int j=0; j<numLoadThreads; j++){
							Properties tprop = new Properties();
							db = DBFactory.newDB(dbname, props);
							tprop = (Properties) props.clone();
							if(j == numLoadThreads-1){
								addUserCnt = remainingUsers;
							}
							tprop.setProperty(USER_COUNT_PROPERTY, Integer.toString(numUserThreadOps+addUserCnt));
							Class friendshipWorkloadclass = classLoader.loadClass(tprop
									.getProperty(FRIENDSHIP_WORKLOAD_PROPERTY));
							friendshipWorkload = (Workload) friendshipWorkloadclass
									.newInstance();
							Vector<Integer> threadMembers = new Vector<Integer>();
							for(int u=j*numUserThreadOps; u< numUserThreadOps*j+numUserThreadOps+addUserCnt; u++){
								threadMembers.add(allMembers.get(u));
							}
							friendshipWorkload.init(tprop, threadMembers);
							Thread t = new ClientThread(db, dotransactions,
									friendshipWorkload, j, 1, tprop,
									((numUserThreadOps+addUserCnt)*friendshipopcount), targetperthreadperms, true);
							
							loadThreads.add((ClientThread) t);
							((ClientThread) t).initThread();
							t.start();
						}
						for(int j=0; j<numLoadThreads; j++)
							loadThreads.get(j).join();
					}
					System.out.println("Done loading friends");
					loadThreads = new Vector<ClientThread>();
					addUserCnt = 0;
					if (resourceopcount != 0) {
						for(int j=0; j<numLoadThreads; j++){
							Properties tprop = new Properties();
							db = DBFactory.newDB(dbname, props);
							tprop = (Properties) props.clone();
							if(j == numLoadThreads-1)
								addUserCnt = remainingUsers;
							tprop.setProperty(USER_COUNT_PROPERTY, Integer.toString(numUserThreadOps+addUserCnt));
							Class resourceWorkloadclass = classLoader.loadClass(tprop
									.getProperty(RESOURCE_WORKLOAD_PROPERTY));
							resourceWorkload = (Workload) resourceWorkloadclass
									.newInstance();
							Vector<Integer> threadMembers = new Vector<Integer>();
							for(int u=j*numUserThreadOps; u< numUserThreadOps*j+numUserThreadOps+addUserCnt; u++){
								threadMembers.add(allMembers.get(u));
							}
							resourceWorkload.init(tprop, threadMembers);
							Thread t = new ClientThread(db, dotransactions,
									resourceWorkload, j, 1, tprop, (numUserThreadOps+addUserCnt)*resourceopcount,
									targetperthreadperms, true);
							loadThreads.add((ClientThread) t);
							((ClientThread) t).initThread();
							t.start();
						}
						for(int j=0; j<numLoadThreads; j++)
							loadThreads.get(j).join();	
					}
					System.out.println("Done loading resources");
		
					System.out.println("Done loading manipulation");
					if(doIndex){
						db = DBFactory.newDB(dbname, props);
						db.init();
						db.buildIndexes(props);
						db.cleanup(true);
					}
					System.out.println("Done creating indexes and closing db connection");
					loadEnd = System.currentTimeMillis();

					//printing out load data
					OutputStream out;
					String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
					String  text = "LoadTime (msec)="+(loadEnd-loadStart)+"\n";
					text += "Load configuration parameters (those missing from the list have default values):\n";
					Enumeration<Object> em = (Enumeration<Object>) props.propertyNames();
					while(em.hasMoreElements()){
						String str = (String)em.nextElement();
					  	text+=("\n"+str + ": " + props.get(str));
					}
										
					//sanity check using statistics from the data store
					text+="\n \n Stats queried from the data store: \n";
					db = DBFactory.newDB(dbname, props);
					db.init();
					Class userWorkloadclass = classLoader.loadClass(props
							.getProperty(USER_WORKLOAD_PROPERTY));
					userWorkload = (Workload) userWorkloadclass.newInstance();
					userWorkload.init(props, null);
					text+= "\t MemberCount=" + userWorkload.getDBInitialStats(db).get("usercount")+"\n";	
					text+= "\t ResourceCountPerUser=" + userWorkload.getDBInitialStats(db).get("resourcesperuser")+"\n";
					text+= "\t FriendCountPerUser=" + userWorkload.getDBInitialStats(db).get("avgfriendsperuser")+"\n";
					text+= "\t PendingCountPerUser=" + userWorkload.getDBInitialStats(db).get("avgpendingperuser")+"\n";
					db.cleanup(false);
					System.out.println("Done doing load sanity check");

					byte [] b= text.getBytes();
					if (exportFile == null) {
						out = System.out;
					} else {
						out = new FileOutputStream(exportFile);
					}
					out.write(b);
					if(stateThread != null) stateThread.setExit();
					stateThread.join();
					System.out.println("load state thread exited");
				} catch (Exception e) {
					e.printStackTrace(System.out);
				}
				
				System.out.println("Loading datastore completed.");
			}
		}
		System.exit(0);
	}
}
