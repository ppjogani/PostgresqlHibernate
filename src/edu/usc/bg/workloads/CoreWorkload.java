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


package edu.usc.bg.workloads;

import java.text.SimpleDateFormat;
import java.util.Properties;

import edu.usc.bg.Member;
import edu.usc.bg.base.*;
import edu.usc.bg.base.generator.CounterGenerator;
import edu.usc.bg.base.generator.DiscreteGenerator;
import edu.usc.bg.base.generator.IntegerGenerator;
import edu.usc.bg.base.generator.ScrambledZipfianGenerator;
import edu.usc.bg.base.generator.SkewedLatestGenerator;
import edu.usc.bg.base.generator.UniformIntegerGenerator;
import edu.usc.bg.generator.DistOfAccess;
import edu.usc.bg.generator.Fragmentation;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;




/**
 * Queries the initial status of the data store
 * This thread is activated in the benchmark phase (which is initiated with the -t flag), only if the INIT_STATS_REQ_APPROACH_PROPERTY property is set
 * The status collected here is used for generating invitations without facing integrity constraint issues and validation
 * @author barahman
 *
 */
class initQueryThread extends Thread{
	DB _db;
	int[] _tMembers;
	Properties _props ;
	HashMap<Integer, Vector<Integer>> _pIds ;
	HashMap<Integer,Vector<Integer>> _cIds ;
	HashMap<Integer, Vector<Integer>> _rIds ;
	HashMap<String, Integer> _initCnt ;

	/**
	 * Initialize the thread and its connection to the data store
	 * @param tMembers
	 * @param props
	 */
	initQueryThread(int[] tMembers, Properties props){
		_tMembers = tMembers;
		_pIds = new HashMap<Integer, Vector<Integer>>(_tMembers.length);
		_cIds = new HashMap<Integer, Vector<Integer>>(_tMembers.length);
		_rIds = new HashMap<Integer, Vector<Integer>>(_tMembers.length);
		_initCnt = new HashMap<String, Integer>(_tMembers.length*3);

		String dbname = props.getProperty(Client.DB_CLIENT_PROPERTY, Client.DB_CLIENT_PROPERTY_DEFAULT);
		_props = props;
		try {
			_db = DBFactory.newDB(dbname, props);
			_db.init();
		} catch (UnknownDBException e) {
			System.out.println("Unknown DB, QpendingThread " + dbname);
			System.exit(0);
		} catch (DBException e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}

	public HashMap<String, Integer> getInit(){
		return _initCnt;
	}

	public HashMap<Integer, Vector<Integer>> getPendings(){
		return _pIds;
	}

	public HashMap<Integer, Vector<Integer>> getConfirmed(){
		return _cIds;
	}

	public HashMap<Integer, Vector<Integer>> getResources(){
		return _rIds;
	}
	
	/**
	 * clears all the structures used by this thread once the thread is done reporting its data to the main thread
	 */
	public void freeResources(){
		_tMembers = null;
		if(_cIds != null){
			_cIds.clear();
			_cIds = null;
		}

		if(_pIds != null){
			_pIds.clear();
			_pIds = null;
		}
		
		if(_rIds != null){
			_rIds.clear();
			_rIds = null;
		}

		if(_initCnt != null){
			_initCnt.clear();
			_initCnt = null;
		}
	}


	public void run(){
		int res =0;
		for(int i=0; i<_tMembers.length; i++){
			HashMap<String, ByteIterator> profileResult = new HashMap<String, ByteIterator>();
			res = _db.viewProfile(_tMembers[i], _tMembers[i], profileResult, false, false);
			if(res < 0){
				System.out.println("Problem in getting initial stats.");
				System.exit(0);	
			}	
			_initCnt.put("PENDFRND-"+_tMembers[i], Integer.parseInt(profileResult.get("pendingcount").toString().trim()));
			_initCnt.put("ACCEPTFRND-"+_tMembers[i], Integer.parseInt(profileResult.get("friendcount").toString().trim()));
			if(profileResult != null){
				profileResult.clear();
				profileResult = null;
			}

			//get all resources for this user
			Vector<HashMap<String, ByteIterator>> resResult = new Vector<HashMap<String, ByteIterator>>();
			Vector<Integer> resIds = new Vector<Integer>();
			res = _db.getCreatedResources(_tMembers[i], resResult);
			if(res < 0){
				System.out.println("Problem in getting initial stats.");
				System.exit(0);	
			}	
			for(int d=0; d<resResult.size(); d++){
				String resId = resResult.get(d).get("rid").toString().trim();
				resIds.add(Integer.parseInt(resId));
				Vector<HashMap<String, ByteIterator>> commentResult = new Vector<HashMap<String, ByteIterator>>();
				res = _db.viewCommentOnResource(_tMembers[i], _tMembers[i], Integer.parseInt(resId), commentResult);
				if(res < 0){
					System.out.println("Problem in getting initial stats.");
					System.exit(0);	
				}	
				_initCnt.put("POSTCOMMENT-"+resId, commentResult.size());
				if(commentResult != null){
					commentResult.clear();
					commentResult = null;
				}
			}
			_rIds.put(_tMembers[i], resIds);
			if(resResult != null){
				resResult.clear();
				resResult = null;
			}
			//get pending friends to relate them
			Vector<Integer> pids = new Vector<Integer>();
			res = _db.queryPendingFriendshipIds(_tMembers[i], pids);
			if(res < 0){
				System.out.println("Problem in getting initial stats.");
				System.exit(0);	
			}	
			_pIds.put(_tMembers[i], pids);
			//get confirmed friends to relate them
			Vector<Integer> cids = new Vector<Integer>();
			res = _db.queryConfirmedFriendshipIds(_tMembers[i], cids);
			if(res < 0){
				System.out.println("Problem in getting initial stats.");
				System.exit(0);	
			}	
			_cIds.put(_tMembers[i], cids);
		}

		try {
			_db.cleanup(true);
		} catch (Exception e) {
			e.printStackTrace(System.out);
			return;
		}
	}
}

/**
 * responsible for creating the benchmarking workload for issuing the queries based on the workload file specified
 * @author barahman
 *
 */
public class CoreWorkload extends Workload
{
	/**
	 * Once set to true, BG generates log files for the actions issued
	 */
	public static final boolean enableLogging = true;
	/**
	 * needed for zipfian distributions
	 */
	public static final String ZIPF_MEAN_PROPERTY= "zipfianmean";
	public static final String ZIPF_MEAN_PROPERTY_DEFAULT = "0.27";
	/**
	 * The name of the property for the the distribution of requests across the keyspace. Options are "uniform", "zipfian" and "latest"
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY="requestdistribution";
	/**
	 * The default distribution of requests across the keyspace
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT="uniform";
	/**
	 * Percentage users that only access their own profile property name
	 */
	public static final String GETOWNPROFILE_PROPORTION_PROPERTY="ViewSelfProfileSession";
	/**
	 * The default proportion of functionalities that are ViewSelfProfile
	 */
	public static final String GETOWNPROFILE_PROPORTION_PROPERTY_DEFAULT="1.0";
	/**
	 * Percentage users that  access their friend profile property name
	 */
	public static final String GETFRIENDPROFILE_PROPORTION_PROPERTY="ViewFrdProfileSession";
	/**
	 * The default proportion of functionalities that are ViewFrdProfile
	 */
	public static final String GETFRIENDPROFILE_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that  post comment on their own resource
	 */
	public static final String POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY="PostCmtOnResSession";
	/**
	 * The default proportion of functionalities that are postCommentOnResource
	 */
	public static final String POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that  delete comment on their own resource
	 */
	public static final String DELCOMMENTONRESOURCE_PROPORTION_PROPERTY="DeleteCmtOnResSession";
	/**
	 * The default proportion of functionalities that are delCommentOnResource
	 */
	public static final String DELCOMMENTONRESOURCE_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that  do the generate friendship requests in their sessions
	 */
	public static final String GENERATEFRIENDSHIP_PROPORTION_PROPERTY="InviteFrdSession";
	/**
	 * The default proportion of functionalities that are generateFriendRequest
	 */
	public static final String GENERATEFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT="0.0";	
	/**
	 * Percentage users that  do the accept friendship sequence
	 */
	public static final String ACCEPTFRIENDSHIP_PROPORTION_PROPERTY="AcceptFrdReqSession";
	/**
	 * The default proportion of functionalities that are acceptFriendship
	 */
	public static final String ACCEPTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the reject friendship sequence
	 */
	public static final String REJECTFRIENDSHIP_PROPORTION_PROPERTY="RejectFrdReqSession";
	/**
	 * The default proportion of functionalities that are rejectFriendship
	 */
	public static final String REJECTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the unfriend friendship user session
	 */
	public static final String UNFRIEND_PROPORTION_PROPERTY="ThawFrdshipSession";
	/**
	 * The default proportion of functionalities that are unFriendFriend
	 */
	public static final String UNFRIEND_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the unfriend/accept friendship sequence
	 */
	public static final String UNFRIENDACCEPTFRIENDSHIP_PROPORTION_PROPERTY="unFriendAcceptProportion";
	/**
	 * The default proportion of functionalities that are unfriend/accept
	 */
	public static final String UNFRIENDACCEPTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the unfriend/reject friendship sequence
	 */
	public static final String UNFRIENDREJECTFRIENDSHIP_PROPORTION_PROPERTY="unFriendRejectProportion";
	/**
	 * The default proportion of functionalities that are unfriend/reject
	 */
	public static final String UNFRIENDREJECTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT="0.0";

	//individual actions
	/**
	 * Percentage users that do the getProfile action
	 */
	public static final String GETRANDOMPROFILEACTION_PROPORTION_PROPERTY="ViewProfileAction";
	/**
	 * The default proportion of getprofile action
	 */
	public static final String GETRANDOMPROFILEACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the listFriends action
	 */
	public static final String GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY="ListFriendsAction";
	/**
	 * The default proportion of listFriends action
	 */
	public static final String GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the getlistofpendingrequests action
	 */
	public static final String GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY="ViewFriendReqAction";
	/**
	 * The default proportion of getlistofpendingrequests action
	 */
	public static final String GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the inviteFriend action
	 */
	public static final String INVITEFRIENDSACTION_PROPORTION_PROPERTY="InviteFriendAction";
	/**
	 * The default proportion of inviteFriend action
	 */
	public static final String INVITEFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the acceptfriends action
	 */
	public static final String ACCEPTFRIENDSACTION_PROPORTION_PROPERTY="AcceptFriendReqAction";
	/**
	 * The default proportion of acceptfriends action
	 */
	public static final String ACCEPTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the rejectfriends action
	 */
	public static final String REJECTFRIENDSACTION_PROPORTION_PROPERTY="RejectFriendReqAction";
	/**
	 * The default proportion of rejectfriends action
	 */
	public static final String REJECTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the unfriendfriends action
	 */
	public static final String UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY="ThawFriendshipAction";
	/**
	 * The default proportion of unfriendfriends action
	 */
	public static final String UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the gettopresources action
	 */
	public static final String GETTOPRESOURCEACTION_PROPORTION_PROPERTY="ViewTopKResourcesAction";
	/**
	 * The default proportion of gettopresources action
	 */
	public static final String GETTOPRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the getcommentsonresources action
	 */
	public static final String GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY="ViewCommentsOnResourceAction";
	/**
	 * The default proportion of getcommentsonresources action
	 */
	public static final String GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the postcommentonresources action
	 */
	public static final String POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY="PostCommentOnResourceAction";
	/**
	 * The default proportion of postcommentonresources action
	 */
	public static final String POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the delcommentonresources action
	 */
	public static final String DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY="DeleteCommentOnResourceAction";
	/**
	 * The default proportion of delcommentonresources action
	 */
	public static final String DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT="0.0";

	/**
	 * keeps a track of the existence of reads in the workload
	 * if no reads have occurred or no read log files created the validation check need not happen-shared for all threads
	 */
	public static boolean readsExist = false;

	/**
	 * Keeps a track of existence of updates in the workload
	 * if no updates have occurred or no update log files have been created the validation check need not happen-shared for all threads 
	 */
	public static boolean updatesExist = false;

	public static HashMap<String, Integer> initStats = new HashMap<String, Integer>();

	private static int numShards = 101;
	private static char[][] userStatusShards;
	private static Semaphore[] uStatSemaphores;
	//keeps a track of user frequency of access
	private static int[][] userFreqShards;
	private static Semaphore[] uFreqSemaphores;
	private static DistOfAccess myDist;
	private static Member[] myMemberObjs;	

	static Random random = new Random();
	IntegerGenerator keysequence;
	CounterGenerator transactioninsertkeysequence;
	DiscreteGenerator operationchooser;
	IntegerGenerator keychooser;
	int usercount;
	int useroffset;

	//keep a track of all related users for every user
	private HashMap<Integer, String>[] userRelations = null;
	private static Semaphore rStat = new Semaphore(1, true);

	//no need to have a lock as a user can't be activated by multiple threads to accept invitations
	private static Vector<Integer>[] pendingFrnds;
	private static Semaphore aFrnds = new Semaphore(1, true);
	private static HashMap<Integer, String>[] acceptedFrnds ;
	HashMap<Integer, Integer> memberIdxs = new HashMap<Integer, Integer>();
	private static Vector<Integer>[] createdResources;
	private static Semaphore sCmts = new Semaphore(1, true);
	private static HashMap<Integer,Vector<Integer>> postedComments;
	private static HashMap<Integer, Integer> maxCommentIds ;

	String requestdistrib = "";
	int machineid = 0;
	int numBGClients = 1;
	double ZipfianMean = 0.27;


	/**
	 * Initialize the scenario. 
	 * Called once, in the main client thread, before any operations are started.
	 */
	public void init(Properties p, Vector<Integer> members) throws WorkloadException
	{	

		//sessions
		double getownprofileproportion=Double.parseDouble(p.getProperty(GETOWNPROFILE_PROPORTION_PROPERTY,GETOWNPROFILE_PROPORTION_PROPERTY_DEFAULT));
		double getfriendprofileproportion=Double.parseDouble(p.getProperty(GETFRIENDPROFILE_PROPORTION_PROPERTY,GETFRIENDPROFILE_PROPORTION_PROPERTY_DEFAULT));
		double postcommentonresourceproportion=Double.parseDouble(p.getProperty(POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY,POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY_DEFAULT));
		double delcommentonresourceproportion=Double.parseDouble(p.getProperty(DELCOMMENTONRESOURCE_PROPORTION_PROPERTY,DELCOMMENTONRESOURCE_PROPORTION_PROPERTY_DEFAULT));
		double acceptfriendshipproportion=Double.parseDouble(p.getProperty(ACCEPTFRIENDSHIP_PROPORTION_PROPERTY,ACCEPTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT));
		double genfriendshipproportion=Double.parseDouble(p.getProperty(GENERATEFRIENDSHIP_PROPORTION_PROPERTY,GENERATEFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT));
		double rejectfriendshipproportion=Double.parseDouble(p.getProperty(REJECTFRIENDSHIP_PROPORTION_PROPERTY,REJECTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT));
		double unfriendfriendproportion=Double.parseDouble(p.getProperty(UNFRIEND_PROPORTION_PROPERTY,UNFRIEND_PROPORTION_PROPERTY_DEFAULT));
		double unfriendacceptfriendshipproportion=Double.parseDouble(p.getProperty(UNFRIENDACCEPTFRIENDSHIP_PROPORTION_PROPERTY,UNFRIENDACCEPTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT));
		double unfriendrejectfriendshipproportion=Double.parseDouble(p.getProperty(UNFRIENDREJECTFRIENDSHIP_PROPORTION_PROPERTY,UNFRIENDREJECTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT));
		//actions
		double getprofileactionproportion = Double.parseDouble(p.getProperty(GETRANDOMPROFILEACTION_PROPORTION_PROPERTY,GETRANDOMPROFILEACTION_PROPORTION_PROPERTY_DEFAULT));
		double getfriendsactionproportion = Double.parseDouble(p.getProperty(GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY,GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT));
		double getpendingrequestsactionproportion = Double.parseDouble(p.getProperty(GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY,GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY_DEFAULT));
		double inviteFriendactionproportion = Double.parseDouble(p.getProperty(INVITEFRIENDSACTION_PROPORTION_PROPERTY,INVITEFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT));
		double acceptfriendactionproportion = Double.parseDouble(p.getProperty(ACCEPTFRIENDSACTION_PROPORTION_PROPERTY,ACCEPTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT));
		double rejectfriendactionproportion = Double.parseDouble(p.getProperty(REJECTFRIENDSACTION_PROPORTION_PROPERTY,REJECTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT));
		double unfriendfriendactionproportion = Double.parseDouble(p.getProperty(UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY,UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT));
		double gettopresourcesactionproportion = Double.parseDouble(p.getProperty(GETTOPRESOURCEACTION_PROPORTION_PROPERTY,GETTOPRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT));
		double postcommentonresourceactionproportion = Double.parseDouble(p.getProperty(POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY,POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT));
		double delcommentonresourceactionproportion = Double.parseDouble(p.getProperty(DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY,DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT));
		double viewCommentOnResourceactionproportion = Double.parseDouble(p.getProperty(GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY,GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT));

		usercount=Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY, Client.USER_COUNT_PROPERTY_DEFAULT));
		useroffset = Integer.parseInt(p.getProperty(Client.USER_OFFSET_PROPERTY,Client.USER_COUNT_PROPERTY_DEFAULT));
		requestdistrib=p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
		machineid = Client.machineid;


		keysequence=new CounterGenerator(useroffset);
		operationchooser=new DiscreteGenerator();
		if (getownprofileproportion>0)
		{
			operationchooser.addValue(getownprofileproportion,"OWNPROFILE");
		}

		if (getfriendprofileproportion>0)
		{
			operationchooser.addValue(getfriendprofileproportion,"FRIENDPROFILE");
		}

		if (postcommentonresourceproportion>0)
		{
			operationchooser.addValue(postcommentonresourceproportion,"POSTCOMMENT");
		}
		
		if (delcommentonresourceproportion>0)
		{
			operationchooser.addValue(delcommentonresourceproportion,"DELCOMMENT");
		}

		if(acceptfriendshipproportion > 0)
		{
			operationchooser.addValue(acceptfriendshipproportion,"ACCEPTREQ");
		}

		if(rejectfriendshipproportion > 0)
		{
			operationchooser.addValue(rejectfriendshipproportion,"REJECTREQ");
		}

		if(unfriendacceptfriendshipproportion > 0)
		{
			operationchooser.addValue(unfriendacceptfriendshipproportion,"UNFRNDACCEPTREQ");
		}

		if(unfriendrejectfriendshipproportion > 0)
		{
			operationchooser.addValue(unfriendrejectfriendshipproportion,"UNFRNDREJECTREQ");
		}

		if(unfriendfriendproportion > 0)
		{
			operationchooser.addValue(unfriendfriendproportion,"UNFRNDREQ");
		}

		if(genfriendshipproportion > 0)
		{
			operationchooser.addValue(genfriendshipproportion,"GENFRNDREQ");
		}

		//actions
		if(getprofileactionproportion > 0)
		{
			operationchooser.addValue(getprofileactionproportion,"GETPROACT");
		}
		if(getfriendsactionproportion > 0)
		{
			operationchooser.addValue(getfriendsactionproportion,"GETFRNDLSTACT");
		}
		if(getpendingrequestsactionproportion > 0)
		{
			operationchooser.addValue(getpendingrequestsactionproportion,"GETPENDACT");
		}
		if(inviteFriendactionproportion > 0)
		{
			operationchooser.addValue(inviteFriendactionproportion,"INVFRNDACT");
		}
		if(acceptfriendactionproportion > 0)
		{
			operationchooser.addValue(acceptfriendactionproportion,"ACCFRNDACT");
		}
		if(rejectfriendactionproportion > 0)
		{
			operationchooser.addValue(rejectfriendactionproportion,"REJFRNDACT");
		}
		if(unfriendfriendactionproportion > 0)
		{
			operationchooser.addValue(unfriendfriendactionproportion,"UNFRNDACT");
		}
		if(gettopresourcesactionproportion > 0)
		{
			operationchooser.addValue(gettopresourcesactionproportion,"GETRESACT");
		}
		if(viewCommentOnResourceactionproportion > 0)
		{
			operationchooser.addValue(viewCommentOnResourceactionproportion,"GETCMTACT");
		}
		if(postcommentonresourceactionproportion > 0)
		{
			operationchooser.addValue(postcommentonresourceactionproportion,"POSTCMTACT");
		}
		if(delcommentonresourceactionproportion > 0)
		{
			operationchooser.addValue(delcommentonresourceactionproportion,"DELCMTACT");
		}

		transactioninsertkeysequence=new CounterGenerator(usercount);
		long loadst = System.currentTimeMillis();
		if (requestdistrib.compareTo("uniform")==0)
		{
			keychooser=new UniformIntegerGenerator(0,usercount-1);
			myMemberObjs = new Member[usercount];
			for(int j=0; j<usercount; j++){
				memberIdxs.put(j+useroffset, j);
				Member newMember = new Member(j+useroffset, j, (j%numShards), (j/numShards+j%numShards));
				myMemberObjs[j] = newMember;
			}
		}
		else if (requestdistrib.compareTo("zipfian")==0)
		{
			//it does this by generating a random "next key" in part by taking the modulus over the number of keys
			//if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
			//so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
			//of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
			//plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
			//just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled zipfian generator
			keychooser=new ScrambledZipfianGenerator(usercount);
			myMemberObjs = new Member[usercount];
			for(int j=0; j<usercount; j++){
				memberIdxs.put(j+useroffset, j);
				Member newMember = new Member(j+useroffset, j, (j%numShards), (j/numShards+j%numShards));
				myMemberObjs[j] = newMember;
			}
		}
		else if(requestdistrib.compareTo("dzipfian")==0){
			System.out.println("Create fragments in workload init phase");
			Fragmentation createFrags = new Fragmentation(usercount, Integer.parseInt(p.getProperty(Client.NUM_BG_PROPERTY,Client.NUM_BG_PROPERTY_DEFAULT)),machineid,p.getProperty("probs",""), ZipfianMean);
			myDist = createFrags.getMyDist();
			int[] myMembers = createFrags.getMyMembers();
			usercount = myMembers.length;
			myMemberObjs =new Member[usercount];
			for(int j=0; j<usercount;j++){
				memberIdxs.put(myMembers[j], j);
				Member newMember = new Member(myMembers[j], j, (j%numShards), (j/numShards));
				myMemberObjs[j] = newMember;
			}

		}
		else if (requestdistrib.compareTo("latest")==0)
		{
			keychooser=new SkewedLatestGenerator(transactioninsertkeysequence);
			myMemberObjs = new Member[usercount];
			for(int j=0; j<usercount; j++){
				memberIdxs.put(j+useroffset, j);
				Member newMember = new Member(j+useroffset, j, (j%numShards), (j/numShards+j%numShards));
				myMemberObjs[j] = newMember;
			}
		}
		else
		{
			throw new WorkloadException("Unknown request distribution \""+requestdistrib+"\"");
		}
		System.out.println("Time to create fragments : "+(System.currentTimeMillis()-loadst)+" msec");

		userStatusShards = new char[numShards][];
		uStatSemaphores = new Semaphore[numShards];

		userFreqShards = new int[numShards][];
		uFreqSemaphores = new Semaphore[numShards];

		int avgShardSize = usercount/numShards;
		int remainingMembers = usercount-(avgShardSize*numShards);
		for(int i=0; i<numShards; i++){
			int numShardUsers = avgShardSize;
			if(i<remainingMembers)
				numShardUsers++;
			userStatusShards[i] = new char[numShardUsers];
			userFreqShards[i] = new int[numShardUsers];
			for(int j=0; j<numShardUsers; j++){
				userStatusShards[i][j]='d';
				userFreqShards[i][j]=0;
			}
			uStatSemaphores[i] = new Semaphore(1, true);
			uFreqSemaphores[i] = new Semaphore(1, true);
		}



		try {
			rStat.acquire();
			userRelations = new HashMap[usercount];
			for(int i=0; i<myMemberObjs.length; i++){
				//initially adding every user to the related vector of themselves
				HashMap<Integer, String> init = new HashMap<Integer, String>();
				init.put(myMemberObjs[i].get_uid(),"");
				userRelations[i] = init;
			}
			rStat.release();
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}


		//initialize the pendingFrnds, the acceptedFrnds and the createdresources data structures
		int numResPerUser = Integer.parseInt(p.getProperty(Client.RESOURCE_COUNT_PROPERTY, Client.RESOURCE_COUNT_PROPERTY_DEFAULT));
		pendingFrnds = new Vector[usercount];
		acceptedFrnds = new HashMap[usercount];
		createdResources = new Vector[usercount];
		postedComments = new HashMap<Integer, Vector<Integer>>();
		maxCommentIds = new HashMap<Integer, Integer>(0);
		for(int i=0; i<myMemberObjs.length;i++){
			pendingFrnds[i]= new Vector<Integer>();
			acceptedFrnds[i] = new HashMap<Integer,String>();
			createdResources[i] = new Vector<Integer>();
			for(int j=0; j<numResPerUser; j++){
				postedComments.put((myMemberObjs[i].get_uid()*numResPerUser+j), new Vector<Integer>());
				maxCommentIds.put((myMemberObjs[i].get_uid()*numResPerUser+j), 0);
			}
		}

		if(p.getProperty(Client.INIT_STATS_REQ_APPROACH_PROPERTY) != null && p.getProperty(Client.INIT_STATS_REQ_APPROACH_PROPERTY).equalsIgnoreCase("QUERYDATA")){
			int numQThreads = 5;
			Vector<initQueryThread> qThreads = new Vector<initQueryThread>();
			int tUserCount = myMemberObjs.length / numQThreads;
			int remainingUsers = myMemberObjs.length - (tUserCount*numQThreads);
			int addUser = 0;
			for(int u=0; u<numQThreads; u++){
				if(u == numQThreads-1)
					addUser = remainingUsers;
				int[] tMembers = new int[tUserCount+addUser];
				for(int d=0; d<tUserCount+addUser; d++)
					tMembers[d] = myMemberObjs[d+u*tUserCount].get_uid();
				initQueryThread t = new initQueryThread(tMembers, p);
				qThreads.add(t);
				t.start();	
			}
			for (Thread t : qThreads) {
				try {
					t.join();
					initStats.putAll(((initQueryThread)t).getInit());
					HashMap<Integer, Vector<Integer>> pends = ((initQueryThread)t).getPendings();
					Set<Integer> keys = pends.keySet();
					Iterator<Integer> it = keys.iterator();
					while(it.hasNext()){
						int aKey = (Integer)(it.next());
						pendingFrnds[memberIdxs.get(aKey)] = pends.get(aKey);
						for(int d=0; d<pends.get(aKey).size(); d++){
							relateUsers(aKey, pends.get(aKey).get(d));
						}
					}
					
					HashMap<Integer, Vector<Integer>> confs = ((initQueryThread)t).getConfirmed();
					keys = confs.keySet();
					it = keys.iterator();
					while(it.hasNext()){
						int aKey = (Integer)(it.next());
						for(int d=0; d<confs.get(aKey).size(); d++){
							acceptedFrnds[memberIdxs.get(aKey)].put(confs.get(aKey).get(d),null);
							relateUsers(aKey, confs.get(aKey).get(d));
						}
					}
					
					HashMap<Integer, Vector<Integer>> resources = ((initQueryThread)t).getResources();
					Set<Integer> ukeys = resources.keySet();
					it = ukeys.iterator();
					while(it.hasNext()){
						int uKey = (Integer)(it.next());
						createdResources[memberIdxs.get(uKey)] = resources.get(uKey);
					}
					((initQueryThread)t).freeResources();
				}catch(Exception e){
					e.printStackTrace(System.out);
				}
			}
			
			qThreads.clear();
			qThreads = null;

			
		}
		else if(p.getProperty(Client.INIT_STATS_REQ_APPROACH_PROPERTY) != null && 
				p.getProperty(Client.INIT_STATS_REQ_APPROACH_PROPERTY).equalsIgnoreCase("LOADFILE"))
		{
			int tUserCount = myMemberObjs.length;
			int numResourcesPerUser = 10;
			int numFriendsPerUser = 100;
			int userOffset = 0;
			double confPerc = 1;
			
			int friendId;
			
			if(p.getProperty(Client.RESOURCE_COUNT_PROPERTY) == null || 
					p.getProperty(Client.FRIENDSHIP_COUNT_PROPERTY) == null || 
					p.getProperty(Client.CONFPERC_COUNT_PROPERTY) == null || 
					p.getProperty(Client.USER_OFFSET_PROPERTY) == null)
			{
				throw new WorkloadException("parameters not specified for initapproach=loadfile. " +
						"Be sure to include the file used to load the data (e.g: -P workloads/populateDB)");
			}
			
			numResourcesPerUser = Integer.parseInt(p.getProperty(Client.RESOURCE_COUNT_PROPERTY));
			numFriendsPerUser = Integer.parseInt(p.getProperty(Client.FRIENDSHIP_COUNT_PROPERTY));
			confPerc = Double.parseDouble(p.getProperty(Client.CONFPERC_COUNT_PROPERTY));
			userOffset = Integer.parseInt(p.getProperty(Client.USER_OFFSET_PROPERTY));
			
			if(confPerc != 0 && confPerc != 1.0)
			{
				throw new WorkloadException("confperc must be 0 or 1 to use initapproach=loadfile");
			}
			else if(userOffset != 0)
			{
				throw new WorkloadException("non-zero useroffset not currently supported for initapproach=loadfile");
			}
			
			
			for( int i = 0; i < tUserCount; i++ )
			{
				initStats.put("PENDFRND-"+i, 0);
				initStats.put("ACCEPTFRND-"+i, numFriendsPerUser);
				for(int j = 0; j < numResourcesPerUser; j++)
				{
					initStats.put("POSTCOMMENT-"+(i*numResourcesPerUser +j), 0);
				}
				
				for(int j = 0; j < numFriendsPerUser + 1; j++)
				{
					friendId = i - (numFriendsPerUser / 2) + j;
					if(friendId < 0)
					{
						friendId += tUserCount;
					}
					else if(friendId >= tUserCount)
					{
						friendId -= tUserCount;
					}
					
					if(friendId != i)
					{
						acceptedFrnds[i].put(friendId, null);
						relateUsers(i, friendId);
					}
				}
			}
		}else if (p.getProperty(Client.INIT_STATS_REQ_APPROACH_PROPERTY) != null && 
				p.getProperty(Client.INIT_STATS_REQ_APPROACH_PROPERTY).equalsIgnoreCase("DETERMINISTIC")){
				
			int tUserCount = myMemberObjs.length;
			int numResourcesPerUser = 10;
			int numFriendsPerUser = 100;
			int userOffset = 0;
			int numLoadThread = 1;
			double confPerc = 1;
			if(p.getProperty(Client.RESOURCE_COUNT_PROPERTY) == null || 
					p.getProperty(Client.FRIENDSHIP_COUNT_PROPERTY) == null || 
					p.getProperty(Client.CONFPERC_COUNT_PROPERTY) == null ||
					p.getProperty(Client.NUM_LOAD_THREAD_PROPERTY) == null )
			{
				throw new WorkloadException("parameters not specified for initapproach=deterministic. " +
						"Be sure to include resourcecountperuser, friendcountperuser, confperc and numloadthreads )");
			}
			
			numResourcesPerUser = Integer.parseInt(p.getProperty(Client.RESOURCE_COUNT_PROPERTY));
			numFriendsPerUser = Integer.parseInt(p.getProperty(Client.FRIENDSHIP_COUNT_PROPERTY));
			confPerc = Double.parseDouble(p.getProperty(Client.CONFPERC_COUNT_PROPERTY));
			userOffset = Integer.parseInt(p.getProperty(Client.USER_OFFSET_PROPERTY));
			numLoadThread = Integer.parseInt(p.getProperty(Client.NUM_LOAD_THREAD_PROPERTY));
			
			if(confPerc != 0 && confPerc != 1.0)
			{
				throw new WorkloadException("confperc must be 0 or 1 to use initapproach=deterministic");
			}
			else if(userOffset != 0)
			{
				throw new WorkloadException("non-zero useroffset not currently supported for initapproach=loadfile");
			}
			
			if(Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY, Client.USER_COUNT_PROPERTY_DEFAULT)) <  numLoadThread){
				numLoadThread = 5;
			}
			if(Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY, Client.USER_COUNT_PROPERTY_DEFAULT)) % numLoadThread != 0){
				while(Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY, Client.USER_COUNT_PROPERTY_DEFAULT))% numLoadThread != 0)
					numLoadThread--; 
			}
			//ensure the friendship creation within clusters for each thread makes sense
			if(numFriendsPerUser != 0){
				int tmp = Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY, Client.USER_COUNT_PROPERTY_DEFAULT))/numLoadThread;
				while(tmp <= numFriendsPerUser){
					numLoadThread--;
					while(Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY, Client.USER_COUNT_PROPERTY_DEFAULT))%numLoadThread != 0)
						numLoadThread--;
					tmp = Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY, Client.USER_COUNT_PROPERTY_DEFAULT))/numLoadThread;					
				} 
			}
			//verify fragment size related to friendships
			if(tUserCount < numFriendsPerUser+1){
				System.out.println("Fragment size is too small, can't determine appropriate friendships. exiting.");
				System.exit(0);
			}
			//verify load thread count and fragment size
			if(numFriendsPerUser > tUserCount/numLoadThread){
				System.out.println("Could not have loaded with "+numLoadThread+" so considering 1 thread as number of load threads for deterministic");
				numLoadThread = 1;
			} 
			
			for( int i = 0; i < tUserCount; i++ )
			{
				if(confPerc == 1){
					initStats.put("PENDFRND-"+myMemberObjs[i].get_uid(), 0);
					initStats.put("ACCEPTFRND-"+myMemberObjs[i].get_uid(), numFriendsPerUser);
				}else{
					initStats.put("PENDFRND-"+myMemberObjs[i].get_uid(), numFriendsPerUser/2);
					initStats.put("ACCEPTFRND-"+myMemberObjs[i].get_uid(), 0);
				}
				//find the resources created for each user
				for(int j = 0; j < numResourcesPerUser; j++)
				{
					initStats.put("POSTCOMMENT-"+(myMemberObjs[i].get_uid()*numResourcesPerUser +j), 0);
					createdResources[i].add(myMemberObjs[i].get_uid()*numResourcesPerUser +j);
				}
				
			}
			
			//create the actual friendship or pending relationships
			int numUsersPerLoadThread = tUserCount/numLoadThread;
			int remaining = tUserCount - (numUsersPerLoadThread*numLoadThread);
			int addUserCnt = 0;
			for(int i = 0; i < numLoadThread; i++)
			{
				if(i == numLoadThread -1)
					addUserCnt = remaining;
				for(int u = i*numUsersPerLoadThread; u<numUsersPerLoadThread+i*numUsersPerLoadThread+addUserCnt; u++){
					int uidx = u;
					int ukey = myMemberObjs[uidx].get_uid();
					//TODO : the check for thread count and number of friends per user should be considered
					for(int j=0; j<numFriendsPerUser/2; j++){
						int fidx = i*numUsersPerLoadThread+((u-i*numUsersPerLoadThread)+j+1)%(numUsersPerLoadThread+addUserCnt);
						int fkey = myMemberObjs[fidx].get_uid();
						relateUsers(ukey, fkey);
						if(confPerc == 1){
							acceptedFrnds[uidx].put(fkey, null);
							acceptedFrnds[fidx].put(ukey, null);
						}else{
							pendingFrnds[fidx].add(ukey);
						}
					}
				}
			}
			
		}
		/*//Create file 
		FileWriter fstream = null;
		try {
			fstream = new FileWriter("outq"+machineid+".txt");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		  BufferedWriter out = new BufferedWriter(fstream);
		  
		Set keys = initStats.keySet();
		Iterator it = keys.iterator();
		while(it.hasNext()){
			String key = (String)(it.next());
			System.out.println(key+" "+initStats.get(key));
			try {
				out.write(key+" "+initStats.get(key)+"\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	
		for(int i=0; i<pendingFrnds.length; i++){
			System.out.println(myMemberObjs[i].get_uid()+" "+pendingFrnds[i]);
			try {
				out.write(myMemberObjs[i].get_uid()+" "+pendingFrnds[i]+"\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		for(int i=0; i<acceptedFrnds.length; i++){
			//System.out.println(myMemberObjs[i].get_uid()+" "+acceptedFrnds[i]);
			try {
				out.write(myMemberObjs[i].get_uid()+" "+acceptedFrnds[i]+"\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		  for(int i=0; i<createdResources.length; i++){
				System.out.println(myMemberObjs[i].get_uid()+" "+createdResources[i]);
				try {
					out.write(myMemberObjs[i].get_uid()+" "+createdResources[i]+"\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		try {
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.exit(0);*/
	}

	/**
	 * Do one transaction operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. 
	 * returns the number of actions done
	 */
	public int doTransaction(DB db, Object threadstate, int threadid,  StringBuilder updateLog, StringBuilder readLog,  int seqID, HashMap<String, Integer> resUpdateOperations
			, HashMap<String, Integer> friendshipInfo, HashMap<String, Integer> pendingInfo, int thinkTime, boolean insertImage, boolean warmup)
	{
		String op=operationchooser.nextString();
		int opsDone = 0;

		if (op.compareTo("OWNPROFILE")==0)
		{
			opsDone = doTransactionOwnProfile(db, threadid, updateLog, readLog ,seqID, thinkTime, insertImage,  warmup);
		}
		else if (op.compareTo("FRIENDPROFILE")==0)
		{
			opsDone = doTransactionFriendProfile(db, threadid, updateLog, readLog,seqID, thinkTime,  insertImage, warmup);
		}
		else if (op.compareTo("POSTCOMMENT")==0)
		{
			opsDone = doTransactionPostCommentOnResource(db,threadid, updateLog, readLog,seqID, resUpdateOperations, thinkTime, insertImage, warmup);
		}
		else if (op.compareTo("DELCOMMENT")==0)
		{
			opsDone = doTransactionDeleteCommentOnResource(db,threadid, updateLog, readLog,seqID, resUpdateOperations, thinkTime, insertImage, warmup);
		}
		else if (op.compareTo("ACCEPTREQ")==0)
		{
			opsDone = doTransactionAcceptFriendship(db,threadid,  updateLog, readLog,seqID, friendshipInfo, pendingInfo, thinkTime, insertImage, warmup);
		}
		else if (op.compareTo("REJECTREQ") == 0)
		{
			opsDone = doTransactionRejectFriendship(db,threadid,  updateLog,readLog ,seqID, friendshipInfo, pendingInfo, thinkTime, insertImage, warmup);
		}
		else if (op.compareTo("UNFRNDACCEPTREQ") == 0)
		{
			opsDone = doTransactionUnfriendPendingFriendship(db,threadid, updateLog,readLog, seqID, friendshipInfo, pendingInfo, thinkTime, "ACCEPT", insertImage, warmup);
		}
		else if (op.compareTo("UNFRNDREJECTREQ") == 0)
		{
			opsDone = doTransactionUnfriendPendingFriendship(db,threadid, updateLog, readLog,seqID, friendshipInfo, pendingInfo, thinkTime, "REJECT", insertImage, warmup);
		}
		else if (op.compareTo("UNFRNDREQ") == 0)
		{
			opsDone = doTransactionUnfriendFriendship(db,threadid,  updateLog, readLog,seqID, friendshipInfo, pendingInfo, thinkTime, insertImage, warmup);
		}
		else if (op.compareTo("GENFRNDREQ") == 0)
		{
			opsDone = doTransactionGenerateFriendship(db,threadid,  updateLog, readLog ,seqID, friendshipInfo, pendingInfo, thinkTime, insertImage, warmup);
		}
		//actions
		else if (op.compareTo("GETPROACT") == 0)
		{
			opsDone = doActionGetProfile(db, threadid, updateLog, readLog ,seqID, insertImage, warmup);
		}
		else if (op.compareTo("GETFRNDLSTACT") == 0)
		{
			opsDone = doActionGetFriends(db,threadid, updateLog,readLog,seqID, insertImage, warmup);
		}
		else if (op.compareTo("GETPENDACT") == 0)
		{
			opsDone = doActionGetPendings(db,threadid, updateLog,readLog,seqID, insertImage, warmup);
		}
		else if (op.compareTo("INVFRNDACT") == 0)
		{
			opsDone = doActioninviteFriend(db,threadid, updateLog,readLog, seqID, friendshipInfo, pendingInfo, insertImage, warmup);
		}
		else if (op.compareTo("ACCFRNDACT") == 0)
		{
			opsDone = doActionAcceptFriends(db, threadid, updateLog,readLog, seqID, friendshipInfo,pendingInfo, thinkTime,  insertImage,warmup);
		}
		else if (op.compareTo("REJFRNDACT") == 0)
		{
			opsDone = doActionRejectFriends(db, threadid, updateLog,readLog, seqID, friendshipInfo,pendingInfo, thinkTime,  insertImage,warmup);
		}
		else if (op.compareTo("UNFRNDACT") == 0)
		{
			opsDone = doActionUnFriendFriends(db, threadid,updateLog,readLog, seqID, friendshipInfo
					,pendingInfo, thinkTime,  insertImage, warmup);
		}
		else if (op.compareTo("GETRESACT") == 0)
		{
			opsDone = doActionGetTopResources(db, threadid, updateLog, readLog ,seqID, insertImage,  warmup);
		}
		else if (op.compareTo("GETCMTACT") == 0)
		{
			opsDone = doActionviewCommentOnResource(db, threadid, updateLog, readLog ,seqID, thinkTime, insertImage,  warmup);
		}
		else if (op.compareTo("POSTCMTACT") == 0)
		{
			opsDone = doActionPostComments(db,threadid, updateLog, readLog,seqID, resUpdateOperations,thinkTime, insertImage,  warmup);
		}
		else if (op.compareTo("DELCMTACT") == 0)
		{
			opsDone = doActionDelComments(db,threadid, updateLog, readLog,seqID, resUpdateOperations,thinkTime, insertImage,  warmup);
		}

		return opsDone;
	}

	public int buildKeyName(int keynum) {
		int key =0;
		if(requestdistrib.compareTo("dzipfian")==0){
			int idx = myDist.GenerateOneItem()-1;
			key = myMemberObjs[idx].get_uid();
		}else
			key = keychooser.nextInt()+useroffset;

		return key;
	}


	public int doTransactionOwnProfile(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, int thinkTime, boolean insertImage,  boolean warmup)
	{			
		int numOpsDone =0;
		int keyname = buildKeyName(usercount);
		//activate the user so no one else can grab it
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		//update frequency of access for the picked user
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.viewProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}	
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("friendcount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+"\n");
			readsExist = true;
		}
		
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.viewTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;	
		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doTransactionFriendProfile(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, int thinkTime,  boolean insertImage, boolean warmup)
	{	
		int numOpsDone=0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.viewProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("friendcount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();
		ret = db.viewTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.listFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in listFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(fResult.size() > 0){
			Random random = new Random();
			int idx = random.nextInt(fResult.size());
			HashMap<String,ByteIterator> fpResult=new HashMap<String,ByteIterator>();
			int friendId = -1;
			friendId = Integer.parseInt(fResult.get(idx).get("userid").toString());
			startReadf = System.nanoTime();
			ret = db.viewProfile(keyname, friendId, fpResult, insertImage, false);
			if(ret < 0){
				System.out.println("There is an exception in getProfile.");
				System.exit(0);
			}
			endReadf = System.nanoTime();
			numOpsDone++;
			if(!warmup && enableLogging){
				readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+friendId+","+startReadf+","+endReadf+","+fpResult.get("friendcount")+"\n");
				//this if should never be true
				if(keyname == friendId){
					readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+friendId+","+startReadf+","+endReadf+","+fpResult.get("pendingcount")+"\n");
				}
				readsExist = true;
			}
			try {
				Thread.sleep(thinkTime);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
			rResult=new Vector<HashMap<String,ByteIterator>>();		
			ret = db.viewTopKResources(keyname, friendId, 5, rResult);
			if(ret < 0){
				System.out.println("There is an exception in getTopResource.");
				System.exit(0);
			}
			numOpsDone++;
		}	

		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doTransactionPostCommentOnResource(DB db,int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, HashMap<String, Integer> resUpdateOperations, int thinkTime, boolean insertImage,  boolean warmup)
	{	
		int numOpsDone=0;
		int commentor = buildKeyName(usercount);
		commentor = activateUser(commentor);
		if(commentor == -1)
			return 0;
		incrUserRef(commentor);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.viewProfile(commentor, commentor, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+commentor+","+startReadp+","+endReadp+","+pResult.get("friendcount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+commentor+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.viewTopKResources(commentor, commentor, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		
		int keyname = buildKeyName(usercount);
		
		pResult=new HashMap<String,ByteIterator>();
		startReadp = System.nanoTime();
		ret = db.viewProfile(commentor, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("friendcount")+"\n");
			if(keyname == commentor){
				readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+"\n");
			}
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		rResult=new Vector<HashMap<String,ByteIterator>>();
		ret = db.viewTopKResources(commentor, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(rResult.size() > 0){
			Random random = new Random();
			int idx = random.nextInt(rResult.size());
			Vector<HashMap<String,ByteIterator>> cResult=new Vector<HashMap<String,ByteIterator>>();
			String resourceID = "";
			String ownerID ="";
			resourceID = rResult.get(idx).get("rid").toString();
			ownerID= rResult.get(idx).get("creatorid").toString();
			long startRead1 = System.nanoTime();
			ret = db.viewCommentOnResource(commentor, keyname, Integer.parseInt(resourceID), cResult);

			if(ret < 0){
				System.out.println("There is an exception in getResourceComment.");
				System.exit(0);
			}
			long endRead1 = System.nanoTime();
			numOpsDone++;
			if(!warmup && enableLogging){
				readLog.append("READ,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startRead1+","+endRead1+","+cResult.size()+"\n");
				readsExist = true;
			}
			try {
				Thread.sleep(thinkTime);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
			if(!warmup){
				HashMap<String,ByteIterator> commentValues = new HashMap<String, ByteIterator>();
				createCommentAttrs(commentValues);
				try {
					sCmts.acquire();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				int mid = maxCommentIds.get(Integer.parseInt(resourceID))+1;
				maxCommentIds.put(Integer.parseInt(resourceID), mid);
				sCmts.release();
				commentValues.put("mid", new ObjectByteIterator(Integer.toString(mid).getBytes()));
				long startUpdate = System.nanoTime();
				ret =db.postCommentOnResource(commentor, Integer.parseInt(ownerID), Integer.parseInt(resourceID), commentValues);
				if(ret < 0){
					System.out.println("There is an exception in postComment.");
					System.exit(0);
				}
				long endUpdate = System.nanoTime();
				//if I add it before the update , a delete may delete it without it actually being in the database
				//resulting in wrong results
				postedComments.get(Integer.parseInt(resourceID)).add(mid);	
				numOpsDone++;
				int numUpdatesTillNow = 0;
				if(resUpdateOperations.get(resourceID)!= null){
					numUpdatesTillNow = resUpdateOperations.get(resourceID);
				}
				resUpdateOperations.put(resourceID, (numUpdatesTillNow+1));
				if(enableLogging){
					updateLog.append("UPDATE,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startUpdate+","+endUpdate+","+(numUpdatesTillNow+1)+",I"+"\n");
					updatesExist = true;
				}
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
			}
			long startRead2 = System.nanoTime();
			cResult=new Vector<HashMap<String,ByteIterator>>();
			ret = db.viewCommentOnResource(commentor, keyname, Integer.parseInt(resourceID), cResult);
			if(ret < 0){
				System.out.println("There is an exception in viewCommentOnResource.");
				System.exit(0);
			}
			long endRead2 = System.nanoTime();
			numOpsDone++;
			if(!warmup && enableLogging){
				readLog.append("READ,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startRead2+","+endRead2+","+cResult.size()+"\n");
				readsExist = true;
			}
		}	
		deactivateUser(commentor);
		return numOpsDone;
	}
	
	
	public int doTransactionDeleteCommentOnResource(DB db,int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, HashMap<String, Integer> resUpdateOperations, int thinkTime, boolean insertImage,  boolean warmup)
	{	//deletes a comment posted on a resource on your wall
		int numOpsDone=0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.viewProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("friendcount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+"\n");
			readsExist = true;
		}

		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();
		ret = db.viewTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(rResult.size() > 0){
			Random random = new Random();
			int idx = random.nextInt(rResult.size());
			Vector<HashMap<String,ByteIterator>> cResult=new Vector<HashMap<String,ByteIterator>>();
			String resourceID = "";
			String ownerID ="";
			resourceID = rResult.get(idx).get("rid").toString();
			ownerID= rResult.get(idx).get("creatorid").toString();
			cResult=new Vector<HashMap<String,ByteIterator>>();
			long startRead1 = System.nanoTime();
			ret = db.viewCommentOnResource(keyname, keyname, Integer.parseInt(resourceID), cResult);
			if(ret < 0){
				System.out.println("There is an exception in getResourceComment.");
				System.exit(0);
			}
			long endRead1 = System.nanoTime();
			numOpsDone++;
			if(!warmup && enableLogging){
				readLog.append("READ,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startRead1+","+endRead1+","+cResult.size()+"\n");
				readsExist = true;
			}
			try {
				Thread.sleep(thinkTime);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
			//if( cResult.size()>0){
			try {
				sCmts.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(postedComments.get(Integer.parseInt(resourceID)) != null && postedComments.get(Integer.parseInt(resourceID)).size()>0 ){
				if(!warmup){
					int midx = random.nextInt(postedComments.get(Integer.parseInt(resourceID)).size());
					int mid = postedComments.get(Integer.parseInt(resourceID)).get(midx);
					postedComments.get(Integer.parseInt(resourceID)).remove(midx);
					sCmts.release();
					long startUpdate = System.nanoTime();
					ret =db.delCommentOnResource(Integer.parseInt(ownerID), Integer.parseInt(resourceID), mid);
					if(ret < 0){
						System.out.println("There is an exception in postComment.");
						System.exit(0);
					}
					long endUpdate = System.nanoTime();
					numOpsDone++;
					int numUpdatesTillNow = 0;
					if(resUpdateOperations.get(resourceID)!= null){
						numUpdatesTillNow = resUpdateOperations.get(resourceID);
					}
					resUpdateOperations.put(resourceID, (numUpdatesTillNow-1));
					if(enableLogging){
						updateLog.append("UPDATE,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startUpdate+","+endUpdate+","+(numUpdatesTillNow+1)+",D"+"\n");
						updatesExist = true;
					}
					try {
						Thread.sleep(thinkTime);
					} catch (InterruptedException e) {
						e.printStackTrace(System.out);
					}
				}else
					sCmts.release();
				long startRead2 = System.nanoTime();
				cResult=new Vector<HashMap<String,ByteIterator>>();
				ret = db.viewCommentOnResource(keyname, keyname, Integer.parseInt(resourceID), cResult);
				if(ret < 0){
					System.out.println("There is an exception in viewCommentOnResource.");
					System.exit(0);
				}
				long endRead2 = System.nanoTime();
				numOpsDone++;
				if(!warmup && enableLogging){
					readLog.append("READ,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startRead2+","+endRead2+","+cResult.size()+"\n");
					readsExist = true;
				}
			}else
				sCmts.release();
		}	
		deactivateUser(keyname);
		return numOpsDone;
	}


	public int doTransactionGenerateFriendship(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,  boolean insertImage, boolean warmup)
	{
		int numOpsDone=0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.viewProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("friendcount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.viewTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.listFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in listFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		int noRelId = -1; 
		noRelId = viewNotRelatedUsers(keyname);
		//if A invited B, B should not be able to invite A else with reject and invite again you will have integrity constraints
		if(noRelId!= -1 && isActive(noRelId) == -1){
			deactivateUser(keyname);
			return numOpsDone;
		}
		if(!warmup){ //so two people wouldnot invite each other at the same time
			if(noRelId == -1){
				//do nothing
			}else{
				long startUpdatei = System.nanoTime();
				ret = db.inviteFriend(keyname, noRelId);
				if(ret < 0){
					System.out.println("There is an exception in invFriends.");
					System.exit(0);
				}
				pendingFrnds[memberIdxs.get(noRelId)].add(keyname);
				int numPendingsForOtherUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(noRelId))!= null){
					numPendingsForOtherUserTillNow = pendingInfo.get(Integer.toString(noRelId));
				}
				pendingInfo.put(Integer.toString(noRelId), (numPendingsForOtherUserTillNow+1));
				long endUpdatei = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+noRelId+","+startUpdatei+","+endUpdatei+","+(numPendingsForOtherUserTillNow+1)+",I"+"\n");
					updatesExist = true;
				}
				relateUsers(keyname, noRelId );
				deactivateUser(noRelId);
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
				
				Vector<HashMap<String,ByteIterator>> peResult=new Vector<HashMap<String,ByteIterator>>();
				startReadp = System.nanoTime();
				ret = db.viewFriendReq(keyname, peResult, insertImage, false);
				if(ret < 0){
					System.out.println("There is an exception in viewPendingFriends.");
					System.exit(0);
				}
				endReadp = System.nanoTime();
				numOpsDone++;
				if(!warmup && enableLogging){
					readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
					readsExist = true;
				}
			}	
		}

		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doTransactionAcceptFriendship(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,  boolean insertImage, boolean warmup)
	{		
		int numOpsDone =0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.viewProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("friendcount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.viewTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.listFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in listFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		Vector<HashMap<String,ByteIterator>> peResult=new Vector<HashMap<String,ByteIterator>>();
		startReadp = System.nanoTime();
		ret = db.viewFriendReq(keyname, peResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewFriendReq.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		if(!warmup){
			/*if(peResult.size() == 0){
				//do nothing*/
			Vector<Integer> ids =pendingFrnds[memberIdxs.get(keyname)];
			if(ids.size() <= 0){
				//do nothing
			}else{
				Random random = new Random();
				int idx = random.nextInt(ids.size());
				//int idx = random.nextInt(peResult.size());
				long startUpdatea = System.nanoTime();
				String auserid = "";
				//auserid =peResult.get(idx).get("userid").toString();
				auserid = ids.get(idx).toString();
				ret = db.acceptFriend(Integer.parseInt(auserid), keyname);
				if(ret < 0){
					System.out.println("There is an exception in acceptFriends.");
					System.exit(0);
				}
				ids.remove(idx);
				try {
					aFrnds.acquire();
					acceptedFrnds[memberIdxs.get(Integer.parseInt(auserid))].put(keyname,""); 
					acceptedFrnds[memberIdxs.get(keyname)].put(Integer.parseInt(auserid),"");
					aFrnds.release();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				int numFriendsForThisUserTillNow = 0;
				if(friendshipInfo.get(Integer.toString(keyname))!= null){
					numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
				}
				friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow+1));

				int numFriendsForOtherUserTillNow = 0;
				if(friendshipInfo.get(auserid)!= null){
					numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
				}
				friendshipInfo.put(auserid, (numFriendsForOtherUserTillNow+1));
				int numPendingsForThisUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(keyname))!= null){
					numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
				}
				pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
				long endUpdatea = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdatea+","+endUpdatea+","+(numFriendsForOtherUserTillNow+1)+",I"+"\n");
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numFriendsForThisUserTillNow+1)+",I"+"\n");
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
					updatesExist = true;
				}
				relateUsers(keyname, Integer.parseInt(auserid));
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
				fResult=new Vector<HashMap<String,ByteIterator>>();
				startReadf = System.nanoTime();
				ret = db.listFriends(keyname, keyname, null, fResult,  insertImage, false);
				if(ret < 0){
					System.out.println("There is an exception in listFriends.");
					System.exit(0);
				}
				endReadf = System.nanoTime();
				numOpsDone++;
				if(!warmup && enableLogging){
					readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
					readsExist = true;
				}
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
				peResult=new Vector<HashMap<String,ByteIterator>>();
				startReadp = System.nanoTime();
				ret = db.viewFriendReq(keyname, peResult,  insertImage, false);
				if(ret < 0){
					System.out.println("There is an exception in viewFriendReq.");
					System.exit(0);
				}
				endReadp = System.nanoTime();
				numOpsDone++;

				if(!warmup && enableLogging){
					readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
					readsExist = true;
				}
			}
		}

		
		deactivateUser(keyname);
		return numOpsDone;

	}

	public int doTransactionRejectFriendship(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,  boolean insertImage, boolean warmup)
	{		
		int numOpsDone =0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.viewProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("friendcount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.viewTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.listFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in listFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> peResult=new Vector<HashMap<String,ByteIterator>>();
		startReadp = System.nanoTime();
		ret = db.viewFriendReq(keyname, peResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewFriendReq.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(!warmup){
			/*if(peResult.size() == 0){
				//do nothing*/
			Vector<Integer> ids =pendingFrnds[memberIdxs.get(keyname)];
			if(ids.size() <= 0){
				//do nothing
			}else{
				Random random = new Random();
				int idx = random.nextInt(ids.size());
				//int idx = random.nextInt(peResult.size());
				String auserid = "";
				//auserid =peResult.get(idx).get("userid").toString();
				auserid = ids.get(idx).toString();
				long startUpdatea = System.nanoTime();
				ret = db.rejectFriend(Integer.parseInt(auserid), keyname);
				if(ret < 0){
					System.out.println("There is an exception in rejectFriend.");
					System.exit(0);
				}
				ids.remove(idx);
				int numPendingsForThisUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(keyname))!= null){
					numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
				}
				pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
				long endUpdatea = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
					updatesExist = true;
				}
				deRelateUsers(keyname, Integer.parseInt(auserid) );
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
				fResult=new Vector<HashMap<String,ByteIterator>>();
				startReadf = System.nanoTime();
				ret = db.listFriends(keyname, keyname, null, fResult,  insertImage, false);
				if(ret < 0){
					System.out.println("There is an exception in listFriends.");
					System.exit(0);
				}
				endReadf = System.nanoTime();
				numOpsDone++;
				if(!warmup && enableLogging){
					readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
					readsExist = true;
				}
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}

				peResult=new Vector<HashMap<String,ByteIterator>>();
				startReadp = System.nanoTime();
				ret = db.viewFriendReq(keyname, peResult, insertImage, false);
				if(ret < 0){
					System.out.println("There is an exception in viewFriendReq.");
					System.exit(0);
				}
				endReadp = System.nanoTime();
				numOpsDone++;
				if(!warmup && enableLogging){
					readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
					readsExist = true;
				}
			}
		}

		
		deactivateUser(keyname);
		return numOpsDone;

	}

	public int doTransactionUnfriendFriendship(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,   boolean insertImage, boolean warmup)
	{	
		int numOpsDone =0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.viewProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("friendcount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+"\n");
			readsExist = true;
		}
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.viewTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResources.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.listFriends(keyname, keyname, null, fResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in listFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		if(!warmup){
			/*if(fResult.size() == 0){
				//do nothing*/
			try {
				aFrnds.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
			if(acceptedFrnds[memberIdxs.get(keyname)].size() <= 0 ){
				aFrnds.release();
			}else{
				/*Random random = new Random();
				int idx = random.nextInt(fResult.size());
				long startUpdater = System.nanoTime();
				String auserid = "";
				auserid =fResult.get(idx).get("userid").toString();
				*/
				String auserid = "";	
				Set<Integer> keys = acceptedFrnds[memberIdxs.get(keyname)].keySet();
				Iterator<Integer> it = keys.iterator();
				auserid = it.next().toString();	
				if(isActive(Integer.parseInt(auserid)) != -1){ //so two people won't delete the same friendhsip at the same time			
					//remove from acceptedFrnds
					acceptedFrnds[memberIdxs.get(keyname)].remove(Integer.parseInt(auserid));
					acceptedFrnds[memberIdxs.get(Integer.parseInt(auserid))].remove(keyname);
					aFrnds.release();
					long startUpdater = System.nanoTime();
					ret = db.thawFriendship(Integer.parseInt(auserid), keyname);
					if(ret < 0){
						System.out.println("There is an exception in unFriendFriend.");
						System.exit(0);
					}
					int numFriendsForThisUserTillNow = 0;
					if(friendshipInfo.get(Integer.toString(keyname))!= null){
						numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
					}
					friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow-1));

					int numFriendsForOtherUserTillNow = 0;
					if(friendshipInfo.get(auserid)!= null){
						numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
					}
					friendshipInfo.put(auserid, (numFriendsForOtherUserTillNow-1));
					long endUpdater = System.nanoTime();
					numOpsDone++;

					if(enableLogging){
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdater+","+endUpdater+","+(numFriendsForOtherUserTillNow-1)+",D"+"\n");
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdater+","+endUpdater+","+(numFriendsForThisUserTillNow-1)+",D"+"\n");
						updatesExist = true;
					}
					deRelateUsers(keyname, Integer.parseInt(auserid));
					deactivateUser(Integer.parseInt(auserid));

					try {
						Thread.sleep(thinkTime);
					} catch (InterruptedException e) {
						e.printStackTrace(System.out);
					}
				}
				fResult=new Vector<HashMap<String,ByteIterator>>();
				startReadf = System.nanoTime();
				ret = db.listFriends(keyname, keyname, null, fResult,  insertImage, false);
				if(ret < 0){
					System.out.println("There is an exception in listFriends.");
					System.exit(0);
				}
				endReadf = System.nanoTime();
				numOpsDone++;
				if(!warmup && enableLogging){
					readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
					readsExist = true;
				}
			}
		}
		

		deactivateUser(keyname);
		return numOpsDone;

	}


	public int doTransactionUnfriendPendingFriendship(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime, String nextOp,  boolean insertImage, boolean warmup)
	{	
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.viewProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("friendcount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+"\n");
			readsExist = true;
		}
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.viewTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		if(!warmup){
			int noRelId = -1;
			noRelId = viewNotRelatedUsers(keyname);
			if(noRelId == -1){
				//do nothing
			}else{
				if(isActive(noRelId) != -1){
					long startUpdatei = System.nanoTime();
					ret = db.inviteFriend(keyname, noRelId);	
					if(ret < 0){
						System.out.println("There is an exception in inviteFriend.");
						System.exit(0);
					}
					int numPendingsForOtherUserTillNow = 0;
					if(pendingInfo.get(Integer.toString(noRelId))!= null){
						numPendingsForOtherUserTillNow = pendingInfo.get(Integer.toString(noRelId));
					}
					pendingInfo.put(Integer.toString(noRelId), (numPendingsForOtherUserTillNow+1));
					long endUpdatei = System.nanoTime();
					numOpsDone++;
					if(enableLogging){
						updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+noRelId+","+startUpdatei+","+endUpdatei+","+(numPendingsForOtherUserTillNow+1)+",I"+"\n");
						updatesExist = true;
					}
					relateUsers(keyname, noRelId);
					deactivateUser(noRelId);
					try {
						Thread.sleep(thinkTime);
					} catch (InterruptedException e) {
						e.printStackTrace(System.out);
					}
				}
			}
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.listFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in listFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist= true;
		}
		if(!warmup){
			if(fResult.size() == 0){
				//do nothing
			}else{
				Random random = new Random();
				int idx = random.nextInt(fResult.size());
				long startUpdater = System.nanoTime();
				String auserid = "";
				auserid =fResult.get(idx).get("userid").toString();

				if(isActive(Integer.parseInt(auserid)) != -1){			

					ret = db.thawFriendship(Integer.parseInt(auserid), keyname);
					if(ret < 0){
						System.out.println("There is an exception in unFriendFriend.");
						System.exit(0);
					}
					int numFriendsForThisUserTillNow = 0;
					if(friendshipInfo.get(Integer.toString(keyname))!= null){
						numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
					}
					friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow-1));


					int numFriendsForOtherUserTillNow = 0;
					if(friendshipInfo.get(auserid)!= null){
						numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
					}
					friendshipInfo.put(auserid, (numFriendsForOtherUserTillNow-1));
					long endUpdater = System.nanoTime();
					numOpsDone++;
					if(enableLogging){
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdater+","+endUpdater+","+(numFriendsForOtherUserTillNow-1)+",D"+"\n");
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdater+","+endUpdater+","+(numFriendsForThisUserTillNow-1)+",D"+"\n");
						updatesExist = true;
					}
					deRelateUsers(keyname, Integer.parseInt(auserid));
					deactivateUser(Integer.parseInt(auserid));
					try {
						Thread.sleep(thinkTime);
					} catch (InterruptedException e) {
						e.printStackTrace(System.out);
					}
				}
			}
		}
		fResult=new Vector<HashMap<String,ByteIterator>>();
		startReadf = System.nanoTime();
		ret = db.listFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in listFriends.");
			System.exit(0);
		}
		endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		Vector<HashMap<String,ByteIterator>> peResult=new Vector<HashMap<String,ByteIterator>>();
		startReadp = System.nanoTime();
		ret = db.viewFriendReq(keyname, peResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewFriendReq.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(!warmup){
			if(peResult.size() == 0){
				//do nothing
			}else{
				Random random = new Random();
				int idx = random.nextInt(peResult.size());
				long startUpdatea = System.nanoTime();
				if(nextOp.equals("ACCEPT")){
					String auserid = "";
					auserid =peResult.get(idx).get("userid").toString();
					ret = db.acceptFriend(Integer.parseInt(auserid), keyname);
					if(ret < 0){
						System.out.println("There is an exception in acceptFriends.");
						System.exit(0);
					}
					int numFriendsForThisUserTillNow = 0;
					if(friendshipInfo.get(Integer.toString(keyname))!= null){
						numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
					}
					friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow+1));


					int numFriendsForOtherUserTillNow = 0;
					if(friendshipInfo.get(auserid)!= null){
						numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
					}
					friendshipInfo.put(auserid, (numFriendsForOtherUserTillNow+1));
					int numPendingsForThisUserTillNow = 0;
					if(pendingInfo.get(Integer.toString(keyname))!= null){
						numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
					}
					pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
					long endUpdatea = System.nanoTime();
					numOpsDone++;
					if(enableLogging){
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdatea+","+endUpdatea+","+(numFriendsForOtherUserTillNow+1)+",I"+"\n");
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numFriendsForThisUserTillNow+1)+",I"+"\n");
						updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
						updatesExist = true;
					}
					relateUsers(keyname, Integer.parseInt(auserid));
				}else if(nextOp.equals("REJECT")){
					String auserid = "";
					auserid =peResult.get(idx).get("userid").toString();
					ret = db.rejectFriend(Integer.parseInt(auserid), keyname);
					if(ret < 0){
						System.out.println("There is an exception in rejectFriend.");
						System.exit(0);
					}
					int numPendingsForThisUserTillNow = 0;
					if(pendingInfo.get(Integer.toString(keyname))!= null){
						numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
					}
					pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
					long endUpdatea = System.nanoTime();
					numOpsDone++;
					if(enableLogging){
						updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
						updatesExist = true;
					}
					deRelateUsers(keyname, Integer.parseInt(auserid) );
				}
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
			}
		}

		fResult=new Vector<HashMap<String,ByteIterator>>();
		startReadf = System.nanoTime();
		ret = db.listFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in listFriends.");
			System.exit(0);
		}
		endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		peResult=new Vector<HashMap<String,ByteIterator>>();
		startReadp = System.nanoTime();
		ret = db.viewFriendReq(keyname, peResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewFriendReq.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}

		deactivateUser(keyname);
		return numOpsDone;
	}


	public int doActionGetProfile(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, boolean insertImage,  boolean warmup)
	{		
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		int profilekeyname = buildKeyName(usercount);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.viewProfile(keyname, profilekeyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+profilekeyname+","+startReadp+","+endReadp+","+pResult.get("friendcount")+"\n");
			if(keyname == profilekeyname)
				readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+profilekeyname+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+"\n");
			readsExist = true;
		}
		deactivateUser(keyname);
		return numOpsDone;
	}


	public int doActionGetFriends(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		int profilekeyname = buildKeyName(usercount);
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		int ret = db.listFriends(keyname, profilekeyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in listFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+profilekeyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}

		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doActionGetPendings(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		Vector<HashMap<String,ByteIterator>> pResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		int ret = db.viewFriendReq(keyname,pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewFriendReq.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+pResult.size()+"\n");
			readsExist = true;
		}
		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doActioninviteFriend(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		int noRelId = -1;
		noRelId = viewNotRelatedUsers(keyname);
		if(noRelId!= -1 && isActive(noRelId) == -1){ //not two people should invite each other at the same time
			deactivateUser(keyname);
			return numOpsDone;
		}
		if(!warmup){
			if(noRelId == -1){
				//do nothing
			}else{
				long startUpdatei = System.nanoTime();
				int ret = db.inviteFriend(keyname, noRelId);
				if(ret < 0){
					System.out.println("There is an exception in inviteFriend.");
					System.exit(0);
				}
				pendingFrnds[memberIdxs.get(noRelId)].add(keyname);
				int numPendingsForOtherUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(noRelId))!= null){
					numPendingsForOtherUserTillNow = pendingInfo.get(Integer.toString(noRelId));
				}
				pendingInfo.put(Integer.toString(noRelId), (numPendingsForOtherUserTillNow+1));
				long endUpdatei = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+noRelId+","+startUpdatei+","+endUpdatei+","+(numPendingsForOtherUserTillNow+1)+",I"+"\n");
					updatesExist= true;
				}
				relateUsers(keyname, noRelId );
				deactivateUser(noRelId);
			}	
		}
		deactivateUser(keyname);
		return numOpsDone;
	}


	public int doActionAcceptFriends(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,  boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname); 
		if(!warmup){
			int auserid = -1;
			Vector<Integer> ids =pendingFrnds[memberIdxs.get(keyname)];
			if(ids.size() > 0){
				auserid = ids.get(ids.size()-1);
				long startUpdatea = System.nanoTime();
				int ret = 0;
				ret = db.acceptFriend(auserid, keyname);
				if(ret < 0){
					System.out.println("There is an exception in acceptFriend.");
					System.exit(0);
				}
				//remove from the list because it has been accepted
				ids.remove(ids.size()-1);
				try {
					aFrnds.acquire();
					acceptedFrnds[memberIdxs.get(auserid)].put(keyname,""); 
					acceptedFrnds[memberIdxs.get(keyname)].put(auserid,"");
					aFrnds.release();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	
				int numFriendsForThisUserTillNow = 0;
				if(friendshipInfo.get(Integer.toString(keyname))!= null){
					numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
				}
				friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow+1));


				int numFriendsForOtherUserTillNow = 0;
				if(friendshipInfo.get(auserid)!= null){
					numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
				}
				friendshipInfo.put(Integer.toString(auserid), (numFriendsForOtherUserTillNow+1));
				
				int numPendingsForThisUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(keyname))!= null){
					numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
				}
				pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
				
				long endUpdatea = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdatea+","+endUpdatea+","+(numFriendsForOtherUserTillNow+1)+",I"+"\n");
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numFriendsForThisUserTillNow+1)+",I"+"\n");
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
					updatesExist = true;
				}
				relateUsers(keyname,auserid);
			}
		}
		deactivateUser(keyname);
		return numOpsDone;

	}

	public int doActionRejectFriends(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,  boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		if(!warmup){
			int auserid = -1;
			Vector<Integer> ids =pendingFrnds[memberIdxs.get(keyname)];
			if(ids.size() > 0){
				auserid = ids.get(ids.size()-1);
				int ret = 0;			
				long startUpdatea = System.nanoTime();
				ret = db.rejectFriend(auserid, keyname);
				if(ret < 0){
					System.out.println("There is an exception in rejectFriend.");
					System.exit(0);
				}
				//remove from the list coz it has been rejected
				ids.remove(ids.size()-1);
				
				int numPendingsForThisUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(keyname))!= null){
					numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
				}
				pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
				
				long endUpdatea = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
					updatesExist = true;
				}
				deRelateUsers(keyname, auserid );
			}
		}
		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doActionUnFriendFriends(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,   boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		if(!warmup){
			int ret = 0;
			try {
				aFrnds.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
			if(acceptedFrnds[memberIdxs.get(keyname)].size() > 0 ){
				int auserid = -1;	
				Set<Integer> keys = acceptedFrnds[memberIdxs.get(keyname)].keySet();
				Iterator<Integer> it = keys.iterator();
				auserid = it.next();	
				if(isActive(auserid) != -1){ //two members should not delete each other at the same time
					//remove from acceptedFrnds
					acceptedFrnds[memberIdxs.get(keyname)].remove(auserid);
					acceptedFrnds[memberIdxs.get(auserid)].remove(keyname);
					aFrnds.release();
					
					long startUpdater = System.nanoTime();
					ret = db.thawFriendship(auserid, keyname);
					if(ret < 0){
						System.out.println("There is an exception in unFriendFriend.");
						System.exit(0);
					}

					int numFriendsForThisUserTillNow = 0;
					if(friendshipInfo.get(Integer.toString(keyname))!= null){
						numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
					}
					friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow-1));


					int numFriendsForOtherUserTillNow = 0;
					if(friendshipInfo.get(auserid)!= null){
						numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
					}
					friendshipInfo.put(Integer.toString(auserid), (numFriendsForOtherUserTillNow-1));
					long endUpdater = System.nanoTime();
					numOpsDone++;
					if(enableLogging){
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdater+","+endUpdater+","+(numFriendsForOtherUserTillNow-1)+",D"+"\n");
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdater+","+endUpdater+","+(numFriendsForThisUserTillNow-1)+",D"+"\n");
						updatesExist = true;
					}
					deRelateUsers(keyname, auserid);
					deactivateUser(auserid);
				}else 
					aFrnds.release();
			}else
				aFrnds.release();
		}
		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doActionGetTopResources(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		int profilekeyname = buildKeyName(usercount);
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		int ret = db.viewTopKResources(keyname, profilekeyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doActionviewCommentOnResource(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, int thinkTime, boolean insertImage,  boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		int profilekeyname = buildKeyName(usercount);
		//get resources for profilekeyname
		Vector<Integer> profilekeynameresources = createdResources[memberIdxs.get(profilekeyname)];
		if(profilekeynameresources.size() > 0){
			Random random = new Random();
			int idx = random.nextInt(profilekeynameresources.size());
			Vector<HashMap<String,ByteIterator>> cResult=new Vector<HashMap<String,ByteIterator>>();
			String resourceID ="";
			resourceID = profilekeynameresources.get(idx).toString();
			long startRead = System.nanoTime();
			int ret = db.viewCommentOnResource(keyname, profilekeyname, Integer.parseInt(resourceID), cResult);
			if(ret < 0){
				System.out.println("There is an exception in getResourceComment."+resourceID);
				System.exit(0);
			}
			long endRead = System.nanoTime();
			numOpsDone++;
			if(!warmup && enableLogging){
				readLog.append("READ,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startRead+","+endRead+","+cResult.size()+"\n");
				readsExist = true;
			}		
		}	
		deactivateUser(keyname);
		return numOpsDone;

	}

	public int doActionPostComments(DB db,int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, HashMap<String, Integer> resUpdateOperations, int thinkTime, boolean insertImage, boolean warmup)
	{
		//posts a comment on one of her owned resources
		int numOpsDone = 0;
		int commentor = buildKeyName(usercount);
		commentor = activateUser(commentor);
		if(commentor == -1)
			return 0;
		incrUserRef(commentor);
		Vector<Integer> profilekeynameresources = createdResources[memberIdxs.get(commentor)];
		if(profilekeynameresources.size() > 0){
			Random random = new Random();
			int idx = random.nextInt(profilekeynameresources.size());
			Vector<HashMap<String,ByteIterator>> cResult=new Vector<HashMap<String,ByteIterator>>();
			String resourceID = "";
			resourceID = profilekeynameresources.get(idx).toString();
			if(!warmup){
				HashMap<String,ByteIterator> commentValues = new HashMap<String, ByteIterator>(); 
				createCommentAttrs(commentValues);
				try {
					//needed for when we have a mix of actions and sessions as other members can also post comments on any resource
					sCmts.acquire();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} 
				int mid = maxCommentIds.get(Integer.parseInt(resourceID))+1;
				maxCommentIds.put(Integer.parseInt(resourceID), mid);	
				sCmts.release();
				commentValues.put("mid", new ObjectByteIterator(Integer.toString(mid).getBytes()));
				long startUpdate = System.nanoTime();
				int ret =db.postCommentOnResource(commentor, commentor, Integer.parseInt(resourceID), commentValues); 
				if(ret < 0){
					System.out.println("There is an exception in postComment."+mid+" "+resourceID+" "+commentor+" "+postedComments.get(Integer.parseInt(resourceID)));
					System.exit(0);
				}
				long endUpdate = System.nanoTime();
				postedComments.get(Integer.parseInt(resourceID)).add(mid);
				numOpsDone++;
				int numUpdatesTillNow = 0;

				if(resUpdateOperations.get(resourceID)!= null){
					numUpdatesTillNow = resUpdateOperations.get(resourceID);
				}
				resUpdateOperations.put(resourceID, (numUpdatesTillNow+1));
				if(enableLogging){
					updateLog.append("UPDATE,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startUpdate+","+endUpdate+","+(numUpdatesTillNow+1)+",I"+"\n");
					updatesExist = true;
				}
			}
		}		
		deactivateUser(commentor);
		return numOpsDone;

	}	

	public int doActionDelComments(DB db,int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, HashMap<String, Integer> resUpdateOperations, int thinkTime, boolean insertImage, boolean warmup)
	{
		// a user can only delete a comment on her own resource
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		Vector<Integer> profilekeynameresources = createdResources[memberIdxs.get(keyname)];
		if(profilekeynameresources.size() > 0){
			Random random = new Random();
			int idx = random.nextInt(profilekeynameresources.size());
			Vector<HashMap<String,ByteIterator>> cResult=new Vector<HashMap<String,ByteIterator>>();
			String resourceID = "";
			resourceID = profilekeynameresources.get(idx).toString();
			if(!warmup){
				try {
					sCmts.acquire();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if(postedComments.get(Integer.parseInt(resourceID)) != null && postedComments.get(Integer.parseInt(resourceID)).size()>0 ){
					int midx = random.nextInt(postedComments.get(Integer.parseInt(resourceID)).size());
					int mid = postedComments.get(Integer.parseInt(resourceID)).get(midx);
					postedComments.get(Integer.parseInt(resourceID)).remove(midx);
					sCmts.release();
					long startUpdate = System.nanoTime();
					int ret =db.delCommentOnResource(keyname,Integer.parseInt(resourceID), mid); 
					if(ret < 0){
						System.out.println("There is an exception in delComment.");
						System.exit(0);
					}
					
					long endUpdate = System.nanoTime();
					numOpsDone++;
					int numUpdatesTillNow = 0;

					if(resUpdateOperations.get(resourceID)!= null){
						numUpdatesTillNow = resUpdateOperations.get(resourceID);
					}
					resUpdateOperations.put(resourceID, (numUpdatesTillNow-1));
					if(enableLogging){
						updateLog.append("UPDATE,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startUpdate+","+endUpdate+","+(numUpdatesTillNow-1)+",D"+"\n");
						updatesExist = true;
					}
				}else
					sCmts.release();
			}
		}
		deactivateUser(keyname);
		return numOpsDone;
	}

	@Override
	public boolean doInsert(DB db, Object threadstate) {
		return false;
	}


	public boolean isRelated(int uid1, int uid2){
		boolean related = false;
		try {
			rStat.acquire();
			if(userRelations[(memberIdxs.get(uid1))] != null){
				HashMap<Integer, String> rels = userRelations[memberIdxs.get(uid1)];
				if(rels.containsKey(uid2)){
					related= true;
				}else
					related = false;
			}else
				related = false;
			if(userRelations[memberIdxs.get(uid2)] != null){
				HashMap<Integer, String> rels = userRelations[memberIdxs.get(uid2)];
				if(rels.containsKey(uid1)){
					related = true;
				}else
					related = false;
			}else
				related = false;
			rStat.release();
		} catch (Exception e) {
			System.out.println("Error in Rels");
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return related;		
	}


	public void relateUsers(int uid1, int uid2){
		try {

			rStat.acquire();
			if(userRelations[memberIdxs.get(uid1)] != null){
				HashMap<Integer, String> rels = userRelations[memberIdxs.get(uid1)];
				if(!rels.containsKey(uid2)){
					rels.put(uid2,"");
				}
				userRelations[memberIdxs.get(uid1)]= rels;
			}else{
				HashMap<Integer, String> rels = new HashMap<Integer, String>();
				rels.put(uid2,"");
				userRelations[memberIdxs.get(uid1)]= rels;
			}
			
			if(userRelations[memberIdxs.get(uid2)] != null){
				HashMap<Integer, String> rels = userRelations[memberIdxs.get(uid2)];
				if(!rels.containsKey(uid1)){
					rels.put(uid1,"");
				}
				userRelations[memberIdxs.get(uid2)]= rels;
			}else{
				HashMap<Integer, String> rels = new HashMap<Integer, String>();
				rels.put(uid1,"");
				userRelations[memberIdxs.get(uid2)]= rels;	
			}

			rStat.release();
		} catch (Exception e) {
			System.out.println("Error in Rels");
			e.printStackTrace(System.out);
			System.exit(-1);
		}		
	}

	public void deRelateUsers(int uid1, int uid2){
		try {
			rStat.acquire();
			HashMap<Integer, String> rels = userRelations[memberIdxs.get(uid1)];
			if(rels.containsKey(uid2)){
				rels.remove(uid2);
			}
			userRelations[memberIdxs.get(uid1)] = rels;

			rels = userRelations[memberIdxs.get(uid2)];
			if(rels.containsKey(uid1)){
				rels.remove(uid1);
			}
			userRelations[memberIdxs.get(uid2)] = rels;
			rStat.release();
		} catch (Exception e) {
			System.out.println("Error in Rels");
			e.printStackTrace(System.out);
			System.exit(-1);
		}	
	}

	public int viewNotRelatedUsers(int uid){
		int key = -1;
		try{
			rStat.acquire();
			HashMap<Integer, String> rels = userRelations[memberIdxs.get(uid)];
			int id = buildKeyName(usercount) ;
			int idx = memberIdxs.get(id);
			//int idx = random.nextInt(usercount)+useroffset;
			for(int i=idx; i<idx+usercount; i++){
				if(!rels.containsKey(myMemberObjs[i%usercount].get_uid())){
					key = myMemberObjs[i%usercount].get_uid();
					break;
				}
			}
			if(key == -1)
				System.out.println("No more friends to allocate for  "+uid+" ; benchmark results invalid");
			rStat.release();	
		}catch(Exception e){
			System.out.println("Error in view not related");
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return key;
	}

	public int isActive(int uid){
		//if active return -1 
		//else return 0
		int actualIdx = memberIdxs.get(uid);
		int shardIdx = myMemberObjs[actualIdx].get_shardIdx();
		int idxInShard = myMemberObjs[actualIdx].get_idxInShard();

		try {
			uStatSemaphores[shardIdx].acquire();
			if (userStatusShards[shardIdx][idxInShard] == 'a'){
				//user is active
				uStatSemaphores[shardIdx].release();
				return -1;
			}else{
				//user is not active
				//activate it
				userStatusShards[shardIdx][idxInShard] = 'a';
				uStatSemaphores[shardIdx].release();
				return 0;
			}	
		} catch (Exception e) {
			System.out.println("Error-Cant activate any user");
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return -1;
	}

	public int activateUser(int uid)
	{
		try {
			int actualIdx = memberIdxs.get(uid);
			int shardIdx = myMemberObjs[actualIdx].get_shardIdx();
			int idxInShard = myMemberObjs[actualIdx].get_idxInShard();
			uStatSemaphores[shardIdx].acquire();
			int cnt =0; //needed for avoiding loops
			//int shardscnt = 0;
			//find a free member within this shard
			while (userStatusShards[shardIdx][idxInShard] != 'd'){
				if(cnt == userStatusShards[shardIdx].length){
					uStatSemaphores[shardIdx].release();
					/*shardscnt ++;
					if(shardscnt == numShards){ //went through all the shards once
						return -1;
					}
					shardIdx = (shardIdx+1)%numShards;
					cnt = 0;
					idxInShard = 0;
					actualIdx = numShards*idxInShard+shardIdx;
					uid = myMemberObjs[actualIdx].get_uid();
					uStatSemaphores[shardIdx].acquire();
					continue;*/
					return -1;
				}
				idxInShard = (idxInShard+1);
				idxInShard = idxInShard%userStatusShards[shardIdx].length;
				//map to actual idx
				actualIdx = numShards*idxInShard+shardIdx;
				uid = myMemberObjs[actualIdx].get_uid();
				cnt++;
			}
			userStatusShards[shardIdx][idxInShard] ='a';
			uStatSemaphores[shardIdx].release();
		} catch (Exception e) {
			System.out.println("Error-Cant activate any user");
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return uid;
	}


	public void deactivateUser(int uid)
	{
		int actualIdx = memberIdxs.get(uid);
		int shardIdx = myMemberObjs[actualIdx].get_shardIdx();
		int idxInShard = myMemberObjs[actualIdx].get_idxInShard();

		try {
			uStatSemaphores[shardIdx].acquire();
			if (userStatusShards[shardIdx][idxInShard] == 'd') {
				System.out.println("Error - The user is already deactivated");	
			}
			userStatusShards[shardIdx][idxInShard]='d'; //Mark as available
			uStatSemaphores[shardIdx].release();
		} catch (Exception e) {
			System.out.println("Error - couldnt deactivate user");
			e.printStackTrace(System.out);
			System.exit(-1);
		}

		return ;
	}

	public void incrUserRef(int uid)
	{
		int actualIdx = memberIdxs.get(uid);
		int shardIdx = myMemberObjs[actualIdx].get_shardIdx();
		int idxInShard = myMemberObjs[actualIdx].get_idxInShard();		
		try {
			uFreqSemaphores[shardIdx].acquire();
			userFreqShards[shardIdx][idxInShard]=userFreqShards[shardIdx][idxInShard]+1;  
			uFreqSemaphores[shardIdx].release();
		} catch (Exception e) {
			System.out.println("Error-Cant increament users frequency of access");
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return;
	}


	public void createCommentAttrs(HashMap<String,ByteIterator> commentValues){
		//insert random timestamp, type and content for the comment created
		String[] fieldName = {"timestamp", "type", "content"};
		for (int i = 1; i <= 3; ++i)
		{
			String fieldKey = fieldName[i-1];
			ByteIterator data;
			if(1 == i){
				Date date = new Date();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				String dateString = sdf.format(date);
				data = new ObjectByteIterator(dateString.getBytes()); // Timestamp.
			}else{
				data = new RandomByteIterator(100); // Other fields.
			}
			commentValues.put(fieldKey, data);
		}
		
	}

	@Override
	public HashMap<String, String> getDBInitialStats(DB db) {
		HashMap<String, String> stats = new HashMap<String, String>();
		stats = db.getInitialStats();
		return stats;
	}

	public static String getFrequecyStats(){
		String userFreqStats = "";
		//int sum = 0;
		for(int i=0; i<myMemberObjs.length; i++){
			userFreqStats+=myMemberObjs[i].get_uid()+" ,"+userFreqShards[myMemberObjs[i].get_shardIdx()][myMemberObjs[i].get_idxInShard()]+"\n";
			//sum += userFreqShards[myMemberObjs[i].get_shardIdx()][myMemberObjs[i].get_idxInShard()];	
		}

		return userFreqStats;
	}

}
