package postgreHibernateClient;

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



import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;




public class postgreClient extends DB {

	private static int NumServers = 1;
	private static Semaphore Server = new Semaphore(NumServers, true);
	private static int ServiceTime = 1; //in msec 
	
	
	boolean initialized = false;
	Session session = null;
	//Hibernate variables
	private  SessionFactory sessionFactory;
	private ServiceRegistry serviceRegistry;
	private Transaction tx;
	private static int load_index;
	
	public boolean init() throws DBException {
		System.out.println("initializing.....");
		if(initialized)
			return true;
		try {
			Configuration configuration = new Configuration();
			configuration.configure().setProperty("hibernate.show_sql", "false");
			serviceRegistry = new ServiceRegistryBuilder().applySettings(
					configuration.getProperties()).buildServiceRegistry();
			sessionFactory = configuration.buildSessionFactory(serviceRegistry);
			load_index = 0;
			}
		catch (Throwable ex) {
			System.err.println("Failed to create sessionFactory object.: " + ex);
			throw new ExceptionInInitializerError(ex);
			}
		
		initialized = true;
		return true;
	}
	
	
	public void cleanup(boolean warmup) {
		System.out.println("shutdown couchbase client connection");
//		tx.commit();
//		session.close();
		initialized = false;
	}

	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage) {

		session = sessionFactory.openSession();


		try{
			tx = session.beginTransaction();
			Class insertClass = Class.forName("postgreHibernateClient."+entitySet.toUpperCase());
			Constructor constructor = insertClass.getConstructor(new Class[]{});

			
			//GET ALL SETTER METHODS FOR THE TABLE entityset
			Map<String, Method> methods = FactoryInsert.getSetterMethods(insertClass);
			Object insertInstance = constructor.newInstance();
			
			if(entitySet.equalsIgnoreCase("users"))
				values.put("userid", new ObjectByteIterator(entityPK.getBytes()));
			else if(entitySet.equalsIgnoreCase("resources"))
				values.put("rid", new ObjectByteIterator(entityPK.getBytes()));
				
			Iterator<String> i = values.keySet().iterator();
			//System.out.print(values.size());
			while( i.hasNext()){
				String s= i.next();
				Method m = methods.get(s);
				if(insertImage && (s.equalsIgnoreCase("pic") || s.equalsIgnoreCase("tpic"))){
						m.invoke(insertInstance, ((ObjectByteIterator)values.get(s)).toArray());
						continue;
				}
				m.invoke(insertInstance, values.get(s).toString());
			}
			session.save(insertInstance);
			
			if(entitySet.equalsIgnoreCase("resources")){
				USERS u = new USERS();
				u = (USERS) session.get(USERS.class, values.get("creatorid").toString());
				u.setResCnt(u.getResCnt()+1);
				session.save(u);
			}
			
			load_index++;
			if(load_index%50==0){
				System.out.println("loading "+ entitySet );
				session.flush();
				session.clear();
			}
			tx.commit();
		}
		catch (Exception e) {
			tx.rollback();
			System.out.println("errrrrrrrrrrror"+e.getMessage()+"   ");
			e.printStackTrace();
		} 
		finally {
			session.close();
		}
		
		return 0;
	}
	
	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {
		
		if (requesterID < 0 || profileOwnerID < 0)
			return -1;
		session= sessionFactory.openSession();
		try {
			tx = session.beginTransaction();
			USERS u = (USERS)session.get(USERS.class, Integer.toString(profileOwnerID));
			
			result.put("friendcount", new ObjectByteIterator(Integer.toString(u.getConfFriendCnt()).getBytes()));
			result.put("resourcecount", new ObjectByteIterator(Integer.toString(u.getResCnt()).getBytes()));
			if(profileOwnerID == requesterID)
				result.put("pendingcount", new ObjectByteIterator(Integer.toString(u.getPendFriendCnt()).getBytes()));

			Map<String, Method> methods = FactoryInsert.getGetterMethods(USERS.class);
			Set<String> keys = methods.keySet();
			keys.remove("confFriendCnt");
			keys.remove("pendFriendCnt");
			keys.remove("resCnt");

			Iterator<String> it = keys.iterator();
			while(it.hasNext()){
				String key = it.next();
				if(!insertImage && (key.equalsIgnoreCase("pic") || key.equalsIgnoreCase("tpic")))
					continue;
				Method methd = methods.get(key);
				Object r = methd.invoke(u, null);
				result.put(key, new ObjectByteIterator(r.toString().getBytes()));											
			}
			tx.commit();
			return 0;
		} catch (Exception e) {
			System.out.println(e.toString());
			return -1;
		}
		finally{
			session.close();
		}
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();
			Criterion t1 = Restrictions.eq("inviteeid", Integer.toString(profileOwnerID));
			Criterion t2 = Restrictions.eq("inviterid", Integer.toString(profileOwnerID));
			
			List<Friendship> obj = session.createCriteria(Friendship.class)
					.add(Restrictions.and(Restrictions.or(t1,t2),
											Restrictions.eq("value", Integer.toString(2))))
					.list();
			Map<String, Method> methods = FactoryInsert.getGetterMethods(USERS.class);
			Set<String> keys;
			if (fields == null){
				keys = methods.keySet();
				keys.remove("confFriendCnt");
				keys.remove("pendFriendCnt");
				keys.remove("resCnt");
			}
			else
				keys = fields;
			
			HashMap<String, ByteIterator> uDetails;
			for(Friendship f : obj){
				uDetails = new HashMap<String, ByteIterator>();
				USERS user = (USERS) session.get(USERS.class, f.getInviterid());
				Iterator<String> it = keys.iterator();
				while(it.hasNext()){
					String key = it.next();
					if(!insertImage && (key.equalsIgnoreCase("pic") || key.equalsIgnoreCase("tpic")))
						continue;
					Method methd = methods.get(key);
					Object r = methd.invoke(user, null);
					uDetails.put(key, new ObjectByteIterator(r.toString().getBytes()));
//					System.out.println("listing friends of: "+profileOwnerID);
				}
				result.add(uDetails);
			}
			
			tx.commit();
//			System.out.println("listed friends of: "+profileOwnerID);
			return 0;
		}catch (Exception e) {
			System.out.println("list friend: "+ profileOwnerID);
			e.printStackTrace(System.out);
			return -1;
		}
		finally{
			session.close();
		}
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> values, boolean insertImage, boolean testMode) {
		if(profileOwnerID < 0)
			return -1;
		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();
			List<Friendship> obj = session.createCriteria(Friendship.class)
								.add(Restrictions.and(Restrictions.eq("inviteeid", Integer.toString(profileOwnerID)),
										Restrictions.eq("value", Integer.toString(1))))
								.list();
			Map<String, Method> methods = FactoryInsert.getGetterMethods(USERS.class);
			Set<String> keys = methods.keySet();
			keys.remove("confFriendCnt");
			keys.remove("pendFriendCnt");
			keys.remove("resCnt");

			
			HashMap<String, ByteIterator> uDetails;
			for(Friendship f : obj){
				uDetails = new HashMap<String, ByteIterator>();
				USERS user = (USERS) session.get(USERS.class, f.getInviterid());
				Iterator<String> it = keys.iterator();
				while(it.hasNext()){
					String key = it.next();
					if(!insertImage && (key.equalsIgnoreCase("pic") || key.equalsIgnoreCase("tpic")))
						continue;
					Method methd = methods.get(key);
					Object r = methd.invoke(user, null);
					uDetails.put(key, new ObjectByteIterator(r.toString().getBytes()));											
				}
				values.add(uDetails);
			}
//			System.out.println("friend requests for "+profileOwnerID+" counts: "+values.size());
			tx.commit();
			return 0;
		}catch (Exception e) {
			System.out.println("view friend reqs: "+ profileOwnerID);
			e.printStackTrace(System.out);
			return -1;
		}
		finally{
			session.close();
		}
	}

	@Override
	public int acceptFriend(int invitorID, int inviteeID) {
		//delete from pending of the invitee
		//add to confirmed of both invitee and invitor
		if(invitorID < 0 || inviteeID < 0)
			return -1;
		
		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();
			Criterion id1 = Restrictions.eq("inviterid", Integer.toString(invitorID));
			Criterion id2 = Restrictions.eq("inviteeid", Integer.toString(inviteeID));
			Criterion v = Restrictions.eq("value", Integer.toString(1));
			
			List<Friendship> obj = (List<Friendship>) session.createCriteria(Friendship.class)
																	.add(Restrictions.and(id1,Restrictions.and(id2,v)))
														.list();
			
			
			for(Friendship m : obj){
				session.delete("Friendship",m);
				
				Friendship f = new Friendship();
				f.setInviterid(Integer.toString(invitorID));
				f.setInviteeid(Integer.toString(inviteeID));
				f.setValue(Integer.toString(2));
				session.saveOrUpdate(f);


				
				USERS u = new USERS();
				u = (USERS) session.get(USERS.class, Integer.toString(inviteeID));
				u.setPendFriendCnt(u.getPendFriendCnt()-1);
				u.setConfFriendCnt(u.getConfFriendCnt()+1);
				session.saveOrUpdate(u);
				
				u = new USERS();
				u = (USERS) session.get(USERS.class, Integer.toString(invitorID));
				u.setConfFriendCnt(u.getConfFriendCnt()+1);
				session.saveOrUpdate(u);

				
				tx.commit();
			}
			return 0;
		}
		catch (Exception e) {
			tx.rollback();
			e.printStackTrace(System.out);
			return -1;
		}
		finally{
			session.close();
		}

	}

	@Override
	public int rejectFriend(int invitorID, int inviteeID) {
		//remove from pending of invitee
		if(invitorID < 0 || inviteeID < 0)
			return -1;
		
		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();
			Criterion id1 = Restrictions.eq("inviterid", Integer.toString(invitorID));
			Criterion id2 = Restrictions.eq("inviteeid", Integer.toString(inviteeID));
			Criterion v = Restrictions.eq("value", Integer.toString(1));
			List<Friendship> obj = (List<Friendship>) session.createCriteria(Friendship.class)
																	.add(Restrictions.and(id1,Restrictions.and(id2,v)))
								.list();

			
			for(Friendship m : obj){
				session.delete("Friendship",m);
				
				USERS u = new USERS();
				u = (USERS) session.get(USERS.class, Integer.toString(inviteeID));
				u.setPendFriendCnt(u.getPendFriendCnt()-1);
				session.saveOrUpdate(u);
				
				tx.commit();
//				tx = session.beginTransaction();
//				System.out.println("rejected friend req from "+ invitorID+" to "+inviteeID+".");
//				tx = session.beginTransaction();
			}
			return 0;
		}
		 catch (Exception e) {
			
			e.printStackTrace(System.out);
			return -1;
		}
		finally{
			session.close();
		}
	}

	@Override
	public int inviteFriend(int invitorID, int inviteeID) {
		//add to pending for the invitee
		if(invitorID < 0 || inviteeID < 0)
			return -1;
		
		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();

			Friendship f = new Friendship();
			f.setInviterid(Integer.toString(invitorID));
			f.setInviteeid(Integer.toString(inviteeID));
			f.setValue("1");
			session.save(f);
			
			USERS u = new USERS();
			u = (USERS) session.get(USERS.class, Integer.toString(inviteeID));
			u.setPendFriendCnt(u.getPendFriendCnt()+1);
			session.saveOrUpdate(u);
			
//			session.flush();
//			session.clear();
			tx.commit();
//			tx = session.beginTransaction();
//			System.out.println("invite friend from "+ invitorID+" to "+inviteeID+".");
//			tx = session.beginTransaction();
			
			return 0;
		}
			catch (Exception e) {
			e.printStackTrace(System.out);
			return -1;
			}
		finally{
			session.close();
		}
	}


	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		//delete from both their confFriends
		if(friendid1 < 0 || friendid2 < 0)
			return -1;
		
		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();

			Criterion t1 = Restrictions.and(Restrictions.eq("inviterid", Integer.toString(friendid1)),
											Restrictions.eq("inviteeid", Integer.toString(friendid2)));
			Criterion t2 =Restrictions.and(Restrictions.eq("inviterid", Integer.toString(friendid2)),
											Restrictions.eq("inviteeid", Integer.toString(friendid1)));

			List<Friendship> obj = (List<Friendship>) session.createCriteria(Friendship.class)
													.add(Restrictions.and(Restrictions.or(t1, t2),
																			Restrictions.eq("value", Integer.toString(2)))
														)
													.list();
			
			for(Friendship f : obj){
				session.delete("Friendship", f);
				
				USERS u = (USERS) session.get(USERS.class, f.getInviterid());
				u.setConfFriendCnt(u.getConfFriendCnt()-1);
				session.saveOrUpdate(u);
				
				u = (USERS) session.get(USERS.class, f.getInviteeid());
				u.setConfFriendCnt(u.getConfFriendCnt()-1);
				session.saveOrUpdate(u);

				tx.commit();
//				session.flush();
//				session.clear();
				
//				tx = session.beginTransaction();
//				System.out.println("Deleted friendship of: "+ friendid1+" and "+friendid2+".");
//				tx = session.beginTransaction();
			}
			

			return 0;
		}
		catch (Exception e) {
			
			tx.rollback();
			System.out.println("Can not Delete friendship of: "+ friendid1+" and "+friendid2+".");
			e.printStackTrace(System.out);
			return -1;
		}
		finally{
			session.close();
		}
	}

	
	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
			session = sessionFactory.openSession();
			try {
				tx = session.beginTransaction();
				//Do work for 500 msec
				//Get the current time and compute when work is complete
	
				List<RESOURCES> obj = session.createCriteria(RESOURCES.class)
								.add(Restrictions.eq("walluserid", Integer.toString(profileOwnerID)))
								.addOrder(Order.desc("rid"))
								.setMaxResults(k)
								.list();
				if (obj.size()>0){
					int i=0;
					Map<String, Method> methods = FactoryInsert.getGetterMethods(RESOURCES.class);
					Set<String> keys = methods.keySet();
					HashMap<String, ByteIterator> values;
					while(i<obj.size()){
						values = new HashMap<String, ByteIterator>();
						Iterator<String> it = keys.iterator();
						RESOURCES res = obj.get(i);
						while(it.hasNext()){
							String key = it.next();
							Method m = methods.get(key);
							Object r = m.invoke(res, null);
							values.put(key, new ObjectByteIterator(r.toString().getBytes()));
						}
						result.add(values);
						i++;
					}
				}	
//				System.out.println("view top "+k+"res of "+profileOwnerID+" and viewed "+result.size());
				tx.commit();
				return 0;
		} catch (Exception e) {
			System.out.println(e.toString());
			return -1;
		}finally{
			session.close();
		}
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		if(profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
			return -1;

		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();
			
//			System.out.print("VIEW ");
			List<MANIPULATIONS> obj = session.createCriteria(MANIPULATIONS.class)
					.add(Restrictions.eq("rid", "'"+resourceID+"'"))
					.list();
			int i=0;
			Map<String, Method> methods = FactoryInsert.getGetterMethods(MANIPULATIONS.class);
			Set<String> keys = methods.keySet();
			HashMap<String, ByteIterator> values;
			while(i<=obj.size()){
				values = new HashMap<String, ByteIterator>();
				Iterator<String> it = keys.iterator();
				MANIPULATIONS m = obj.get(i);
				while(it.hasNext()){
					String key = it.next();
					Method methd = methods.get(key);
					Object r = methd.invoke(m, null);
					values.put(key, new ObjectByteIterator(r.toString().getBytes()));						
				}
				result.add(values);
				i++;
			}
//			System.out.println("res of "+profileOwnerID+"   ccc"+result.size());
			tx.commit();
			return 0;
		}catch (Exception e) {
			System.out.println("error in viewCommentOnResource:  "+resourceID);
			e.printStackTrace(System.out);
			return -1;
		}finally{
			session.close();
		}

	}
	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String,ByteIterator> commentValues) {
		if(profileOwnerID < 0 || commentCreatorID < 0 || resourceID < 0)
			return -1;
		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();
			System.out.println("posting comment");
			
			MANIPULATIONS manip = new MANIPULATIONS();
			
			Map<String, Method> methods = FactoryInsert.getSetterMethods(MANIPULATIONS.class);
			methods.get("mid").invoke(manip, commentValues.get("mid").toString());
			methods.get("creatorid").invoke(manip, profileOwnerID);
			methods.get("rid").invoke(manip, resourceID);
			methods.get("modifierid").invoke(manip, commentCreatorID);
			methods.get("timestamp").invoke(manip, commentValues.get("timestamp").toString());
			methods.get("type").invoke(manip, commentValues.get("type").toString());
			methods.get("content").invoke(manip, commentValues.get("content").toString());
			
			session.save(manip);
			System.out.println("posted comment on "+profileOwnerID+" by "+commentCreatorID);
			tx.commit();
			return 0;

		}catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}finally{
			session.close();
		}
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		if(resourceCreatorID < 0 || manipulationID < 0 || resourceID < 0)
			return -1;
		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();
			
			List<MANIPULATIONS> obj= session.createCriteria(MANIPULATIONS.class)
					.add(Restrictions.and(Restrictions.eq("mid",Integer.toString(manipulationID)),
							Restrictions.eq("rid", Integer.toString(resourceID))))
					.list();
			
			for (MANIPULATIONS m: obj){
				session.delete("MANIPULATIONS", m);
			
			System.out.println("Deleted comment with ID "+ manipulationID+" on resource "+resourceID+".");
			}
			tx.commit();
			return 0;
		}catch (Exception e) {
			e.printStackTrace(System.out);
			return -1;
		}finally{
			session.close();
		}
	}



	@Override
	public HashMap<String, String> getInitialStats() {
		HashMap<String, String> stats = new HashMap<String, String>();
		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();

			
			Criteria c= session.createCriteria(USERS.class)
							.setProjection(Projections.count("userid"));
			
			Long usercount = (Long) c.list().get(0);
			usercount = usercount>0 ? usercount : 0;
			stats.put("usercount", Long.toString(usercount));

			Criteria c1 = session.createCriteria(USERS.class)
					.setProjection(Projections.min("userid"));
			
			String offset = (String) c1.list().get(0);
			//get resources per user
	
			USERS u = new USERS();
			u = (USERS) session.get(USERS.class, offset);
			stats.put("avgfriendsperuser",Integer.toString(u.getConfFriendCnt()));
	
			stats.put("resourcesperuser",Integer.toString(u.getResCnt()));
	
			stats.put("avgpendingperuser",Integer.toString(u.getPendFriendCnt()));
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			session.close();
		}
		return stats;
	}

	@Override
	public int CreateFriendship(int memberA, int memberB) {
		if(memberA < 0 || memberB < 0)
			return -1;

		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();
			Friendship f = new Friendship();
			f.setInviterid(Integer.toString(memberA));
			f.setInviteeid(Integer.toString(memberB));
			f.setValue("2");
			session.save(f);
			
			USERS u = new USERS();
			u = (USERS) session.get(USERS.class, Integer.toString(memberA));
			u.setConfFriendCnt(u.getConfFriendCnt()+1);
			session.save(u);
			
			u= null;
			
			u = (USERS) session.get(USERS.class, Integer.toString(memberB));
			u.setConfFriendCnt(u.getConfFriendCnt()+1);
			session.save(u);
			
			tx.commit();
		}
		catch(Exception e){
			//tx.rollback();
			System.out.println("exception in create friendship: "+ memberA + "to"+ memberB+". Below:");
			e.printStackTrace();
		}
		finally{
			session.close();
		}
		return 0;
	}

	public int queryPendingFriendships(int userOffset, int userCount, HashMap<Integer, Vector<Integer>> pendingFrnds){
		return 0;

	}

	@Override
	public void createSchema(Properties props) {

	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		
		if(memberID < 0) return -1;
		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();

			Criterion t1 = Restrictions.eq("inviterid", Integer.toString(memberID));
			Criterion t2 = Restrictions.eq("inviteeid", Integer.toString(memberID));
			Criterion t3 = Restrictions.eq("inviterid", Integer.toString(1));
			
			List<Friendship> l= (List<Friendship>)session.createCriteria(Friendship.class)
												.add(Restrictions.and(
																Restrictions.or(t1,t2),t3))
												.list();
//			System.out.println("query pending friends:"+ memberID);
//			System.out.println("query pending friends:"+ l.size());
			if (l.size()>0){
				Iterator<Friendship> it = l.iterator();
				while(it.hasNext()){
					Friendship f = it.next();
					pendingIds.add(Integer.parseInt(f.getInviteeid()));
				}
			}
			tx.commit();
		}catch(Exception e){
			
		}finally{
			session.close();
		}
		return 0;

	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		if(memberID < 0) return -1;

//		System.out.println("query confirmed friends:"+ memberID);
		session = sessionFactory.openSession();
		try {
			tx = session.beginTransaction();

			Criterion t1 = Restrictions.eq("inviterid", Integer.toString(memberID));
			Criterion t2 = Restrictions.eq("inviteeid", Integer.toString(memberID));
			Criterion t3 = Restrictions.eq("inviterid", Integer.toString(1));
			
			List<Friendship> l= (List<Friendship>)session.createCriteria(Friendship.class)
												.add(Restrictions.and(
																Restrictions.or(t1,t2),t3))
												.list();
//			System.out.println("query confirmed friends:"+ l.size());
			if (l.size()>0){
				Iterator<Friendship> it = l.iterator();
				while(it.hasNext()){
					Friendship f = it.next();
					confirmedIds.add(Integer.parseInt(f.getInviteeid()));
				}
			}tx.commit();
		}catch(Exception e){
			
		}finally{
			session.close();
		}
		
		return 0;
	}

}