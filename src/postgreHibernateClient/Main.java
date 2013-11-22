package postgreHibernateClient;


import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;

public class Main {
	private static SessionFactory sessionFactory;
	private static ServiceRegistry serviceRegistry;

	public static void main(String[] args){
		Session session = null;
		try{
			try {
				Configuration configuration = new Configuration();
				configuration.configure();//.setProperty("hibernate.show_sql", "false");
				serviceRegistry = new ServiceRegistryBuilder().applySettings(
						configuration.getProperties()).buildServiceRegistry();
				sessionFactory = configuration.buildSessionFactory(serviceRegistry);
				}
			catch (Throwable ex) {
				System.err.println("Failed to create sessionFactory object.: " + ex);
				throw new ExceptionInInitializerError(ex);
				}
			session = sessionFactory.openSession();
			Transaction tx = session.beginTransaction();


			Student_Info student = new Student_Info();
			Class insertClass = Class.forName("postgreHibernateClient.Student_Info");
			//System.out.println(insertClass.getName());
			//get constructor that takes a String as argument
			//Class<insertClass> clazz = 
			Constructor constructor = insertClass.getConstructor(new Class[]{});

			Object insertInstance ;
//			System.out.println(insertInstance.getClass().getName());
			Map<String, Method> methods = FactoryInsert.getSetterMethods(insertClass);
			System.out.println(methods.size());
			
			for (int j = 0; j<5; j++){
				insertInstance = constructor.newInstance();
				for(Iterator<String> i = methods.keySet().iterator(); i.hasNext();){
					String s= i.next();
					System.out.println(s);
					Object x= methods.get(s).invoke(insertInstance, "hhh"+i);
				}
				session.save(insertInstance);
				if(j==3){
					session.flush();
					session.clear();
				}
			}
			tx.commit();
			System.out.println("Done");
		}
		catch (Exception e) {
			System.out.println("errrrrrrrrrrror"+e.getMessage());
		} 
		finally {
			session.close();
		}
		
		
		
	}

}
