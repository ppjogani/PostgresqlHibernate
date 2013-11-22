package postgreHibernateClient;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

public class FactoryInsert {
	
	static final String GET = "get";
    static final String IS = "is";
    static final String SET = "set";

  
    
    /**
     * Converts a method name into a camel-case field name, starting from {@code start}.
     */
    public static String toProperty(int start, String methodName)
    {
        char[] prop = new char[methodName.length()-start];
        methodName.getChars(start, methodName.length(), prop, 0);
        int firstLetter = prop[0];
        prop[0] = (char)(firstLetter<91 ? firstLetter + 32 : firstLetter);
        return new String(prop);
    }
    

    
    /**
     * Gets the setters of a pojo as a map of {@link String} as key and 
     * {@link Method} as value.
     */
    
    public static Map<String,Method> getSetterMethods(Class<?> pojoClass) 
    {
        HashMap<String,Method> methods = new HashMap<String,Method>();
        fillSetterMethods(pojoClass, methods);
        return methods;
    }
    
    public static boolean isSetter(Method method){
    	  if(!method.getName().startsWith("set")) return false;
    	  if(method.getParameterTypes().length != 1) return false;
    	  return true;
    }
    
    public static boolean isGetter(Method method){
    	  if(!method.getName().startsWith("get"))      return false;
    	  if(method.getParameterTypes().length != 0)   return false;  
    	  if(void.class.equals(method.getReturnType())) return false;
    	  return true;
    }
    
    private static void fillSetterMethods(Class<?> pojoClass, Map<String,Method> baseMap) 
    {
        if(pojoClass.getSuperclass()!=Object.class)
            fillSetterMethods(pojoClass.getSuperclass(), baseMap);
        
        Method[] methods = pojoClass.getDeclaredMethods();
        for(int i=0; i<methods.length; i++)
        {
            Method m = methods[i];
            if(isSetter(m))
            {
                baseMap.put(toProperty(SET.length(), m.getName()), m);
            }
        }
    }
     
    
    
    public static Map<String,Method> getGetterMethods(Class<?> pojoClass) 
    {
        HashMap<String,Method> methods = new HashMap<String,Method>();
        fillGetterMethods(pojoClass, methods);
        return methods;
    }
    
    private static void fillGetterMethods(Class<?> pojoClass, Map<String,Method> baseMap) 
    {
        if(pojoClass.getSuperclass()!=Object.class)
            fillGetterMethods(pojoClass.getSuperclass(), baseMap);

        Method[] methods = pojoClass.getDeclaredMethods();
        for (int i=0;i<methods.length;i++)
        {
            Method m=methods[i];
            if (isGetter(m))
            {
                String name=m.getName();
                if (name.startsWith(GET))
                    baseMap.put(toProperty(GET.length(), name), m);
            }
        }
    }}