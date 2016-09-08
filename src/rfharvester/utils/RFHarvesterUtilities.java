package rfharvester.utils;

import java.util.ArrayList;

final public class RFHarvesterUtilities
{

	public static String arrayListToString(ArrayList<String> list, String separator)
	{
		if(list==null || list.isEmpty())
			return null;
		String result = list.get(0);
		for(int i = 1; i< list.size(); i++)
			result += (separator + list.get(i));
		return result;
	}

	@SafeVarargs
	public static final ArrayList<String> createSet(ArrayList<String> ... list)
	{
		ArrayList<String> set = new ArrayList<String>();
		for(ArrayList<String> l : list)
		{
			if(l!=null)
			{
				set.addAll(l);
			}
		}
		return set;
	}
}
