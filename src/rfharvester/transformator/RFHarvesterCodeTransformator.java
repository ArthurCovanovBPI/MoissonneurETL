package rfharvester.transformator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RFHarvesterCodeTransformator implements RFHarvesterTransformatorInterfaceV2
{
//	private String regexLine = "(\\S+?)[\t ]*(<(((()|\\[(\\S+)\\])=[\\t ]*(\\S.*)))|<(\\S+)>)";
	private String regexLine = "(\\S+?)[\t ]*(<(((()|\\[(\\S+)\\])=[\\t ]*(\\S.*)))|<(.+)>)";
	private Pattern patternLine = Pattern.compile(regexLine);

	private String regexVariable = "\\$_\\((\\S+?)\\)(\\[(.*?)\\]|\\{([0-9]+|i)\\})?";
	private Pattern patternVariable = Pattern.compile(regexVariable, Pattern.DOTALL);

	String code = "";

	HashMap<String, ArrayList<String>>	transformationsType1 = new HashMap<String, ArrayList<String>>();
	HashMap<String, ArrayList<String[]>>transformationsType2 = new HashMap<String, ArrayList<String[]>>();
	HashMap<String, ArrayList<String>>	transformationsType3 = new HashMap<String, ArrayList<String>>();

	private void transformatorLinesParser()
	{
		Matcher matcherLine = patternLine.matcher(code);
		while(matcherLine.find())
		{
			String G1 = matcherLine.group(1);
			String G7 = matcherLine.group(7);
			String G8 = matcherLine.group(8);
			String G9 = matcherLine.group(9);
			if(G9!=""&&G9!=null)
			{
				ArrayList<String> T3 = ((transformationsType3.containsKey(G1))? transformationsType3.get(G1) : new ArrayList<String>());
				T3.add(G9);
				transformationsType3.put(G1, T3);
			}
			else if(matcherLine.group(7)!=""&&matcherLine.group(7)!=null)
			{
				ArrayList<String[]> T2 = ((transformationsType2.containsKey(G1))? transformationsType2.get(G1) : new ArrayList<String[]>());
				String Type2[] = new String[2];
				Type2[0] = G7;
				Type2[1] = G8;
				T2.add(Type2);
				transformationsType2.put(G1, T2);
			}
			else
			{
				ArrayList<String> T1 = ((transformationsType1.containsKey(G1))? transformationsType1.get(G1) : new ArrayList<String>());
				T1.add(G8);
				transformationsType1.put(G1, T1);
			}
		}
	}

	public RFHarvesterCodeTransformator(String c)
	{
		this.code = c;
		transformatorLinesParser();
	}

	private String transformLine(String line, HashMap<String, ArrayList<String>> source)
	{
		return transformLine(line, source, 0);
	}

	private String transformLine(String line, HashMap<String, ArrayList<String>> source, int i)
	{
		String resultLine = new String(line);
//		System.out.println("resultLine: " + resultLine);

		Matcher matcherVariable = patternVariable.matcher(resultLine);
//		System.out.println("matcherVariable.find(): "+matcherVariable.find());
		while(matcherVariable.find())
		{
			String m1 = matcherVariable.group(1);
			String m3 = matcherVariable.group(3);
			String m4 = matcherVariable.group(4);
//			System.out.println("m1: "+m1);
			if(!source.containsKey(m1))
			{
				resultLine=null;
				break;
			}

			String replacement=null;

			if(m4!=""&&m4!=null)
			{
				int j =((m4.compareTo("i")==0)? i : Integer.parseInt(m4));
				if(source.get(m1).size()<=j)
					return null;
				replacement=source.get(m1).get(j);
			}
			else
			{
				String separator = ", ";
				if(m3!=""&&m3!=null)
				{
					separator = m3;
				}
				ArrayList<String> datas = source.get(m1);

				if(datas!=null && datas.size()>0)
				{
					replacement=datas.get(0);
					for(int j = 1; j < datas.size(); j++)
					{
						replacement=replacement.concat(separator).concat(datas.get(j));
					}
				}
			}
//			System.out.println("\t+"+replacement);
			if(replacement==null)
				return null;
			replacement = replacement.replaceAll("\\\\", "\\\\\\\\");
			replacement = replacement.replaceAll("\\$", "\\\\\\$");
//			System.out.println("-"+replacement);
			resultLine=matcherVariable.replaceFirst(replacement);
//			System.out.println("resultLine: " + resultLine);
			matcherVariable = patternVariable.matcher(resultLine);
		}

		return resultLine;
	}

	private ArrayList<String> transformatorType1(String line, HashMap<String, ArrayList<String>> source)
	{
		ArrayList<String> result = new ArrayList<String>();

		String resultLine = transformLine(line, source);
		
		if(resultLine==null)
			return null;

		result.add(resultLine);
		return result;
	}

	private ArrayList<String> transformatorType2(String[] line, HashMap<String, ArrayList<String>> source)
	{
		if(!source.containsKey(line[0]))
			return null;
		ArrayList<String> resource = source.get(line[0]);
		int resourceSize = resource.size();

		ArrayList<String> result = new ArrayList<String>();
		for(int i = 0; i<resourceSize; i++)
		{
			String resultLine = transformLine(line[1], source, i);
			if(resultLine==null)
				return null;
			result.add(resultLine);
		}
		return result;
	}

	private ArrayList<String> transformatorType3(String sourceCase, HashMap<String, ArrayList<String>> source)
	{
//		System.out.println(sourceCase + " - " + source.toString());
		if(!source.containsKey(sourceCase))
		{
			if(source.containsKey(sourceCase.replaceAll("\"", "'")))
				return new ArrayList<String>(source.get(sourceCase.replaceAll("\"", "'")));
			else
				return null;
		}
		return new ArrayList<String>(source.get(sourceCase));
	}

	@Override
	public HashMap<String, ArrayList<String>> transform(HashMap<String, ArrayList<String>> source)
	{
		HashMap<String, ArrayList<String>> destination = new HashMap<String, ArrayList<String>>();

		for(String K : transformationsType1.keySet())
		{
			for(String T1 : transformationsType1.get(K))
			{
//				System.out.println(K + "<= "+T1);
				ArrayList<String> Insertion = transformatorType1(T1, source);
				if(Insertion!=null)
				{
					if(destination.containsKey(K))
						Insertion.addAll(destination.get(K));
					destination.put(K, Insertion);
				}
//				if(Insertion!=null)
//					destination.put(K, Insertion);
			}
		}

		for(String K : transformationsType2.keySet())
		{
			for(String[] T2 : transformationsType2.get(K))
			{
				ArrayList<String> Insertion = transformatorType2(T2, source);
				if(Insertion!=null)
				{
					if(destination.containsKey(K))
						Insertion.addAll(destination.get(K));
					destination.put(K, Insertion);
				}
//				if(Insertion!=null)
//					destination.put(K, Insertion);
			}
		}

		for(String K : transformationsType3.keySet())
		{
			for(String T3 : transformationsType3.get(K))
			{
				ArrayList<String> Insertion = transformatorType3(T3, source);
				if(Insertion!=null)
				{
					if(destination.containsKey(K))
						Insertion.addAll(destination.get(K));
					destination.put(K, Insertion);
				}
//				if(Insertion!=null)
//					destination.put(K, Insertion);
			}
		}

		return destination;
	}
}
