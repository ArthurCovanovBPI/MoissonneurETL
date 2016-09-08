package OAI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OAIRecordMetadata
{
	private String regexOAIDC  = "<oai_dc:dc.*?>(.*)<\\/oai_dc:dc>";
	private Pattern patternOAIDC = Pattern.compile(regexOAIDC, Pattern.DOTALL);

	private String regexValues = "<(.*?)>\\s?(.*?)\\s?<\\/.*?>";
	private Pattern patternValues = Pattern.compile(regexValues, Pattern.DOTALL);

	private String XML = null;
	private String OAIDC = null;
	private HashMap<String, ArrayList<String>> values = new HashMap<String, ArrayList<String>>();

	public String getXML()
	{
		return XML;
	}

	public String getOAIDC()
	{
		return OAIDC;
	}

	public HashMap<String, ArrayList<String>> getValues()
	{
		return values;
	}

	private void OAIDCAnalysis()
	{
		Matcher matcherIdentifier = patternOAIDC.matcher(XML);
		if(matcherIdentifier.find())
		{
			OAIDC = matcherIdentifier.group(1);
		}
	}

	private void valuesAnalysis()
	{
		Matcher matcherValues = patternValues.matcher(OAIDC);
		while(matcherValues.find())
		{
			String K = matcherValues.group(1);
			String V = matcherValues.group(2);
			if(!values.containsKey(K))
				values.put(K, new ArrayList<String>());
			values.get(K).add(V);
		}
	}

	public OAIRecordMetadata(String XML)
	{
		this.XML=XML;
		OAIDCAnalysis();
		valuesAnalysis();
//		System.out.println(XML);
//		System.out.println(this.toString());
	}

	public String toString()
	{
		String result =	"metadata:\n";
		for(String K : values.keySet())
		result+=		"\t" + K + " : " + values.get(K) + "\n";
		return result;
	}
}
