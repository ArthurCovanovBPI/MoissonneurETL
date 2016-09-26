package ONIX;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ONIXRecordHeader
{
	private String regexIdentifier  = "<identifier.*?>(.*)<\\/identifier>";
	private Pattern patternIdentifier = Pattern.compile(regexIdentifier, Pattern.DOTALL);

	private String regexDatestamp = "<datestamp.*?>(.*)<\\/datestamp>";
	private Pattern patternDatestamp = Pattern.compile(regexDatestamp, Pattern.DOTALL);

	private String regexSetSpecs = "<setSpec.*?>(.*)<\\/setSpec>.*?<setSpec.*?>(.*)<\\/setSpec>";
	private Pattern patternSetSpecs = Pattern.compile(regexSetSpecs, Pattern.DOTALL);

	private String XML = null;
	private String identifier = null;
	private String datestamp = null;
	private String setSpecsCS = null;
	private String setSpecsMath = null;

	public String getXML()
	{
		return XML;
	}

	public String getIdentifier()
	{
		return identifier;
	}

	public String getDatestamp()
	{
		return datestamp;
	}

	public String getSetSpecsCS()
	{
		return setSpecsCS;
	}

	public String getSetSpecsMath()
	{
		return setSpecsMath;
	}

	public String toString()
	{
		String result =	"header:\n";
		result+=		"\tidentifier: "	+ identifier	+ "\n";
		result+=		"\tdatestamp : "	+ datestamp		+ "\n";
		result+=		"\tsetSpecsCS: "	+ setSpecsCS	+ "\n";
		result+=		"\tsetSpecsMath: "	+ setSpecsMath	+ "\n";
		return result;
	}

	private void identifierAnalysis()
	{
		Matcher matcherIdentifier = patternIdentifier.matcher(XML);
		if(matcherIdentifier.find())
		{
			identifier = matcherIdentifier.group(1);
		}
	}

	private void datestampAnalysis()
	{
		Matcher matcherDatestamp = patternDatestamp.matcher(XML);
		if(matcherDatestamp.find())
		{
			datestamp = matcherDatestamp.group(1);
		}
	}

	private void setSpecsAnalysis()
	{
		Matcher matcherSetSpecs = patternSetSpecs.matcher(XML);
		if(matcherSetSpecs.find())
		{
			setSpecsCS = matcherSetSpecs.group(1);
			setSpecsMath = matcherSetSpecs.group(2);
		}
	}

	public ONIXRecordHeader(String XML)
	{
		this.XML=XML;
		identifierAnalysis();
		datestampAnalysis();
		setSpecsAnalysis();
	}
}
