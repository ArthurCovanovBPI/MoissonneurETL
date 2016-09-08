package OAI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OAIRecord
{
	private String regexRecord = "\\s?<record.*?>.*?<\\/record>\\s?";
	private Pattern patternRecord = Pattern.compile(regexRecord, Pattern.DOTALL);
	
	private String regexHeader = "<header.*?>(.*)<\\/header>";
	private Pattern patternHeader = Pattern.compile(regexHeader, Pattern.DOTALL);
	
	private String regexMetadata = "<metadata.*?>(.*)<\\/metadata>";
	private Pattern patternMetadata = Pattern.compile(regexMetadata, Pattern.DOTALL);
	
	private OAIRecordHeader header = null;
	private OAIRecordMetadata metadata = null;
	
	private String XML = null;

	public String getXML()
	{
		return XML;
	}

	public OAIRecordHeader getHeader()
	{
		return header;
	}

	public OAIRecordMetadata getMetadata()
	{
		return metadata;
	}

	private void headerAnalysis()
	{
		Matcher matcherHeader = patternHeader.matcher(XML);
		if(matcherHeader.find())
		{
			header = new OAIRecordHeader(matcherHeader.group(1));
		}
	}

	private void metadataAnalysis()
	{
		Matcher matcherMetadata = patternMetadata.matcher(XML);
		if(matcherMetadata.find())
		{
			metadata = new OAIRecordMetadata(matcherMetadata.group(1));
		}
	}

	public OAIRecord(String XML) throws OAIRecordException
	{
		Matcher matcherRecord = patternRecord.matcher(XML);
		if(!matcherRecord.matches())
		{
			throw new OAIRecordException("Record not matched");
		}
		this.XML=XML;
		headerAnalysis();
		metadataAnalysis();
//		System.out.println(header);
	}
}
