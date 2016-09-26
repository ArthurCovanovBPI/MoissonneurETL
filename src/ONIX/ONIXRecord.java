package ONIX;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ONIXRecord
{
	private String regexRecord = "\\s?<record.*?>.*?<\\/record>\\s?";
	private Pattern patternRecord = Pattern.compile(regexRecord, Pattern.DOTALL);
	
	private String regexHeader = "<header.*?>(.*)<\\/header>";
	private Pattern patternHeader = Pattern.compile(regexHeader, Pattern.DOTALL);
	
	private String regexMetadata = "<metadata.*?>(.*)<\\/metadata>";
	private Pattern patternMetadata = Pattern.compile(regexMetadata, Pattern.DOTALL);
	
	private ONIXRecordHeader header = null;
	private ONIXRecordMetadata metadata = null;
	
	private String XML = null;

	public String getXML()
	{
		return XML;
	}

	public ONIXRecordHeader getHeader()
	{
		return header;
	}

	public ONIXRecordMetadata getMetadata()
	{
		return metadata;
	}

	private void headerAnalysis()
	{
		Matcher matcherHeader = patternHeader.matcher(XML);
		if(matcherHeader.find())
		{
			header = new ONIXRecordHeader(matcherHeader.group(1));
		}
	}

	private void metadataAnalysis()
	{
		Matcher matcherMetadata = patternMetadata.matcher(XML);
		if(matcherMetadata.find())
		{
			metadata = new ONIXRecordMetadata(matcherMetadata.group(1));
		}
	}

	public ONIXRecord(String XML) throws ONIXRecordException
	{
		Matcher matcherRecord = patternRecord.matcher(XML);
		if(!matcherRecord.matches())
		{
			throw new ONIXRecordException("Record not matched");
		}
		this.XML=XML;
		headerAnalysis();
		metadataAnalysis();
//		System.out.println(header);
	}
}
