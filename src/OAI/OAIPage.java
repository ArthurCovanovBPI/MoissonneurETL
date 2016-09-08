package OAI;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OAIPage
{
	private String regexResumptionToken = "<resumptionToken\\s?(.*)?\\s?>(.*)<\\/resumptionToken>";
	private Pattern patternResumptionToken = Pattern.compile(regexResumptionToken, Pattern.DOTALL);
	
	private String regexCompleteListSize = "completeListSize=\"(\\d+)\"";
	private Pattern patternCompleteListSize = Pattern.compile(regexCompleteListSize, Pattern.DOTALL);
	
	private String regexCursor = "cursor=\"(\\d+)\"";
	private Pattern patternCursor = Pattern.compile(regexCursor, Pattern.DOTALL);

	private String regexListRecords = "<ListRecords.*?>(.*)<\\/ListRecords>";
	private Pattern patternListRecords = Pattern.compile(regexListRecords, Pattern.DOTALL);

	private String regexRecord = "(<record.*?>.*?<\\/record>)";
	private Pattern patternRecord = Pattern.compile(regexRecord, Pattern.DOTALL);

	private String XML = null;
	private String resuptionToken = null;
	private String cursor = null;
	private String completeListSize = null;
	private String listRecords = null;

	ArrayList<OAIRecord> records = new ArrayList<OAIRecord>();

	public String getXML()
	{
		return XML;
	}

	public String getResuptionToken()
	{
		return resuptionToken;
	}

	public String getCursor()
	{
		return cursor;
	}

	public String getCompleteListSize()
	{
		return completeListSize;
	}

	public ArrayList<OAIRecord> getRecords()
	{
		return records;
	}

	private void resuptionTokenAnalysis()
	{
		Matcher matcherResumptionToken = patternResumptionToken.matcher(XML);
		String resumptionTokenAttributes = null;
		if(matcherResumptionToken.find())
		{
			resumptionTokenAttributes = matcherResumptionToken.group(1);
			resuptionToken = matcherResumptionToken.group(2);
		}
		if(resumptionTokenAttributes!=null)
		{
			Matcher matcherCompleteListSize = patternCompleteListSize.matcher(resumptionTokenAttributes);
			Matcher matcherCursor = patternCursor.matcher(resumptionTokenAttributes);
			if(matcherCompleteListSize.find())
				completeListSize = matcherCompleteListSize.group(1);
			if(matcherCursor.find())
				cursor = matcherCursor.group(1);
		}
	}

	private void listRecordsAnalysis()
	{
		Matcher matcherListRecords = patternListRecords.matcher(XML);
		if(matcherListRecords.find())
		{
			listRecords = matcherListRecords.group(1);
		}
	}

	private void listRecords()
	{
		Matcher matcherRecord = patternRecord.matcher(listRecords);
		String recordXML=null;
		while(matcherRecord.find())
		{
			recordXML = matcherRecord.group(1);
			try
			{
				OAIRecord record = new OAIRecord(recordXML);
				records.add(record);
			}
			catch(OAIRecordException e)
			{
				e.printStackTrace();
			}
		}
	}

	public OAIPage(String XML)
	{	// ( . Y . ) \\
		this.XML=XML;
		resuptionTokenAnalysis();
		listRecordsAnalysis();
		listRecords();
	}
}
