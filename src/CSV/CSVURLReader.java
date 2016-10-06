package CSV;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;

public class CSVURLReader
{
	private String separator;

	private BufferedReader br;

	private String columns[];

	private int line = 0;
	private int lines = 0;

	public int getLine()
	{
		return line;
	}

	public int getLinesCount()
	{
		return lines;
	}

	public CSVURLReader(String downloadURL, String separator) throws IOException
	{
//		System.out.println(downloadURL);
        URL website = new URL(downloadURL);
        URLConnection connection = website.openConnection();
//		System.out.println(downloadURL);
        BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

		while (reader.readLine() != null) lines++;
		reader.close();

		this.separator = separator;

        connection = website.openConnection();
		this.br = new BufferedReader(new InputStreamReader(connection.getInputStream()));

		line++;

		String firstLine = br.readLine();

		this.columns = firstLine.split(separator);
//		while(line<156000)
//		{
//			line++;
//			br.readLine();
//		}
	}

	public HashMap<String, ArrayList<String>> nextLine() throws CSVReaderException
	{
		HashMap<String, ArrayList<String>> result = new HashMap<String, ArrayList<String>>();

		line++;

		String nextLine;

		String values[];
		try
		{
			nextLine = br.readLine();
			values = nextLine.split(separator);
//			System.out.println(values.length + " : " + Arrays.toString(values));
		}
		catch (IOException e)
		{
			throw new CSVReaderException(e);
		}

//		if(values.length != columns.length)
//			throw new CSVFileReaderException("Values count different from column count at line " + line + ": " + values.length +  " != " +  columns.length);
//
//		for(int i = 0; i < columns.length; i++)
		for(int i = 0; i < values.length && i < columns.length; i++)
		{
			ArrayList<String> value = new ArrayList<String>();
			value.add(values[i]);
			result.put(columns[i], value);
		}

		return result;
	}
}
