package CSV;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class CSVFileReader
{
	private String separator;

	private FileReader fr;
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

	public CSVFileReader(String filePath, String separator) throws IOException
	{
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		while (reader.readLine() != null) lines++;
		reader.close();

		this.separator = separator;

		this.fr = new FileReader(filePath);
		this.br = new BufferedReader(fr);

		line++;

		String firstLine = br.readLine();

		this.columns = firstLine.split(separator);
	}

	public HashMap<String, ArrayList<String>> nextLine() throws CSVFileReaderException
	{
		HashMap<String, ArrayList<String>> result = new HashMap<String, ArrayList<String>>();

		line++;

		String nextLine;

		String values[];
		try
		{
			nextLine = br.readLine();
			values = nextLine.split(separator);
		}
		catch (IOException e)
		{
			throw new CSVFileReaderException(e);
		}

		if(values.length != columns.length)
			throw new CSVFileReaderException("Values count different from column count at line " + line);

		for(int i = 0; i< columns.length; i++)
		{
			ArrayList<String> value = new ArrayList<String>();
			value.add(values[i]);
			result.put(columns[i], value);
		}

		return result;
	}
}
