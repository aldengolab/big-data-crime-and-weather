package agolab.CrimeWeatherApp.IngestCrime;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.apache.commons.csv.CSVFormat;
import agolab.CrimeAndWeatherApp.crimeEvent.CrimeEvent;


public abstract class CrimeEventProcessor {
	static class MissingDataException extends Exception {

	    public MissingDataException(String message) {
	        super(message);
	    }

	    public MissingDataException(String message, Throwable throwable) {
	        super(message, throwable);
	    }

	}
	
	static double tryToReadMeasurement(String name, String s, String missing) throws MissingDataException {
		if(s.equals(missing))
			throw new MissingDataException(name + ": " + s);
		return Double.parseDouble(s.trim());
	}

	void processRecord(CSVRecord record, File file) throws IOException {
		try {
			processCrimeEvent(crimeFromRecord(record), file);
		} catch(MissingDataException e) {
			// Ignore lines with missing data just in case; EXCEL format should take care of this
		}
	}

	abstract void processCrimeEvent(CrimeEvent event, File file) throws IOException;
	BufferedReader getFileReader(File file) throws FileNotFoundException, IOException {
		return new BufferedReader(new InputStreamReader(new FileInputStream(file)));
	}
	
	void processCrimeFile(String filepath) throws IOException {
		File file = new File(filepath);
		CSVParser reader = new CSVParser(new FileReader(file),CSVFormat.EXCEL);
		for (CSVRecord record : reader) {
			processRecord(record, file);
		}
	}

	CrimeEvent crimeFromRecord(CSVRecord record) throws NumberFormatException, MissingDataException {
		DateTime date = DateTime.parse(record.get(2), 
                DateTimeFormat.forPattern("dd/MM/yyyy h:mm:ss a"));
		CrimeEvent event 
			= new CrimeEvent((long) Integer.parseInt(record.get(0)), (long) date.getYear(), (long) date.getMonthOfYear(), 
					(long) date.getDayOfMonth(), (long) date.getHourOfDay(), record.get(5), record.get(6),
					"Chicago", "IL");
		if (!record.get(19).equals("")) {
			event.latitude = (long) Integer.parseInt(record.get(19));
			event.longitude = (long) Integer.parseInt(record.get(20));
		}
		return event;
	}

}
