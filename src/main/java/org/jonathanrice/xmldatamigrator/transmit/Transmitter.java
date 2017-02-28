package org.jonathanrice.xmldatamigrator.transmit;

import org.apache.log4j.Logger;
import org.jonathanrice.xmldatamigrator.consumer.HeaderDetailVelocityConsumer;
import org.jonathanrice.xmldatamigrator.util.PropertyUtil;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Transmitter {
	private static Logger logger = Logger.getLogger(HeaderDetailVelocityConsumer.class);
	
	public Transmitter() {
	}
	
	public boolean transmitXMLMessage(String headerKey, String xmlMessage) throws IOException {
		String transmissionType = PropertyUtil.getProperties().getProperty("tranmission_type");
		if (transmissionType.equals("http")) {
			return postMessageViaHttp(headerKey, xmlMessage);
		}
		if (transmissionType.equals("file")) {
			return postMessageViaFile(headerKey, xmlMessage);
		}
		if (transmissionType.equals("stdout")) {
			return postMessageViaStdOut(headerKey, xmlMessage);	
		}
		return false;
	}

	protected boolean postMessageViaStdOut(String headerKey, String xmlMessage) {
		System.out.println(xmlMessage);
		return true;
	}

	protected boolean postMessageViaFile(String headerKey, String xmlMessage) throws IOException {
		String folder = PropertyUtil.getProperties().getProperty("file.folder");
		String fileName = folder + "/" + headerKey + ".xml";
		File xmlFile = new File(fileName);
		xmlFile.getParentFile().mkdirs();
		FileWriter fileWriter = new FileWriter(xmlFile);
		fileWriter.write(xmlMessage);
        fileWriter.close();
        return true;
	}

	protected boolean postMessageViaHttp(String headerKey, String xmlStr) {
		HttpURLConnection urlConn = null;
		URL url = null;
		String httpUrl = PropertyUtil.getProperties().getProperty("http.url");
		try {
			url = new URL(httpUrl);
			urlConn = (HttpURLConnection) url.openConnection();

			urlConn.setDoInput(true);
			urlConn.setDoOutput(true);

			urlConn.setUseCaches(false);
			urlConn.setRequestMethod("POST");
			urlConn.connect();

			DataOutputStream output = null;
			output = new DataOutputStream(urlConn.getOutputStream());
			String content = URLEncoder.encode(xmlStr, "UTF8");

			output.writeBytes(content);
			output.flush();
			output.close();

			// Get Response
			InputStream is;
			int resp = urlConn.getResponseCode();
			if (resp >= 400) {
			    is = urlConn.getErrorStream();
			} else {
			    is = urlConn.getInputStream();
			}

			BufferedReader rd = new BufferedReader(new InputStreamReader(is));
			String line;
			StringBuffer response = new StringBuffer();
			while ((line = rd.readLine()) != null) {
				response.append(line);
			}
			rd.close();
			// if you want to save the response
			if (resp >= 400) {
				String strResponse = response.toString();
				Pattern pattern = Pattern.compile("CDATA\\[(.+)\\]");
				Matcher matcher = pattern.matcher(strResponse);

				List<String> listMatches = new ArrayList<String>();

				while(matcher.find())
					{
						listMatches.add(matcher.group(2));
					}

				for(String s : listMatches)
					{
						logger.error(headerKey + ": " + s);
					}
				return false;
			} else {
				logger.debug("Sucess posting " + headerKey + " response: " + response.toString());
				return true;
			}
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
			return false;
		} finally {
			if (urlConn != null) {
				urlConn.disconnect();
			}
		}
	}
}
