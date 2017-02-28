package org.jonathanrice.xmldatamigrator.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class PropertyUtil {

	protected static Properties prop = null;
	protected static String propFileName;
	
	public static void setPropFileName(String propFileName) {
		PropertyUtil.propFileName = propFileName;
	}
	
	public static Properties getProperties() {
		if (prop == null) {
			prop = new Properties();
			try {
				prop.load(new FileInputStream(PropertyUtil.propFileName));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return prop;
	}
}
