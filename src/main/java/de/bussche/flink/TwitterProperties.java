package de.bussche.flink;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class TwitterProperties extends Properties implements Serializable {

    private Properties prop = null;

    public TwitterProperties() throws FileNotFoundException {
        try {
            String propFileName = "settings.properties";
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
            this.prop = new Properties();
            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
        } catch (Exception e) {

        }
    }

    public Properties getProperties() {
        return this.prop;
    }

    public String getPropertyAsString(String propertyName) {
        return this.prop.getProperty(propertyName);
    }

    public int getPropertyAsInt(String propertyName) {
        return Integer.parseInt(this.prop.getProperty(propertyName));
    }
}
