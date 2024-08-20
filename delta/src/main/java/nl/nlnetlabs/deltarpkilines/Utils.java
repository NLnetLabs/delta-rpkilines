package nl.nlnetlabs.deltarpkilines;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class Utils {
    private static Properties properties = new Properties();

    public static Document parseXml(String xml) throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        InputSource is = new InputSource(new StringReader(xml));
        Document doc = builder.parse(is);
        return doc;
    }

    public static void initSettings() throws FileNotFoundException, IOException {
        properties.load(new FileInputStream("app.properties"));
    }

    public static String getJdbcString() {
        return "jdbc:postgresql://" + properties.getProperty("dbHost") + "/" + 
            properties.getProperty("dbName") + "?user=" + properties.getProperty("dbUser") + 
            "&password=" + properties.getProperty("dbPass");
    }

    public static String getRoutinatorUrl() {
        return properties.getProperty("routinatorUrl");
    }
}
