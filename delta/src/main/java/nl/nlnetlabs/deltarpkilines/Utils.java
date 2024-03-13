package nl.nlnetlabs.deltarpkilines;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Properties;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class Utils {
    private static AmazonS3 s3 = null;
    private static String s3bucket = "";
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

    public static void initS3() {
        if (s3 == null) {
            EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(properties.getProperty("endpoint"), properties.getProperty("region"));
            AWSCredentials credentials = new BasicAWSCredentials(properties.getProperty("accessKey"), properties.getProperty("secretKey"));
            s3 = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withPathStyleAccessEnabled(true)
                .build();
            s3bucket = properties.getProperty("bucket");
        }
    }

    public static void uploadFile(String filename, byte[] data) throws IOException {
        initS3();
        if (!s3.doesObjectExist(s3bucket, filename)) {
            try (InputStream inputStream = new ByteArrayInputStream(data)) {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(data.length);
                PutObjectRequest putObjectRequest = new PutObjectRequest(s3bucket, filename, inputStream, metadata);
                s3.putObject(putObjectRequest);
            }
        }
    }
}
