package nl.nlnetlabs.deltarpkilines;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class Fetcher {
    private Processor processor;

    private String baseUrl;
    private String sha1;
    private String hexUrl;
    private CloseableHttpClient client;
    private Timer timer;

    private long lastSeenSerial;

    private String savePath = "./data/";

    public Fetcher(Processor processor, String url, int delay, int frequency) {
        this.processor = processor;

        this.baseUrl = url;
        this.sha1 = DigestUtils.sha1Hex(url);
        this.client = HttpClients.custom().setUserAgent("NLnet Labs Delta RPKIlines 0.3.0").build();
        this.timer = new Timer("Timer-Fetcher-" + url);
        this.lastSeenSerial = 0;

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    Document doc = makeXmlRequest(baseUrl);
                    Element root = doc.getDocumentElement();
                    long serial = Long.parseLong(root.getAttribute("serial"));

                    XPathFactory xpathFactory = XPathFactory.newInstance();
                    XPath xpath = xpathFactory.newXPath();

                    if (serial < lastSeenSerial) {
                        // The serial decreased, that indicates something is going wrong, revert to serial 0
                        // This will redownload the snapshot
                        lastSeenSerial = 0;
                    }
                    for (long i = lastSeenSerial + 1; i <= serial; i++) {
                        Element deltaElement = (Element) xpath.evaluate(String.format("/notification/delta[@serial='%s']", i), doc, XPathConstants.NODE);
                        long currentTimeMillis = System.currentTimeMillis();
                        if (deltaElement != null) {
                            String uri = deltaElement.getAttribute("uri");
                            String hash =  deltaElement.getAttribute("hash");
                            // downloadFile(uri, savePath + sha1 + "-" + System.currentTimeMillis() + "-delta-" + i + ".xml");
                            processor.addToQueue(new NotificationItem(makeRequest(uri), currentTimeMillis, hash, uri, baseUrl));
                        } else {
                            Element snapshotElement = (Element) xpath.evaluate("/notification/snapshot", doc, XPathConstants.NODE);
                            String uri = snapshotElement.getAttribute("uri");
                            String hash =  snapshotElement.getAttribute("hash");
                            // downloadFile(uri, savePath + sha1 + "-" + System.currentTimeMillis() + "-snapshot.xml");
                            processor.addToQueue(new NotificationItem(makeRequest(uri), currentTimeMillis, hash, uri, baseUrl));
                            break;
                        }
                    }
                    lastSeenSerial = serial;
                    System.out.println("Fetch: " + url);
                } catch (IOException | ParserConfigurationException | UnsupportedOperationException | SAXException | XPathExpressionException | OutOfMemoryError e) {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    e.printStackTrace(pw);
                    System.out.println("Fetch: " + url + "\n" + sw.toString());
                } finally {
                    System.gc();
                }
            }
        }, delay, frequency * 1000);

    }

    public void stop() {
        processor.addToQueue(new NotificationItem("<removed></removed>", System.currentTimeMillis(), "", "", baseUrl));
        this.timer.cancel();
        this.timer = null;
        try {
            this.client.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private boolean downloadFile(String url, String output) throws FileNotFoundException, IOException {
        try (CloseableHttpResponse response = client.execute(new HttpGet(url))) {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try (FileOutputStream outstream = new FileOutputStream(output)) {
                    entity.writeTo(outstream);
                    return true;
                }
            }
        }      
        return false;  
    }

    private String makeRequest(String url) throws IOException {
        HttpGet request = new HttpGet(url);
        CloseableHttpResponse response = client.execute(request);

        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw new IOException("Entity is null");
        }
        String result = new String(entity.getContent().readAllBytes(), StandardCharsets.UTF_8);
        response.close();
        return result;
    }

    private Document makeXmlRequest(String url)
            throws IOException, ParserConfigurationException, UnsupportedOperationException, SAXException {
        String result = makeRequest(url);
        return Utils.parseXml(result);
    }

    private void compressStringToGzip(String data, Path target) throws IOException {
        try (GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(target.toFile()))) {
            gos.write(data.getBytes(StandardCharsets.US_ASCII));
        }
    }
}
