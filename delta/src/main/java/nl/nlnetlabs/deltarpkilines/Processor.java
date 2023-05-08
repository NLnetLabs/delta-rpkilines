package nl.nlnetlabs.deltarpkilines;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.digest.DigestUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Processor {
    private Connection connection;
    private Queue<NotificationItem> queue = new LinkedBlockingQueue<>();
    private Timer timer;

    // private static final Lock databaseLock = new ReentrantLock();

    public void addToQueue(NotificationItem item) {
        queue.offer(item);
    }

    public Processor() {
        this.timer = new Timer("Timer-Processor");

        try {
            this.setupDatabase();
        } catch (SQLException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                while (true) {
                    NotificationItem item = queue.poll();
                    if (item == null) {
                        break;
                    }
                    try {
                        Document doc = Utils.parseXml(item.getDocument());
                        Element root = doc.getDocumentElement();
                        System.out.println("Process " + root.getTagName() + ": " + item.getUri());
                        if ("removed".equals(root.getTagName())) {
                            addEvent("removed", item.getTimestamp(), item.getUri(), item.getHash(), item.getPublicationPoint());
                            removeObjects(item.getPublicationPoint(), item.getTimestamp());
                        } else if ("snapshot".equals(root.getTagName())) {
                            addEvent("snapshot", item.getTimestamp(), item.getUri(), item.getHash(), item.getPublicationPoint());
                            removeObjects(item.getPublicationPoint(), item.getTimestamp());

                            NodeList publishes = root.getElementsByTagName("publish");
                            for (int i = 0; i < publishes.getLength(); i++) {
                                Node child = publishes.item(i);
                                if (child instanceof Element) {
                                    Element childElement = (Element)child;
                                    String uri = childElement.getAttribute("uri");
                                    String content = childElement.getTextContent();
                                    String hash = DigestUtils.sha256Hex(Base64.getDecoder().decode(content.replaceAll("\\s", "")));
                                    addObject(content, item.getTimestamp(), hash, uri, item.getPublicationPoint());
                                }
                            }
                        } else if ("delta".equals(root.getTagName())) {
                            addEvent("delta", item.getTimestamp(), item.getUri(), item.getHash(), item.getPublicationPoint());
                            NodeList withdraws = root.getElementsByTagName("withdraw");
                            for (int i = 0; i < withdraws.getLength(); i++) {
                                Node child = withdraws.item(i);
                                if (child instanceof Element) {
                                    Element childElement = (Element)child;
                                    String uri = childElement.getAttribute("uri");
                                    String hash = childElement.getAttribute("hash");
                                    removeObject(uri, hash, item.getTimestamp());
                                }
                            }

                            NodeList publishes = root.getElementsByTagName("publish");
                            for (int i = 0; i < publishes.getLength(); i++) {
                                Node child = publishes.item(i);
                                if (child instanceof Element) {
                                    Element childElement = (Element)child;
                                    String uri = childElement.getAttribute("uri");
                                    String content = childElement.getTextContent();
                                    String hash = DigestUtils.sha256Hex(Base64.getDecoder().decode(content.replaceAll("\\s", "")));
                                    addObject(content, item.getTimestamp(), hash, uri, item.getPublicationPoint());
                                }
                            }
                        } else {
                            throw new IOException("No idea what's going on here");
                        }
                        commit();
                    } catch (ParserConfigurationException | SAXException | IOException | SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        System.gc();
                    }
                }           
            }
        }, 0, 5);
    }

    public void addStartup(long timestamp) {
        try {
            addEvent("startup", timestamp, null, null, null);
            removeObjects(timestamp);
            commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void addObject(String content, long timestamp, String hash, String uri, String publicationPoint) throws SQLException {
        removeObject(uri, timestamp);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO objects (content, visibleOn, hash, uri, publicationPoint) VALUES (?, ?, ?, ?, ?)")) {
            statement.setString(1, content);
            statement.setLong(2, timestamp);
            statement.setString(3, hash);
            statement.setString(4, uri);
            statement.setString(5, publicationPoint);
            statement.executeUpdate();
        }
    }

    private void removeObject(String uri, String hash, long timestamp) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("UPDATE objects SET disappearedOn = ? WHERE uri = ? AND hash = ? AND disappearedOn IS NULL")) {
            statement.setLong(1, timestamp);
            statement.setString(2, uri);
            statement.setString(3, hash);
            statement.executeUpdate();
        }
    }

    private void removeObject(String uri, long timestamp) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("UPDATE objects SET disappearedOn = ? WHERE uri = ? AND disappearedOn IS NULL")) {
            statement.setLong(1, timestamp);
            statement.setString(2, uri);
            statement.executeUpdate();
        }
    }

    private void removeObjects(String publicationPoint, long timestamp) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("UPDATE objects SET disappearedOn = ? WHERE publicationPoint = ? AND disappearedOn IS NULL")) {
            statement.setLong(1, timestamp);
            statement.setString(2, publicationPoint);
            statement.executeUpdate();
        }
    }

    private void removeObjects(long timestamp) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("UPDATE objects SET disappearedOn = ? WHERE disappearedOn IS NULL")) {
            statement.setLong(1, timestamp);
            statement.executeUpdate();
        }
    }

    private void addEvent(String event, long timestamp, String uri, String hash, String publicationPoint) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO events (event, timestamp, uri, hash, publicationPoint) VALUES (?, ?, ?, ?, ?)")) {
            statement.setString(1, event);
            statement.setLong(2, timestamp);
            statement.setString(3, uri);
            statement.setString(4, hash);
            statement.setString(5, publicationPoint);
            statement.executeUpdate();
        }
    }

    private void commit() throws SQLException {
        connection.commit();
    }

    private void setupDatabase() throws SQLException {
        connection = DriverManager.getConnection("jdbc:sqlite:rpki.db");
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(30);

        // statement.executeUpdate("DROP TABLE IF EXISTS objects");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS objects (content TEXT, visibleOn INTEGER, disappearedOn INTEGER, hash TEXT, uri TEXT, publicationPoint TEXT)");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_objects_uri ON objects (uri)");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_objects_publicationPoint ON objects (publicationPoint)");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_objects_visibleOn ON objects (visibleOn)");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_objects_disappearedOn ON objects (disappearedOn)");

        // statement.executeUpdate("DROP TABLE IF EXISTS events");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS events (event TEXT, timestamp INTEGER, uri TEXT, hash TEXT, publicationPoint TEXT)");
    }
}
