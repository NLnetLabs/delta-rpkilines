package nl.nlnetlabs.deltarpkilines;

import java.io.IOException;
import java.sql.*;
import java.util.Base64;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Processor {
    private BasicDataSource dataSource;
    private Queue<NotificationItem> queue = new LinkedBlockingQueue<>();
    private Timer timer;

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
            System.exit(1);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
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
                        Connection connection = getConnection();

                        Document doc = Utils.parseXml(item.getDocument());
                        Element root = doc.getDocumentElement();
                        System.out.println("Process " + root.getTagName() + ": " + item.getUri());
                        if ("removed".equals(root.getTagName())) {
                            addEvent(connection, "removed", item.getTimestamp(), item.getUri(), item.getHash(), item.getPublicationPoint());
                            removeObjects(connection, item.getPublicationPoint(), item.getTimestamp());
                        } else if ("snapshot".equals(root.getTagName())) {
                            addEvent(connection, "snapshot", item.getTimestamp(), item.getUri(), item.getHash(), item.getPublicationPoint());
                            removeObjects(connection, item.getPublicationPoint(), item.getTimestamp());

                            NodeList publishes = root.getElementsByTagName("publish");
                            for (int i = 0; i < publishes.getLength(); i++) {
                                Node child = publishes.item(i);
                                if (child instanceof Element) {
                                    Element childElement = (Element)child;
                                    String uri = childElement.getAttribute("uri");
                                    String content = childElement.getTextContent();
                                    byte[] rawContent = Base64.getDecoder().decode(content.replaceAll("\\s", ""));
                                    String hash = DigestUtils.sha256Hex(rawContent);
                                    Utils.uploadFile(hash, rawContent);
                                    addObject(connection, content, item.getTimestamp(), hash, uri, item.getPublicationPoint());
                                }
                            }
                        } else if ("delta".equals(root.getTagName())) {
                            addEvent(connection, "delta", item.getTimestamp(), item.getUri(), item.getHash(), item.getPublicationPoint());
                            NodeList withdraws = root.getElementsByTagName("withdraw");
                            for (int i = 0; i < withdraws.getLength(); i++) {
                                Node child = withdraws.item(i);
                                if (child instanceof Element) {
                                    Element childElement = (Element)child;
                                    String uri = childElement.getAttribute("uri");
                                    String hash = childElement.getAttribute("hash");
                                    removeObject(connection, uri, hash, item.getTimestamp());
                                }
                            }

                            NodeList publishes = root.getElementsByTagName("publish");
                            for (int i = 0; i < publishes.getLength(); i++) {
                                Node child = publishes.item(i);
                                if (child instanceof Element) {
                                    Element childElement = (Element)child;
                                    String uri = childElement.getAttribute("uri");
                                    String content = childElement.getTextContent();
                                    byte[] rawContent = Base64.getDecoder().decode(content.replaceAll("\\s", ""));
                                    String hash = DigestUtils.sha256Hex(rawContent);
                                    Utils.uploadFile(hash, rawContent);
                                    addObject(connection, content, item.getTimestamp(), hash, uri, item.getPublicationPoint());
                                }
                            }
                        } else {
                            throw new IOException("No idea what's going on here");
                        }
                        commit(connection);
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
            Connection connection = getConnection();
            addEvent(connection, "startup", timestamp, null, null, null);
            removeObjects(connection, timestamp);
            commit(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void addObject(Connection connection, String content, long timestamp, String hash, String uri, String publicationPoint) throws SQLException {
        removeObject(connection, uri, timestamp);
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO objects (content, visibleOn, hash, uri, publicationPoint) VALUES (?, ?, ?, ?, ?)")) {
            statement.setString(1, content);
            statement.setLong(2, timestamp);
            statement.setString(3, hash);
            statement.setString(4, uri);
            statement.setString(5, publicationPoint);
            statement.executeUpdate();
        }
    }

    private void removeObject(Connection connection, String uri, String hash, long timestamp) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("UPDATE objects SET disappearedOn = ? WHERE uri = ? AND hash = ? AND disappearedOn IS NULL")) {
            statement.setLong(1, timestamp);
            statement.setString(2, uri);
            statement.setString(3, hash);
            statement.executeUpdate();
        }
    }

    private void removeObject(Connection connection, String uri, long timestamp) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("UPDATE objects SET disappearedOn = ? WHERE uri = ? AND disappearedOn IS NULL")) {
            statement.setLong(1, timestamp);
            statement.setString(2, uri);
            statement.executeUpdate();
        }
    }

    private void removeObjects(Connection connection, String publicationPoint, long timestamp) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("UPDATE objects SET disappearedOn = ? WHERE publicationPoint = ? AND disappearedOn IS NULL")) {
            statement.setLong(1, timestamp);
            statement.setString(2, publicationPoint);
            statement.executeUpdate();
        }
    }

    private void removeObjects(Connection connection, long timestamp) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("UPDATE objects SET disappearedOn = ? WHERE disappearedOn IS NULL")) {
            statement.setLong(1, timestamp);
            statement.executeUpdate();
        }
    }

    private void addEvent(Connection connection, String event, long timestamp, String uri, String hash, String publicationPoint) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO events (event, timestamp, uri, hash, publicationPoint) VALUES (?, ?, ?, ?, ?)")) {
            statement.setString(1, event);
            statement.setLong(2, timestamp);
            statement.setString(3, uri);
            statement.setString(4, hash);
            statement.setString(5, publicationPoint);
            statement.executeUpdate();
        }
    }

    private void commit(Connection connection) throws SQLException {
        connection.commit();
        connection.close();
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    private void setupDatabase() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        dataSource = new BasicDataSource();
        dataSource.setUrl(Utils.getJdbcString());
        dataSource.setDefaultAutoCommit(false);

        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(30);

        // statement.executeUpdate("DROP TABLE IF EXISTS objects");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS objects (content TEXT, visibleOn NUMERIC, disappearedOn NUMERIC, hash TEXT, uri TEXT, publicationPoint TEXT)");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_objects_uri ON objects (uri)");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_objects_publicationPoint ON objects (publicationPoint)");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_objects_visibleOn ON objects (visibleOn)");
        statement.executeUpdate("CREATE INDEX IF NOT EXISTS idx_objects_disappearedOn ON objects (disappearedOn)");

        // statement.executeUpdate("DROP TABLE IF EXISTS events");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS events (event TEXT, timestamp NUMERIC, uri TEXT, hash TEXT, publicationPoint TEXT)");

        commit(connection);
    }
}
