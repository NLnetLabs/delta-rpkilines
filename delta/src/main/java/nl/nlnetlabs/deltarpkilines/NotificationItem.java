package nl.nlnetlabs.deltarpkilines;

public class NotificationItem {
    private String document;
    private long timestamp;
    private String hash;
    private String uri;
    private String publicationPoint;

    public NotificationItem(String document, long timestamp, String hash, String uri, String publicationPoint) {
        this.document = document;
        this.timestamp = timestamp;
        this.hash = hash;
        this.uri = uri;
        this.publicationPoint = publicationPoint;
    }

    public String getDocument() {
        return document;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getHash() {
        return hash;
    }

    public String getUri() {
        return uri;
    }

    public String getPublicationPoint() {
        return publicationPoint;
    }
}
