package nl.nlnetlabs.deltarpkilines;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


public class App {
    private HashMap<String, Fetcher> urls = new HashMap<>();

    public App() {
        Processor processor = new Processor();
        processor.addStartup(System.currentTimeMillis());

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                Set<String> rrdpUrls = fetchRrdpUrls();

                int frequency = 120; // Frequency in seconds
                int delay = Math.round((frequency * 1000) / rrdpUrls.size()); // Spread out equally over frequency

                int i = 0;
                for (String url : rrdpUrls) {
                    if (!urls.containsKey(url)) {
                        Fetcher fetcher = new Fetcher(processor, url, i * delay, frequency);
                        System.out.println("Added: " + url);
                        urls.put(url, fetcher);
                        i++;
                    }
                }
                for (String url : urls.keySet()) {
                    if (!rrdpUrls.contains(url)) {
                        Fetcher fetcher = urls.get(url);
                        System.out.println("Removed: " + url);
                        fetcher.stop();
                        urls.remove(url);
                    }
                }
            }
        }, 0, 60 * 60 * 1000);
    }

    private Set<String> fetchRrdpUrls() {
        // Location of the Routinator log used to obtain the RRDP URIs
        String baseUri = "https://ftp.ripe.net/rpki/%s.tal/%s/%s/%s/routinator.log";

        LocalDate now = LocalDate.now(ZoneId.of("UTC"));
        String year = now.format(DateTimeFormatter.ofPattern("yyyy"));
        String month = now.format(DateTimeFormatter.ofPattern("MM"));
        String day = now.format(DateTimeFormatter.ofPattern("dd"));
        String[] anchors = new String[] { "ripencc", "lacnic", "apnic", "arin", "afrinic" };
        Pattern pattern = Pattern.compile("^RRDP\\s(.+?)\\:\\s", Pattern.MULTILINE);

        HashSet<String> rrdpUrls = new HashSet<>();

        CloseableHttpClient httpClient = HttpClients.createDefault();

        for (String anchor : anchors) {
            // Obtain all RRDP URIs for all trust anchors
            try {
                String url = String.format(baseUri, anchor, year, month, day);
                HttpGet request = new HttpGet(url);
                CloseableHttpResponse response = httpClient.execute(request);

                HttpEntity entity = response.getEntity();
                if (entity == null) {
                    throw new IOException("Entity is null");
                }
                String result = EntityUtils.toString(entity);
                Matcher matcher = pattern.matcher(result);
                while (matcher.find()) {
                    rrdpUrls.add(matcher.group(1));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return rrdpUrls;
    }

    public static void main(String[] args) {
        App app = new App();
    }
}
