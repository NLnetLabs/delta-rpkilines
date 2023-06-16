package nl.nlnetlabs.deltarpkilines;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;


public class App {
    private HashMap<String, Fetcher> urls = new HashMap<>();

    public App() throws FileNotFoundException, IOException {
        Utils.initSettings();

        Processor processor = new Processor();
        processor.addStartup(System.currentTimeMillis());

        Timer timer = new Timer("Timer-App");
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
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
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 0, 60 * 60 * 1000);
    }

    private Set<String> fetchRrdpUrls() throws IOException {
        String baseUrl = "https://rpki-validator.ripe.net/api/v1/status";

        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet request = new HttpGet(baseUrl);
        CloseableHttpResponse response = httpClient.execute(request);
        
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw new IOException("Entity is null");
        }
        String result = EntityUtils.toString(entity);

        JSONObject obj = new JSONObject(result);
        JSONObject rrdp = obj.getJSONObject("rrdp");

        return rrdp.keySet();
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        App app = new App();
    }
}
