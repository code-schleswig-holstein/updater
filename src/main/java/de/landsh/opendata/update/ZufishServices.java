package de.landsh.opendata.update;

import de.landsh.opendata.DatasetUpdate;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;

public class ZufishServices implements Generator {

    private final URL url;

    public ZufishServices(DatasetUpdate update) throws MalformedURLException {
        this.url = new URL(update.getOriginalURL());
    }

    @Override
    public boolean generateDistributions(File directory) throws Exception {
        final InputStream is = url.openStream();
        final JSONArray json = new JSONArray(new JSONTokener(is));

        final PrintStream outCSV = new PrintStream(new FileOutputStream(new File(directory, "online-services.csv")));
        outCSV.println("id,url,name,deliveryChannel,hasPaymentMethod,trustLevel");

        JSONArray output = new JSONArray();

        for (int i = 0; i < json.length(); i++) {
            final JSONObject os = json.getJSONObject(i).getJSONObject("object");
            final String id = os.get("id").toString();
            final String link = os.getString("link");
            final String name = os.getString("name");

            String deliveryChannel = "";
            if (os.has("deliveryChannel")
                    && os.getJSONObject("deliveryChannel").has("communicationSystem")
                    && os.getJSONObject("deliveryChannel").getJSONObject("communicationSystem").has("type")) {
                deliveryChannel = os.getJSONObject("deliveryChannel").getJSONObject("communicationSystem").getJSONObject("type").getString("key");
            }

            boolean hasPaymentMethod = !os.getJSONArray("paymentMethods").isEmpty();
            final String trustLevel = os.getString("trustLevel");

            final JSONObject entry = new JSONObject();
            entry.put("id", id);
            entry.put("link", link);
            entry.put("name", name);
            entry.put("deliveryChannel", deliveryChannel);
            entry.put("hasPaymentMethod", hasPaymentMethod);
            entry.put("trustLevel", trustLevel);
            output.put(entry);

            outCSV.println(id + "," + link + ",\"" + name + "\"," + deliveryChannel + "," + hasPaymentMethod + "," + trustLevel);

        }

        final FileWriter jsonWriter = new FileWriter(new File(directory, "online-services.json"));
        output.write(jsonWriter);
        jsonWriter.close();

        outCSV.close();
        return true;
    }
}
