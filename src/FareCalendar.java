import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.apache.xpath.operations.Bool;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by sreerajk on 27/04/14.
 */
public class FareCalendar {
    private static AWSCredentials credentials;
    private static AmazonDynamoDBClient dbClient;

    public static void main(String[] args) throws IOException, ParseException {
        credentials = new PropertiesCredentials(new File("credential.properties"));
        dbClient = new AmazonDynamoDBClient(credentials);
        dbClient.setEndpoint("https://dynamodb.us-west-2.amazonaws.com");
        Map<String, String> flightMap = new HashMap<String, String>();
        final WebClient webClient = new WebClient();
        webClient.waitForBackgroundJavaScript(2000);
        webClient.getOptions().setJavaScriptEnabled(false);
        final HtmlPage page = webClient.getPage("http://flights.makemytrip.com/makemytrip/fareCalAjax.do?month=6&from=HYD&to=COK&adult=1&child=0&infant=0");
        final String pageAsXml = page.asXml();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd.MMM.yyyy");
        Document document = Jsoup.parse(pageAsXml);
        Elements elements = document.select("tbody > tr > td.save_td");
        for (Element element : elements) {
            String date[] = element.select("div.fade > span.date_txt").text().split(" ");
            Date flightDate = simpleDateFormat.parse(date[1] + "." + date[0].substring(0, 3) + ".2014"); //Todo: year hardcoded
            String price = element.select("div.fade > span.save_price").text().split(" ")[1];
            Integer priceInt = NumberFormat.getNumberInstance(java.util.Locale.US).parse(price).intValue();
            flightMap.put(flightDate.toString(), String.valueOf(priceInt));
        }
        Map<String, String> message = queryDynamo(flightMap);
        messageMe(message);
        // writeToDynamo(flightMap);
    }

    public static void messageMe(Map<String, String> message) {
       if(message.size()>1){
           System.out.print("Multiple flight price drop: ");
           for(Map.Entry<String,String> entry: message.entrySet()){
               System.out.print(entry.getKey().substring(0,10)+" Rs "+entry.getValue()+", ");
           }
       }
        else if (message.size()==1){
           System.out.println("Flight price drop "+message.toString());
       }
    }

    public static Map<String, String> queryDynamo(Map<String, String> flightMap) {
        String tableName = "FareCalendar";
        Iterator it = flightMap.entrySet().iterator();
        Map<String, String> messageMap = new HashMap<String, String>();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry) it.next();
            Condition hashKeyCondition = new Condition()
                    .withComparisonOperator(ComparisonOperator.EQ)
                    .withAttributeValueList(new AttributeValue().withS((String) pairs.getKey()));

            Map<String, Condition> keyConditions = new HashMap<String, Condition>();
            keyConditions.put("DateOfJourney", hashKeyCondition);

            QueryRequest queryRequest = new QueryRequest()
                    .withTableName(tableName)
                    .withLimit(1)
                    .withKeyConditions(keyConditions);
            queryRequest.setScanIndexForward(false);
            QueryResult result = dbClient.query(queryRequest);
            for (Map<String, AttributeValue> item : result.getItems()) {
                if (Integer.parseInt((String) pairs.getValue()) < Integer.parseInt(item.get("Price").getN())) {
                    messageMap.put((String) pairs.getKey(), (String) pairs.getValue());
                }
            }
        }
        return messageMap;
    }

    public static void writeToDynamo(Map<String, String> flightMap) throws IOException {
        String tableName = "FareCalendar";
        Iterator it = flightMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry) it.next();
            Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
            item.put("DateOfJourney", new AttributeValue().withS((String) pairs.getKey()));
            item.put("TimeStamp", new AttributeValue().withS(new Date().toString()));
            item.put("Price", new AttributeValue().withN((String) pairs.getValue()));

            PutItemRequest putItemRequest = new PutItemRequest()
                    .withTableName(tableName)
                    .withItem(item);
            PutItemResult result = dbClient.putItem(putItemRequest);
            System.out.println(result.toString());
            it.remove(); // avoids a ConcurrentModificationException
        }
    }
}


