package dynamoDBTest.dynamoDBtest;
import java.util.HashMap;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

public class QueryAndScan {
	public void query(){
		AmazonDynamoDBClient client = new AmazonDynamoDBClient()
        .withEndpoint("http://localhost:8000");

    DynamoDB dynamoDB = new DynamoDB(client);

    Table table = dynamoDB.getTable("Movies");

    HashMap<String, String> nameMap = new HashMap<String, String>();
    nameMap.put("#yr", "year");

    HashMap<String, Object> valueMap = new HashMap<String, Object>();
    valueMap.put(":yyyy", 2013);

    QuerySpec querySpec = new QuerySpec()
        .withKeyConditionExpression("#yr =:yyyy")
        .withNameMap(nameMap)
        .withValueMap(valueMap);

    ItemCollection<QueryOutcome> items = null;
    Iterator<Item> iterator = null;
    Item item = null;

    try {
        System.out.println("Movies from 1985");
        items = table.query(querySpec);

        iterator = items.iterator();
        while (iterator.hasNext()) {
            item = iterator.next();
            System.out.println(item.getNumber("year") + ": "
                    + item.getString("title"));
        }

    } catch (Exception e) {
        System.err.println("Unable to query movies from 1985");
        System.err.println(e.getMessage());
    }

    valueMap.put(":yyyy", 1992);
    valueMap.put(":letter1", "A");
    valueMap.put(":letter2", "L");

    querySpec
            .withProjectionExpression(
                    "#yr, title, info.genres, info.actors[0]")
            .withKeyConditionExpression(
                    "#yr = :yyyy and title between :letter1 and :letter2")
            .withNameMap(nameMap).withValueMap(valueMap);

    try {
        System.out
                .println("Movies from 1992 - titles A-L, with genres and lead actor");
        items = table.query(querySpec);

        iterator = items.iterator();
        while (iterator.hasNext()) {
            item = iterator.next();
            System.out.println(item.getNumber("year") + ": "
                    + item.getString("title") + " " + item.getMap("info"));
        }

    } catch (Exception e) {
        System.err.println("Unable to query movies from 1992:");
        System.err.println(e.getMessage());
    }
	}
	
	public void scan(){
        AmazonDynamoDBClient client = new AmazonDynamoDBClient()
        .withEndpoint("http://localhost:8000");

    DynamoDB dynamoDB = new DynamoDB(client);

    Table table = dynamoDB.getTable("Movies");

    ScanSpec scanSpec = new ScanSpec()
        .withProjectionExpression("#yr, title, info.rating")
        .withFilterExpression("#yr between :start_yr and :end_yr")
        .withNameMap(new NameMap().with("#yr",  "year"))
        .withValueMap(new ValueMap().withNumber(":start_yr", 1950).withNumber(":end_yr", 1959));

    try {
        ItemCollection<ScanOutcome> items = table.scan(scanSpec);

        Iterator<Item> iter = items.iterator();
        while (iter.hasNext()) {
            Item item = iter.next();
            System.out.println(item.toString());
        }
        
    } catch (Exception e) {
        System.err.println("Unable to scan the table:");
        System.err.println(e.getMessage());
    }
	}
	
	

}
