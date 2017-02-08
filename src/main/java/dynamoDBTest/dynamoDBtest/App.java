package dynamoDBTest.dynamoDBtest;

import dynamoDBTest.dynamoDBtest.MoviesLoadData;
import dynamoDBTest.dynamoDBtest.Operations;
import dynamoDBTest.dynamoDBtest.CreateTable;
import dynamoDBTest.dynamoDBtest.QueryAndScan;
/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
//		CreateTable createTable=new CreateTable();
//		createTable.createTable();
//		
//		MoviesLoadData loader=new MoviesLoadData();
//		loader.loadData();

//		Operations operator=new Operations();
//		operator.insertItem();
//		System.out.println("load data success!!");
//		
//		operator.deleteItem();
//		System.out.println("delete data success!!");
		
		
		QueryAndScan query=new QueryAndScan();
//		query.query();
		query.scan();

		
		
		
	}
}
