package iudx.connector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;

public class MetricsVerticle extends AbstractVerticle {
	
	private static final Logger logger = LoggerFactory.getLogger(MetricsVerticle.class.getName());
	private String COLLECTION;
	private JsonObject query, isotime;
	
	private MongoClient mongo;
	private JsonObject	mongoconfig;
	private String 		database_uri;
	private String 		database_host;
	private int 		database_port;
	private String 		database_user;
	private String 		database_password;
	private String 		database_name;

	
	@Override
	public void start() throws Exception {

		logger.info("Metrics Verticle started!");

		vertx.eventBus().consumer("update-metrics", message -> {
			logger.info("Got the event : " + message.body().toString());
			message.reply("SUCCESS");
			updateMetrics(message);
		});
		

		vertx.eventBus().consumer("get-metrics", message -> {
			logger.info("Got the event : " + message.body().toString());
			getMetrics(message);
		});

		Properties prop = new Properties();
	    InputStream input = null;

	    try {

	        input = new FileInputStream("config.properties");
	        prop.load(input);

	        database_user		=	prop.getProperty("database_user");
	        database_password	=	prop.getProperty("database_password");
	        database_host 		=	prop.getProperty("database_host");
	        database_port		=	Integer.parseInt(prop.getProperty("database_port"));
	        database_name		=	prop.getProperty("database_name");
	        

	        logger.info("database_user 	: " + database_user);
	        logger.info("database_password	: " + database_password);
	        logger.info("database_host 	: " + database_host);
	        logger.info("database_port 	: " + database_port);
	        logger.info("database_name		: " + database_name);
	        	        
	    } catch (IOException ex) {
	        ex.printStackTrace();
	    } finally {
	        if (input != null) {
	            try {
	                input.close();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	    }
		
		mongoconfig		= 	new JsonObject()
							.put("username", database_user)
							.put("password", database_password)
							.put("authSource", "test")
							.put("host", database_host)
							.put("port", database_port)
							.put("db_name", database_name);

		mongo = MongoClient.createShared(vertx, mongoconfig);

	}

	private void updateMetrics(Message<Object> message) {

		JsonObject request = new JsonObject(message.body().toString());

		if(request.getString("api").equalsIgnoreCase("metrics")) 
		{
			COLLECTION = "metrics";			
		} else if(request.containsKey("email"))
		{
			COLLECTION = "user_metrics";
		} 
		else
		{
			COLLECTION = "api_metrics";	
		}

		mongo.insert(COLLECTION, request, write_response -> {
			if (write_response.succeeded()) 
			{
				logger.info("Metrics Saved ! ");
				message.reply("SUCCESS");
			} 
			else 
			{
				message.fail(1, "database-insert error");
			}
		});

	}
	
	private void getMetrics(Message<Object> message) {

		JsonObject request = new JsonObject(message.body().toString());
		JsonObject query = decoderequest(request);
		
		if (request.containsKey("api")) {
			if (request.getString("api").equalsIgnoreCase("metrics")) 
			{
				COLLECTION = "metrics";
			} 
			else
			{
				COLLECTION = "api_metrics";	
			}
		} 
		
		mongoCount(COLLECTION, query, message);
	}
	
	private JsonObject decoderequest(JsonObject requested_data) {

		JsonObject query = new JsonObject();
		int state;

		if (! requested_data.containsKey("api") && requested_data.containsKey("options") && requested_data.getString("options").contains("total") 
				&& !requested_data.containsKey("resource-id") && !requested_data.containsKey("resource-group-id")) {
			state = 1;
			query = constructQuery(state, requested_data);
		}

		if (requested_data.containsKey("api") && requested_data.containsKey("options") && requested_data.getString("options").contains("total") 
				&& !requested_data.containsKey("resource-id") && !requested_data.containsKey("resource-group-id")) {
			state = 2;
			query = constructQuery(state, requested_data);
		}

		else if (requested_data.containsKey("api") && requested_data.containsKey("options") && requested_data.getString("options").contains("total")
				&& requested_data.containsKey("resource-id") && !requested_data.containsKey("resource-group-id")) {
			state = 3;
			query = constructQuery(state, requested_data);
		}

		else if (requested_data.containsKey("api") && requested_data.containsKey("options") && requested_data.getString("options").contains("total")
				&& !requested_data.containsKey("resource-id") && requested_data.containsKey("resource-group-id")) {
			state = 4;
			query = constructQuery(state, requested_data);
		}

		return query;

	}
	
	private JsonObject constructQuery(int state, JsonObject requested_data) {

		JsonObject query = new JsonObject();
		JsonArray expressions = new JsonArray();

		switch (state) {

		case 1:
			expressions.add(query);
			break;
		
		case 2:
			query.put("api", requested_data.getString("api"));
			expressions.add(query);
			break;

		case 3:
			query.put("api", requested_data.getString("api"));
			query.put("resource-id", requested_data.getString("resource-id"));
			expressions.add(query);
			break;

		case 4:
			query.put("api", requested_data.getString("api"));
			query.put("resource-group-id", requested_data.getString("resource-group-id"));
			expressions.add(query);
			break;
		}

		query = new JsonObject();
		query.put("$and", expressions);

		return query;
	}

	private void mongoCount(String COLLECTION, JsonObject query, Message<Object> message) {
		mongo.count(COLLECTION, query, database_response -> {	
			if (database_response.succeeded()) {

				JsonObject response = new JsonObject();
				long numItems = database_response.result();
				response.put("count", numItems);

				logger.info("Database Reply is : " + database_response.result().toString());
				logger.info("Response is : " + response.toString());

				message.reply(response);

			} else {
				message.fail(1, "item-not-found");
			}
		});
	}
}
