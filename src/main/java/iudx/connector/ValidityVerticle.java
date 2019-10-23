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

public class ValidityVerticle extends AbstractVerticle {

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
	private String 		auth_database;

	@Override
	public void start() throws Exception {

		logger.info("Validity Verticle started!");

		vertx.eventBus().consumer("update-limit-on-ip", message -> {
			logger.info("Got the event : " + message.body().toString());
			ipRateLimit(message);
		});

		vertx.eventBus().consumer("get-limit-on-ip", message -> {
			logger.info("Got the event : " + message.body().toString());
			getipRateLimit(message);
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
	        auth_database		=	prop.getProperty("auth_database");
	        

	        logger.info("database_user 	: " + database_user);
	        logger.info("database_password	: " + database_password);
	        logger.info("database_host 	: " + database_host);
	        logger.info("database_port 	: " + database_port);
	        logger.info("database_name		: " + database_name);
	        logger.debug("auth_database		: " + auth_database);
	        	        
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
							.put("authSource", auth_database)
							.put("host", database_host)
							.put("port", database_port)
							.put("db_name", database_name);

		mongo = MongoClient.createShared(vertx, mongoconfig);

	}

	private void getipRateLimit(Message<Object> message) {
		JsonObject request = new JsonObject(message.body().toString());
		JsonObject query = decoderequest(request);
		
		if (request.containsKey("ip")) {
			COLLECTION = "ip_based_rate_limit";
		} 
		
		mongoCount(COLLECTION, query, message);

	}

	private void ipRateLimit(Message<Object> message) {

		JsonObject request = new JsonObject(message.body().toString());

		if (request.containsKey("ip")) {
			COLLECTION = "ip_based_rate_limit";
		} 
		
		mongo.insert(COLLECTION, request, write_response -> {
			if (write_response.succeeded()) 
			{
				logger.info("Validity Metrics Saved ! ");
				message.reply("SUCCESS");
			} 
			else 
			{
				message.fail(1, "database-insert error");
			}
		});

		
		
	}

	private JsonObject decoderequest(JsonObject requested_data) {

		JsonObject query = new JsonObject();
		int state;

		if (requested_data.containsKey("ip")) {
			state = 1;
			query = constructQuery(state, requested_data);
		}

		return query;

	}
	
	private JsonObject constructQuery(int state, JsonObject requested_data) {

		JsonObject query = new JsonObject();
		JsonArray expressions = new JsonArray();

		switch (state) {

		case 1:
			query.put("ip", requested_data.getString("ip"));
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
