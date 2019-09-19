package iudx.connector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

public class MetricsVerticle extends AbstractVerticle {
	
	private static final Logger logger = Logger.getLogger(MetricsVerticle.class.getName());
	private MongoClient mongo;
	private JsonObject mongoconfig;
	private String database_uri;
	private int database_port;
	private String database_name;
	private String COLLECTION;

	
	@Override
	public void start() throws Exception {

		logger.info("Metrics Verticle started!");

		vertx.eventBus().consumer("metrics", message -> {
			logger.info("Got the event : " + message.body().toString());
			updatemetrics(message);
		});

		Properties prop = new Properties();
	    InputStream input = null;

	    try {

	        input = new FileInputStream("config.properties");
	        prop.load(input);

	        database_uri = prop.getProperty("database_uri");
	        database_port  = Integer.parseInt(prop.getProperty("database_port"));
	        database_name = prop.getProperty("database_name");

	        logger.info("database_uri : " + database_uri);
	        logger.info("database_port : " + database_port);
	        logger.info("database_name : " + database_name);
	        
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
		
		database_uri = "mongodb://" + database_uri + ":"	+ database_port;

		mongoconfig = new JsonObject().put("connection_string", database_uri).put("db_name", database_name);

		mongo = MongoClient.createShared(vertx, mongoconfig);

	}

	private void updatemetrics(Message<Object> message) {

		JsonObject details = new JsonObject(message.body().toString());

		if(details.containsKey("email"))
		{
			COLLECTION = "user_metrics";
		} 
		else
		{
			COLLECTION = "api_metrics";	
		}

		mongo.insert(COLLECTION, details, write_response -> {
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
}

