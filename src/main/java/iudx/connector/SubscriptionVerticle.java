package iudx.connector;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;

public class SubscriptionVerticle extends AbstractVerticle 
{
	private static final Logger logger = LoggerFactory.getLogger(SubscriptionVerticle.class);
	
	private MongoClient mongo;
	private JsonObject	mongoconfig;
	private String 		database_host;
	private int 		database_port;
	private String 		database_user;
	private String 		database_password;
	private String 		database_name;
	private String 		connectionStr;
	
	private JsonObject 	query;
	private JsonObject	isotime;
	
	@Override
	public void start(Future<Void> startFuture) 
	{
		logger.debug("Started subscription verticle");
		
		Properties prop = new Properties();
	    InputStream input = null;

	    try 
	    {
	        input = new FileInputStream("config.properties");
	        prop.load(input);
	        
	        database_user		=	prop.getProperty("database_user");
	        database_password	=	prop.getProperty("database_password");
	        database_host 		=	prop.getProperty("database_host");
	        database_port		=	Integer.parseInt(prop.getProperty("database_port"));
	        database_name		=	prop.getProperty("database_name");
	        

	        logger.debug("database_user 	: " + database_user);
	        logger.debug("database_password	: " + database_password);
	        logger.debug("database_host 	: " + database_host);
	        logger.debug("database_port 	: " + database_port);
	        logger.debug("database_name		: " + database_name);
	        
	        
	        input.close();
	        
	    } 
	    catch (Exception e) 
	    {
	        e.printStackTrace();
	    } 

		mongoconfig		= 	new JsonObject()
							.put("username", database_user)
							.put("password", database_password)
							.put("authSource", "test")
							.put("host", database_host)
							.put("port", database_port)
							.put("db_name", database_name);

		mongo = MongoClient.createShared(vertx, mongoconfig);
		vertx.eventBus().consumer("subscription", this::onMessage);
				
		startFuture.complete();
	}
	
	public void onMessage(Message<JsonObject> message)
	{
		String type	=	message.headers().get("type");
		
		switch(type)
		{
			case "stream":
				stream(message);
				break;
			case "callback":
				callback(message);
				break;
				
		}
	}
	
	private void stream(Message<JsonObject> message)
	{
		//Step 1: Check if user has already been registered
		//Step 2: If no, then register the user and store the apikey somewhere (But where?)
		//Step 3: If yes, then go on to other steps
				
		//*********Simple way to do it**************
		//Step 4: Bind all relevant exchanges to the user's queue
		//Step 5: Retrieve user's apikey from the database
		//Step 6: Construct an amqp URI and send it in the response
				
		//*********The way we probably want it
		//Step 4: Create a temporary queue for the user's subscription
		//Step 5: Bind all relevant exchanges to the temporary queue
		//Step 6: Periodically subscribe to this queue, modify the data according to user's needs (How?)
		//Step 7: Publish to user's exchange
		
		message.reply("ok");
	}
	
	private void callback(Message<JsonObject> message)
	{
		//Same steps as stream. The last step will publish to callback_url instead of to the broker
		
		message.reply("ok");
	}
	
	private String checkRegistration(String username)
	{
		String apikey	=	"";
		
		return apikey; 
	}

}
