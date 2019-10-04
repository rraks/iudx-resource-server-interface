package iudx.connector;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

//Why is is this needed again?
import java.nio.charset.StandardCharsets;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.HttpURLConnection;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.core.Future;
import io.vertx.core.Promise;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

import com.google.common.hash.Hashing;

import org.apache.commons.lang3.RandomStringUtils;

public class SubscriptionVerticle extends AbstractVerticle 
{
	//TODO: Lines are too long!
	private static final Logger logger	    =	LoggerFactory.getLogger(SubscriptionVerticle.class);
	private static final String PASSWORDCHARS   =	"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-";
	
	private MongoClient		mongo;
	private JsonObject		mongoconfig;
	private String 			database_host;
	private int 			database_port;
	private String 			database_user;
	private String 			database_password;
	private String 			database_name;
	private String 			connectionStr;
	private JsonObject 		query;
	private JsonObject		isotime;
	private String			resource_server_url;
	private String			resource_server_admin_key;
	public	Map<String, Channel>	pool;
	public	Connection 		connection;
	public	Channel 		channel;
	public	ConnectionFactory	factory;


	
	@Override
	public void start(Future<Void> startFuture) 
	{
	    logger.debug("Started subscription verticle");
		
	    Properties prop	=   new Properties();
	    InputStream input	=   null;
	    pool		=   new HashMap<String, Channel>();

	    try 
	    {
	        input = new FileInputStream("config.properties");
	        prop.load(input);
	        
	        database_user		    =	prop.getProperty("database_user");
	        database_password  	    =	prop.getProperty("database_password");
	        database_host 	   	    =	prop.getProperty("database_host");
	        database_port	   	    =	Integer.parseInt(prop.getProperty("database_port"));
	        database_name	   	    =	prop.getProperty("database_name");
		resource_server_url	    =	prop.getProperty("resource_server_url");
		resource_server_admin_key   =	prop.getProperty("resource_server_admin_key");	
	        

	        logger.debug("database_user 	: " + database_user);
	        logger.debug("database_password	: " + database_password);
	        logger.debug("database_host 	: " + database_host);
	        logger.debug("database_port 	: " + database_port);
	        logger.debug("database_name	: " + database_name);
	        
	        input.close();
	        
	    } 
	    catch (Exception e) 
	    {
	        e.printStackTrace();
	    } 

		mongoconfig =	new JsonObject()
				.put("username", database_user)
				.put("password", database_password)
				.put("authSource", "admin")
				.put("host", database_host)
				.put("port", database_port)
				.put("db_name", database_name);

		mongo	    =   MongoClient.createShared(vertx, mongoconfig);

		vertx.eventBus().consumer("subscription", this::onMessage);
				
		startFuture.complete();
	}
	
	public void onMessage(Message<JsonObject> message)
	{
		String type =   message.headers().get("type");
		
		switch(type)
		{
		    case "stream":
			stream(message);
			break;
		    case "callback":
			callback(message);
			break;
		    case "status":
			status(message);
			break;
		    case "delete":
			deleteStream(message);
			break;
		}
	}
	
	//TODO: Use variable "resourceIds" consistently everywhere
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

	    JsonObject  messageBody	=   message.body();
	    String	username	=   messageBody.getString("username");
	    JsonArray   resourceIds	=   messageBody.getJsonArray("resourceIds");
		
	    checkRegistration(username)
	    .setHandler(register -> {
	    
		if(!register.succeeded())
		{
		    message.reply(null);
		}

		String subscriptionId	=   RandomStringUtils
					    .random(8, 0, PASSWORDCHARS.length(),
					    true, true, PASSWORDCHARS.toCharArray());

		JsonObject document	=   new JsonObject();

		document.put("username", username);
		document.put("resourceIds", resourceIds);
		document.put("type", "stream");
		document.put("subscriptionId", subscriptionId);
		document.put("status", "processing");
		    
		mongo.insert("subscriptions", document, res -> {
		
		    if(!res.succeeded())
		    {
			message.reply(null);
		    }

		    String  result[]	=   register.result().split(",");
		    String  entityId	=   result[0].replace("/", "%2f");
		    String  apikey	=   result[1];

		    //Strip https from url
		    String  url		=   resource_server_url
					    .substring(8, resource_server_url.length());

		    String  amqpUrl	=   "amqp://"		+   
					    entityId		+   
					    ":"			+	
					    apikey		+   
					    "@"			+   
					    url			+   
					    ":5672"		+
					    "/%2f";


		    message.reply(amqpUrl+","+subscriptionId);

		    asyncOp(username, resourceIds, "bind")
		    .setHandler(bind -> {
		    
			if(!bind.succeeded())
			{
			    JsonObject update =	new JsonObject();

			    logger.info(bind.cause());
			    update.put("$set",new JsonObject().put("status", "errored"));

			    //TODO: What if this fails?
			    mongo.updateCollection("subscriptions", document, update, resp -> {});
			}
			else
			{
			    JsonObject update =	new JsonObject();

			    update.put("$set",new JsonObject().put("status", "succeeded"));

			    //TODO: What if this fails?
			    mongo.updateCollection("subscriptions", document, update, resp -> {});
			}
		    });
		});
	    });
	}
	
	private void deleteStream(Message<JsonObject> message)
	{
	    JsonObject  messageBody	=   message.body();
	    String	username	=   messageBody.getString("username");
	    String	subscriptionId	=   messageBody.getString("subscriptionId");
		
	    checkRegistration(username)
	    .setHandler(register -> {
	    
		if(!register.succeeded())
		{
		    message.reply(null);
		}

		JsonObject document	=   new JsonObject();

		document.put("username", username);
		document.put("subscriptionId", subscriptionId);
		    
		mongo.find("subscriptions", document, res -> {
		
		    if(!res.succeeded())
		    {
			message.reply(null);
		    }

		    JsonObject	queryResult =	res.result().get(0);

		    if(queryResult.size() == 0)
		    {
			message.reply(null);
			return;
		    }

		    JsonArray	resourceIds =	queryResult.getJsonArray("resourceIds");

		    message.reply("ok");

		    asyncOp(username, resourceIds, "unbind")
		    .setHandler(op -> {
		    
			//Need to handle this properly
			if(!op.succeeded())
			{
			    JsonObject update   =	new JsonObject();

			    logger.info(op.cause());
			    update.put("$set",new JsonObject().put("status", "errored-while-deleting"));

			    //TODO: What if this fails?
			    mongo.updateCollection("subscriptions", document, update, resp -> {});
			}
			else
			{
			    //TODO: What if this fails?
			    mongo.removeDocument("subscriptions", document, delete -> {});
			}
		    });
		});
	    });
	}

	private void callback(Message<JsonObject> message)
	{
		//Same steps as stream. The last step will publish to callback_url instead of to the broker
		
		message.reply("ok");
	}
	
	private Future<String> checkRegistration(String username)
	{
		String		    apikey;
		URL		    url;
		HttpURLConnection   con;

		String	    registerUrl	=   resource_server_url	+   "/owner/register-entity";
		String	    userHash	=   Hashing.sha1()
					    .hashString(username, StandardCharsets.UTF_8)
					    .toString();
		JsonObject  body	=   new JsonObject();
		Promise	    promise	=   Promise.promise();   

		body.put("dummy", "schema");
			
		try 
		{
		    url = new URL(registerUrl);
		    con = (HttpURLConnection) url.openConnection();
		    con.setRequestMethod("POST");
		    con.setRequestProperty("id", "admin");
		    con.setRequestProperty("apikey", resource_server_admin_key);
		    con.setRequestProperty("entity", userHash); 
		    con.setRequestProperty("is-autonomous", "true");
		    con.setRequestProperty("Content-Type", "application/json; utf-8");
		    con.setDoOutput(true);
				
		    con.getOutputStream().write(body.encode().getBytes("UTF-8"));
				
		    int code=con.getResponseCode();
				
		    logger.info("response code="+code);
				
	            if(code ==	201)
	            {
			BufferedReader br   =	new BufferedReader(new InputStreamReader(con.getInputStream()));
			String line	    =	br.lines().collect(Collectors.joining());
			
			br.close();

			JsonObject response =	new JsonObject(line);
			JsonObject document =	new JsonObject();
			apikey		    =	response.getString("apikey");

			document.put("username", username);
			document.put("apikey", apikey);

			mongo.insert("apikeys", document, res -> {
			
			    if(!res.succeeded())
			    {
				logger.error(res.cause());
				promise.fail(res.cause());
			    }

			    promise.complete(userHash+","+apikey);
			});
	            }
	            else
		    {
			//Search mongo for apikey and return it

			JsonObject query = new JsonObject();

			query.put("username", username);

			//What about using findOne?
			mongo.find("apikeys", query, res -> {
			
			    if (!res.succeeded()) 
			    {
				logger.error(res.cause());
				promise.fail(res.cause());
			    }

			    JsonObject json =	res.result().get(0); 

			    promise.complete(userHash+","+json.getString("apikey"));
			});
		    }
		}
		catch(Exception e)
		{
		    e.printStackTrace();
		}

		return promise.future();
	}

	//TODO: Since this code is blocking, it is safer to use vertx.executeBlocking
	public Channel getAdminChannel()
	{
		String	token	=   "admin"; 

		//Strip https from url
		String	url	=   resource_server_url
				    .substring(8, resource_server_url.length());
		
		if  (	(!pool.containsKey(token))
				  ||
			(!pool.get(token).isOpen())
		    )
		{
			factory = new ConnectionFactory();
			factory.setUsername("admin");
			factory.setPassword(resource_server_admin_key);
			factory.setVirtualHost("/");
			factory.setHost(url);
			factory.setPort(5672);

			try 
			{
				connection = factory.newConnection();
				channel = connection.createChannel();
				
				logger.debug("Rabbitmq channel created");
				
				pool.put(token, channel);
				
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
		return pool.get(token);
	}

	//TODO: Use executeBlocking here as well
	//TODO: Add support for message type 
	//TODO: Add support for routing key
	public Future<Void> asyncOp(String username, JsonArray resourceIds, String type)
	{
	    Promise promise =	Promise.promise();

	    List<String>    resourceIdList  =	resourceIds.getList();
	    String	    userHash	    =   Hashing.sha1()
						.hashString(username, StandardCharsets.UTF_8)
						.toString();

	    try
	    {
		for(String resource: resourceIdList)
	    	{
	    	    String  idComponents[]	=   resource.split("/");
	    	    int	    len			=   idComponents.length;
	    	    String  owner;
	    	    String  resourceName;

	    	    if(idComponents[len-2].equalsIgnoreCase("changebhai"))
	    	    {
	    	        owner		=	"changebhai";
	    	        resourceName	=	"crowd-sourced-images";
	    	    }
	    	    else
	    	    {
	    	        owner		=	"pscdcl";
	    	        resourceName    =	idComponents[len-2] +	"/" +	idComponents[len-1];
	    	    }

		    logger.info("Owner="+owner);
		    logger.info("resourceName="+resourceName);

	    	    String resourceNameHash =	Hashing.sha1()
						.hashString(resourceName, 
							    StandardCharsets.UTF_8
							    )
						.toString();

	    	    String requestedResourceName    =	owner		    +   
							"/"		    +   
							resourceNameHash    +
							".public";

		    if(type.equalsIgnoreCase("bind"))
		    {
			getAdminChannel().queueBind("admin/"+userHash, requestedResourceName, "#");
		    }
		    else if(type.equalsIgnoreCase("unbind"))
		    {
			getAdminChannel().queueUnbind("admin/"+userHash, requestedResourceName, "#");
		    }
	    	}

		promise.complete();
	    }
	    catch(Exception e)
	    {
		e.printStackTrace();
		promise.fail(e.getMessage());
	    }
	    return promise.future();
	}
    
    private void status(Message<JsonObject> message)
    {
	JsonObject	messageBody	=   message.body();
	String		username	=   messageBody.getString("username");
	String		subscriptionId	=   messageBody.getString("subscriptionId");

	JsonObject query = new JsonObject();

	query.put("username", username);
	query.put("subscriptionId", subscriptionId);

	//What about using findOne?
	mongo.find("subscriptions", query, res -> {
			
	    if (!res.succeeded()) 
	    {
		logger.error(res.cause());
		message.reply(null);
	    }

	    JsonObject	json	=   res.result().get(0); 
	    json.remove("_id");
	    message.reply(json);
	});
    }
}
