package iudx.connector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import javax.net.ssl.SSLPeerUnverifiedException;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

public class APIServerVerticle extends AbstractVerticle {

	private static final Logger logger = Logger.getLogger(APIServerVerticle.class.getName());
	private HttpServer server;
	private ClientAuth clientAuth ;
	private final int port = 443;
	private final String basepath = "/resource-server/pscdcl/v1";
	private String event, api;
	private HashMap<String, String> upstream;
	int state;
	JsonObject metrics;
	ConcurrentHashMap<String,Integer> validity = new ConcurrentHashMap<String,Integer>();
	String ip;
	int count;

	private String keystore, keystorePassword, truststore, truststorePassword;
	
	private TimeZone tz;
	private DateFormat df; 
	private Calendar now;
	
	private boolean certificateStatus;
	private Principal cn;

	private String certificateClass[];
	private String classLevel;
	private String emailID;
	private String onboarder;
	private String designation;
	private String[] oidClass;
	private String level;
	
	private static int totalRequestsPerDay; 

	@Override
	public void start() {

		Set<String> allowedHeaders = new HashSet<>();
		allowedHeaders.add("Accept");
		allowedHeaders.add("token");
		allowedHeaders.add("Content-Length");
		allowedHeaders.add("Content-Type");
		allowedHeaders.add("Host");
		allowedHeaders.add("Origin");
		allowedHeaders.add("Referer");
	    allowedHeaders.add("Access-Control-Allow-Origin");
	    
	    Set<HttpMethod> allowedMethods = new HashSet<>();
	    allowedMethods.add(HttpMethod.GET);
	    allowedMethods.add(HttpMethod.POST);
	    allowedMethods.add(HttpMethod.OPTIONS);
	    allowedMethods.add(HttpMethod.DELETE);
	    allowedMethods.add(HttpMethod.PATCH);
	    allowedMethods.add(HttpMethod.PUT);

		Router router = Router.router(vertx);
		router.route().handler(CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods));
		router.route().handler(BodyHandler.create());

		router.post(basepath + "/search").handler(this::search);
		router.post(basepath + "/count").handler(this::count);
		router.post(basepath + "/subscriptions").handler(this::subscriptionsRouter);
		router.post(basepath + "/media").handler(this::search);
		router.post(basepath + "/download").handler(this::search);
		router.post(basepath + "/metrics").handler(this::metrics);

		router.get(basepath + "/subscriptions/:subId").handler(this::subscriptionStatus);

		router.delete(basepath + "/subscriptions/:subId").handler(this::deleteSubscription);

		router.put(basepath + "/subscriptions/:subId").handler(this::updateSubscription);

		Properties prop = new Properties();
	    InputStream input = null;
	    
	    try {

	        input = new FileInputStream("config.properties");
	        prop.load(input);

	        keystore 	=	prop.getProperty("keystore");
	        keystorePassword = prop.getProperty("keystorePassword");

			truststore = prop.getProperty("truststore");
			truststorePassword = prop.getProperty("truststorePassword");
 	        	        
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
		
		clientAuth = ClientAuth.REQUEST;

		server =
		        vertx.createHttpServer(
		            new HttpServerOptions()
		                .setSsl(true)
		                .setClientAuth(clientAuth)
		                .setTrustStoreOptions(new JksOptions().setPath(truststore).setPassword(truststorePassword))
		                .setKeyStoreOptions(
		                    new JksOptions().setPath(keystore).setPassword(keystorePassword)));

		server.requestHandler(router::accept).listen(port);

		logger.info("IUDX Connector started at Port : " + port + " !");

		tz = TimeZone.getTimeZone("UTC");
		df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); 
		df.setTimeZone(tz);
		
	}

	private void search(RoutingContext routingContext) {
			
		if(decodeCertificate(routingContext))
		{
			totalRequestsPerDay = 500;
		}
		else 
		{
			totalRequestsPerDay = 50; 
		}
		
		Future<Void> validity = validateRequest(routingContext, "search");
		
		validity.setHandler(validationResultHandler -> {
			
			HttpServerResponse response = routingContext.response();
			
			if(validationResultHandler.succeeded()) 
			{
				JsonObject requested_data = new JsonObject();
				DeliveryOptions options = new DeliveryOptions();
				requested_data = routingContext.getBodyAsJson();
				api = "search";
				event = "search";
				metrics = new JsonObject();
				
				now = Calendar.getInstance();
				String nowAsISO = df.format(now.getTime()); 
				
				metrics.put("time", new JsonObject().put("$date", nowAsISO));
				
				metrics.put("endpoint", api);
				
				metrics.put("ip", ip);
				
				if(certificateStatus) {
					metrics.put("emailID", emailID);
				}
				
				switch (decoderequest(requested_data)) {

				case 0:
					break;

				case 1:

					if (requested_data.getString("options").contains("latest")) {
						logger.info("case-1: latest data for an item in group");
						options.addHeader("state", Integer.toString(state));
						options.addHeader("options", "latest");
						publishEvent(event, requested_data, options, response);
					}

					else if (requested_data.getString("options").contains("status")) {
						logger.info("case-1: status for an item in group");
						options.addHeader("state", Integer.toString(state));
						options.addHeader("options", "status");
						publishEvent(event, requested_data, options, response);			
					}

					
					break;

				case 2:
					logger.info("case-2: latest data for all the items in group");
					options.addHeader("state", Integer.toString(state));
					publishEvent(event, requested_data, options, response);
					break;

				case 3:
					logger.info("case-3: time-series data for an item in group");
					options.addHeader("state", Integer.toString(state));
					publishEvent(event, requested_data, options, response);
					break;

				case 4:
					break;
					
				case 5:
					logger.info("case-5: geo search for an item group");
					options.addHeader("state", Integer.toString(state));
					publishEvent(event, requested_data, options, response);
					break;			
					
				case 7:
					logger.info("case-7: geo search(bbox) for an item group");
					options.addHeader("state", "7");
					publishEvent(event,requested_data, options, response);
					break;			

				case 8:
					logger.info("case-8: geo search(Polygon/LineString) for an item group");
					options.addHeader("state", "8");
					publishEvent(event,requested_data, options, response);
					break;			


				case 11:
					logger.info("case-11: attribute search for resource for an item group");
					options.addHeader("state","11");
					publishEvent(event,requested_data,options,response);
					break;

				}
			
			}
			
			else 
			{
				handle429(response);
			}
			
		});
	}

	private void count(RoutingContext routingContext) {

		if(decodeCertificate(routingContext))
		{
			totalRequestsPerDay = 500;
		}
		else 
		{
			totalRequestsPerDay = 50; 
		}
		
		Future<Void> validity = validateRequest(routingContext, "count");
		validity.setHandler(validationResultHandler -> {
			
		HttpServerResponse response = routingContext.response();
			
		if(validationResultHandler.succeeded()) 
		{

		JsonObject requested_data = new JsonObject();
		DeliveryOptions options = new DeliveryOptions();
		requested_data = routingContext.getBodyAsJson();
		api = "count";
		event = "search"; 
		
		metrics = new JsonObject();

		now = Calendar.getInstance();
		String nowAsISO = df.format(now.getTime()); 
		
		metrics.put("time", new JsonObject().put("$date", nowAsISO));
		metrics.put("endpoint", api);
		metrics.put("ip", ip);
		
		switch (decoderequest(requested_data)) {

		case 4:
			logger.info("case-4: count for time-series data for an item in group");
			options.addHeader("state", Integer.toString(state));
			options.addHeader("options", "count");
			publishEvent(event, requested_data, options, response);
			break;
			
		case 6:
			logger.info("case-6: count for geo search for an item group");
			options.addHeader("state", Integer.toString(state));
			options.addHeader("options", "count");
			publishEvent(event, requested_data, options, response);
			break;
		
        case 9:
			logger.info("case-9: count for geo search(bbox) for an item group");
			options.addHeader("state", "9");
			options.addHeader("options", "count");
			publishEvent(event,requested_data, options, response);
			break;
		
        case 10:
			logger.info("case-10: count for geo search(Polygon/LineString) for an item group");
			options.addHeader("state", "10");
			options.addHeader("options", "count");
			publishEvent(event,requested_data, options, response);
			break;

		case 12:
			logger.info("case-12: count for attribute search for an item group");
			options.addHeader("state","12");
			options.addHeader("options","count");
			publishEvent(event,requested_data,options, response);
			break;
			
		default:
			logger.info("case-unknown: invalid api request");
			handle400(response);
			}
			}
		
			else 
			{
				handle429(response);
			}
			
		});
	}
	
	private void metrics(RoutingContext routingContext) {

		if(decodeCertificate(routingContext))
		{
			totalRequestsPerDay = 500;
		}
		else 
		{
			totalRequestsPerDay = 50; 
		}
		
		Future<Void> validity = validateRequest(routingContext, "metrics");
		
		validity.setHandler(validationResultHandler -> {
			
		HttpServerResponse response = routingContext.response();
			
		if(validationResultHandler.succeeded()) 
		{

		final JsonObject requested_data = routingContext.getBodyAsJson();

		DeliveryOptions options = new DeliveryOptions();

		api = "metrics"; 

		event = "get-metrics"; 

		metrics = new JsonObject();
		
		now = Calendar.getInstance();
		String nowAsISO = df.format(now.getTime()); 
		
		metrics.put("time", new JsonObject().put("$date", nowAsISO));
		metrics.put("ip", ip);
		
		publishEvent(event, requested_data, options, response);
		
		}
		
		else 
		{
			handle429(response);
		}
		
	});
		
	}
	
	private int decoderequest(JsonObject requested_data) {

		String[] splitId = requested_data.getString("id").split("/");
		String resource_group = splitId[3];
		String resource_id = splitId[2] + "/" + splitId[3] + "/" + splitId[4];
		requested_data.put("resource-group-id", resource_group);
		requested_data.put("resource-id", resource_id);
		requested_data.remove("id");
		
		state = 0;
		if (api.equalsIgnoreCase("search") && requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("resource-id") && !requested_data.containsKey("time")  && !requested_data.containsKey("lat")
				&& !requested_data.containsKey("geometry") && !requested_data.containsKey("attribute-name")) {
			state = 1;

		}

		else if (api.equalsIgnoreCase("search") && requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& !requested_data.containsKey("resource-id") && !requested_data.containsKey("time")  && !requested_data.containsKey("lat")
				&& !requested_data.containsKey("geometry") && !requested_data.containsKey("attribute-name")) {
			state = 2;
		}

		else if (api.equalsIgnoreCase("search") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("resource-id") && requested_data.containsKey("time")
				&& requested_data.containsKey("TRelation")  && !requested_data.containsKey("lat")
				&& !requested_data.containsKey("geometry") && !requested_data.containsKey("attribute-name")) {
			state = 3;
		}

		else if (api.equalsIgnoreCase("count") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("resource-id") && requested_data.containsKey("time")
				&& requested_data.containsKey("TRelation")  && !requested_data.containsKey("lat")
				&& !requested_data.containsKey("geometry") && !requested_data.containsKey("attribute-name")) {
			state = 4;
		}

		else if (api.equalsIgnoreCase("search") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("lat") && requested_data.containsKey("lon")
				&& requested_data.containsKey("radius") && !requested_data.containsKey("time")
				&& !requested_data.containsKey("geometry")) {
			state = 5;
		}

		else if (api.equalsIgnoreCase("count") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("lat") && requested_data.containsKey("lon")
				&& requested_data.containsKey("radius") && !requested_data.containsKey("time")
				&& !requested_data.containsKey("geometry")) {
			state = 6;
		}
		
		else if (api.equalsIgnoreCase("search") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("bbox") && !requested_data.containsKey("time")  && !requested_data.containsKey("lat")
				&& !requested_data.containsKey("geometry")) {
			state = 7;
		}

		else if (api.equalsIgnoreCase("search") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("geometry") && !requested_data.containsKey("time")  && !requested_data.containsKey("lat")) {
			state = 8;
		}
		
        else if (api.equalsIgnoreCase("count") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("bbox") && !requested_data.containsKey("time")  && !requested_data.containsKey("lat")
				&& !requested_data.containsKey("geometry")) {
			state = 9;
		}

		else if (api.equalsIgnoreCase("count") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("geometry") && !requested_data.containsKey("time")  && !requested_data.containsKey("lat")) {
			state = 10;
		}

		else if (api.equalsIgnoreCase("search") && requested_data.containsKey("attribute-name") && requested_data.containsKey("attribute-value")
				&& requested_data.containsKey("resource-group-id") && (requested_data.containsKey("comparison-operator") || requested_data.containsKey("logical-operator"))
				&& !requested_data.containsKey("time")  && !requested_data.containsKey("lat") && !requested_data.containsKey("geometry")){
			state=11;
		}

		else if (api.equalsIgnoreCase("count") && requested_data.containsKey("attribute-name") && requested_data.containsKey("attribute-value")
				&& (requested_data.containsKey("comparison-operator") || requested_data.containsKey("logical-operator"))
				&& requested_data.containsKey("resource-group-id") && !requested_data.containsKey("time")  && !requested_data.containsKey("lat")
				&& !requested_data.containsKey("geometry")){
			state=12;
		}
		System.out.println("STATE: "+ state);
		return state;
	}
	
	private void publishEvent(String event, JsonObject requested_data, DeliveryOptions options, HttpServerResponse response) {
		vertx.eventBus().send(event, requested_data, options, replyHandler -> {
			if (replyHandler.succeeded()) 
			{
				handle200(response, replyHandler, requested_data);

			} else 
			{
				response.setStatusCode(400).end();
			}
		});
	}

	private void handle200(HttpServerResponse response, AsyncResult<Message<Object>> replyHandler, JsonObject requested_data) {
		response.setStatusCode(200).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
				.end(replyHandler.result().body().toString());
		updatemetrics(requested_data, metrics);
		updatevalidity(metrics);
	}

	private void handle429(HttpServerResponse response) {
		response.setStatusCode(429).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").end();
	}

	private void handle400(HttpServerResponse response) {
		response.setStatusCode(400).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json").end();
		updatevalidity(metrics);
	}
	
	private void updatemetrics(JsonObject requested_data, JsonObject metrics) {

		metrics.put("api", api);
		metrics.put("endpoint", api);
		metrics.put("resource-group-id", requested_data.getString("resource-group-id"));

		if(certificateStatus) {
			metrics.put("emailID", emailID);
		}
		
		if (state != 5 || state != 6) 
		{
			metrics.put("resource-id", requested_data.getString("resource-id"));
		}

		if (state == 1) 
		{

			metrics.put("options", requested_data.getString("options"));
		}

		else if (state == 3 || state == 4) 
		{

			metrics.put("TRelation", requested_data.getString("TRelation"));
		}

		else if (state == 5 || state == 6) 
		{

			metrics.put("GRelation", "circle");

		}

		logger.info("Metrics is : " + metrics);
		
		vertx.eventBus().send("update-metrics", metrics, replyHandler -> {
			
			if (replyHandler.succeeded()) 
			{
				logger.info("SUCCESS");
			} 
			
			else 
			{
				logger.info("FAILED");
			}
		});
	}
	
	//TODO: Handle validation, bad requests and sanitise inputs
	private void subscriptionsRouter(RoutingContext context)
	{
		HttpServerResponse  response	=   context.response();
		HttpServerRequest   request	=   context.request();
		DeliveryOptions	    options	=   new DeliveryOptions();
		JsonObject 	    message	=   new JsonObject();
		JsonObject	    body;
		
		try
		{
		    body = context.getBodyAsJson();
		}
		catch(Exception e)
		{
		    response.setStatusCode(400).end("Body is not a valid JSON");
		    return;
		}
		
		//Replace this with TIP
		String username	=   request.headers().get("username");
		
		//Input validation
		if  (	(username	==	null)
					||
			(!body.containsKey("resourceIds"))
					||
			(!body.containsKey("type"))
		    )
		{
		    response.setStatusCode(400).end("Missing fields in header or body");
		    return;
		}
		
		JsonArray resourceIds	    =	body.getJsonArray("resourceIds");
		
		logger.info(resourceIds.toString());
		
		if(body.getString("type").equals("callback"))
		{
			if(!body.containsKey("callbackUrl"))
			{
			    response.setStatusCode(400).end("Missing callbackUrl");
			    return;
			}
			
			String callbackUrl  =	body.getString("callbackUrl");
			
			options.addHeader("type", "callback");
			message.put("username",username);
			message.put("resourceIds",resourceIds);
			message.put("callbackUrl", callbackUrl);
			
			vertx.eventBus().send("subscription", message, options, reply -> {
				
			    if(!reply.succeeded())
			    {
				response.setStatusCode(500).end("Internal server error");
				return;
			    }
				
			    response.setStatusCode(202).end();
				
			});
		}
		
		else if(body.getString("type").equals("stream"))
		{
			options.addHeader("type", "stream");
			message.put("username", username);
			message.put("resourceIds", resourceIds);
			
			vertx.eventBus().send("subscription", message, options, reply -> {
				
			    if	(	(!reply.succeeded())
						||
				    (reply.result().body()  ==  null)
				)
			    {
				response.setStatusCode(500).end("Internal server error");
				return;
			    }
			    else
			    {
				String  replyString[]	=   reply.result().body().toString().split(",");
			    	String  amqpUrl		=   replyString[0];
			    	String  subscriptionId  =   replyString[1];	

				JsonObject responseJson	=   new JsonObject();

				responseJson.put("amqpUrl", amqpUrl);
				responseJson.put("subscriptionId", subscriptionId);
			    	    
			    	response.putHeader("content-type", "application/json")
					.setStatusCode(201)
					.end(responseJson.encodePrettily());
				return;
			    }
			});
		}
	}

	private void subscriptionStatus(RoutingContext context)
	{
	    HttpServerResponse  response	=   context.response();
	    HttpServerRequest	request		=   context.request();
	    String		subscriptionId	=   context.request().getParam("subId");
	    
	    //Replace this with TIP results
	    String		username	=   request.headers().get("username");
	    DeliveryOptions	options		=   new DeliveryOptions();
	    JsonObject		message		=   new JsonObject();

	    options.addHeader("type", "status");
	    message.put("username", username);
	    message.put("subscriptionId", subscriptionId);

	    vertx.eventBus().send("subscription", message, options, reply -> {
		
		if  (	    (!reply.succeeded())
				    ||
			(reply.result().body()  ==  null)
		    )
		{
		    response.setStatusCode(500).end("Internal server error");
		    return;
		}
		else
		{
		    JsonObject replyJson    =	(JsonObject)reply.result().body();
		    
		    response.putHeader("content-type", "application/json")
			    .setStatusCode(200)
			    .end(replyJson.encodePrettily());
		    return;
		}
	    });
	}

	private void deleteSubscription(RoutingContext context)
	{
	    HttpServerResponse  response	=   context.response();
	    HttpServerRequest	request		=   context.request();
	    String		subscriptionId	=   context.request().getParam("subId");
	    
	    //Replace this with TIP results
	    String		username	=   request.headers().get("username");
	    DeliveryOptions	options		=   new DeliveryOptions();
	    JsonObject		message		=   new JsonObject();

	    options.addHeader("type", "delete");
	    message.put("username", username);
	    message.put("subscriptionId", subscriptionId);

	    vertx.eventBus().send("subscription", message, options, reply -> {
		
		if  (	    (!reply.succeeded())
				    ||
			(reply.result().body()  ==  null)
		    )
		{
		    response.setStatusCode(500).end("Internal server error");
		    return;
		}
		else
		{
		    response.setStatusCode(204).end();
		    return;
		}
	    });
	}

	private void updateSubscription(RoutingContext context)
	{
	    HttpServerResponse  response	=   context.response();
	    HttpServerRequest   request		=   context.request();
	    DeliveryOptions	options		=   new DeliveryOptions();
	    JsonObject		message		=   new JsonObject();
	    JsonObject		body;
	    String		subscriptionId	=   context.request().getParam("subId");
		
	    try
	    {
	        body = context.getBodyAsJson();
	    }
	    catch(Exception e)
	    {
	        response.setStatusCode(400).end("Body is not a valid JSON");
	        return;
	    }
		
	    //Replace this with TIP
	    String username	=   request.headers().get("username");
		
	    //Input validation
	    if  (   (username	==	null)
		    		||
		    (!body.containsKey("resourceIds"))
		)
	    {
		response.setStatusCode(400).end("Missing fields in header or body");
		return;
	    }
		
	    JsonArray resourceIds	    =	body.getJsonArray("resourceIds");
		
	    logger.info(resourceIds.toString());

	    options.addHeader("type", "update");
	    message.put("username", username);
	    message.put("subscriptionId", subscriptionId);
	    message.put("resourceIds", resourceIds);

	    vertx.eventBus().send("subscription", message, options, reply -> {
		
		if  (	    (!reply.succeeded())
				    ||
			(reply.result().body()  ==  null)
		    )
		{
		    response.setStatusCode(500).end("Internal server error");
		    return;
		}
		else
		{
		    response.setStatusCode(200).end();
		    return;
		}
	    });
	}

	private Future<Void> validateRequest(RoutingContext routingContext, String api) {
		// TODO Auto-generated method stub

		Future<Void> validation = Future.future();
		ip = routingContext.request().remoteAddress().host();

		if (certificateStatus) {
			if (validity.containsKey(emailID)) {
				count = validity.get(emailID);
			} else {
				count = 0;
			}
		} else {
			if (validity.containsKey(ip)) {
				count = validity.get(ip);
			} else {
				count = 0;
			}
		}

		if (count <= totalRequestsPerDay) {
			validation.complete();
			count = count + 1;
			
			if(certificateStatus) {
				validity.put(emailID, count);				
			} else {
				validity.put(ip, count);				
			}

			logger.info("User from IP " + ip + " Accessed APIs for " + count + " times.");
			logger.info("Allowed requests per day is : " +totalRequestsPerDay);
		} else {
			validation.fail("failed");
			logger.info("User from IP " + ip + " Accessed APIs for " + count + " times.");
			logger.info("Allowed requests per day is : " +totalRequestsPerDay);
		}

		return validation;

	}

	private void updatevalidity(JsonObject metrics) {
		logger.info("Metrics is : " + metrics);

		vertx.eventBus().send("update-limit-on-ip", metrics, replyHandler -> {

			if (replyHandler.succeeded()) {
				logger.info("update-limit-on-ip : SUCCESS");

				vertx.eventBus().send("get-limit-on-ip", metrics, limit_count_replyHandler -> {

					if (limit_count_replyHandler.succeeded()) {

						JsonObject response = (JsonObject) limit_count_replyHandler.result().body();
						count = response.getInteger("count");
						
						if(certificateStatus) {
							validity.put(emailID, count);
						} else {
							validity.put(ip, count);
						}
					}
				});
			}

			else {
				logger.info("update-limit-on-ip : FAILED");
			}
		});

	}
	
	private boolean decodeCertificate(RoutingContext routingContext) {

		certificateStatus = false;
		
		try {
			cn = routingContext.request().connection().sslSession().getPeerPrincipal();

			certificateClass = cn.toString().split(",");
			classLevel = certificateClass[0];
			emailID = certificateClass[8].split("=")[1];
			onboarder = certificateClass[1];
			designation = onboarder.split("=")[1];
			oidClass = classLevel.split("=");
			level = oidClass[1].split(":")[1];

			certificateStatus = true;

			logger.info("e-mail : " + emailID + " : designation : " + designation + " : class level : " + level  );
			
		} catch (SSLPeerUnverifiedException e) {
			certificateStatus = false;
		}

		return certificateStatus;
	}

}
