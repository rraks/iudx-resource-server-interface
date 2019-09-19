package iudx.connector;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.util.ISO8601DateFormat;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class APIServerVerticle extends AbstractVerticle {

	private static final Logger logger = Logger.getLogger(APIServerVerticle.class.getName());
	private HttpServer server;
	private final int port = 18443;
	private final String basepath = "/resource-server/pscdcl/v1";
	private String api, email, username, domain;
	private HashMap<String, String> upstream;
	int state;
	JsonObject metrics;

	@Override
	public void start() {

		Router router = Router.router(vertx);
		router.route().handler(BodyHandler.create());

		router.post(basepath + "/search").handler(this::search);
		router.post(basepath + "/count").handler(this::count);
		router.post(basepath + "/subscriptions").handler(this::subscriptionsRouter);
		router.post(basepath + "/media").handler(this::search);
		router.post(basepath + "/download").handler(this::search);
		router.post(basepath + "/metrics").handler(this::search);

		server = vertx.createHttpServer(new HttpServerOptions().setSsl(true)
				.setKeyStoreOptions(new JksOptions().setPath("my-keystore.jks").setPassword("password")));

		server.requestHandler(router::accept).listen(port);

		logger.info("IUDX Connector started at Port : " + port + " !");

	}

	private void search(RoutingContext routingContext) {

		HttpServerResponse response = routingContext.response();

		JsonObject requested_data = new JsonObject();
		DeliveryOptions options = new DeliveryOptions();
		requested_data = routingContext.getBodyAsJson();
		api = "search";

		metrics = new JsonObject();
		metrics.put("time", ZonedDateTime.now( ZoneOffset.UTC ).format( DateTimeFormatter.ISO_INSTANT ));
		
		switch (decoderequest(requested_data)) {

		case 0:
			break;

		case 1:

			if (requested_data.getString("options").contains("latest")) {
				logger.info("case-1: latest data for an item in group");
				options.addHeader("state", Integer.toString(state));
				options.addHeader("options", "latest");
				publishEvent(requested_data, options, response, metrics);
			}

			else if (requested_data.getString("options").contains("status")) {
				logger.info("case-1: status for an item in group");
				options.addHeader("state", Integer.toString(state));
				options.addHeader("options", "status");
				publishEvent(requested_data, options, response, metrics);			
			}

			
			break;

		case 2:
			logger.info("case-2: latest data for all the items in group");
			options.addHeader("state", Integer.toString(state));
			publishEvent(requested_data, options, response, metrics);
			break;

		case 3:
			logger.info("case-3: time-series data for an item in group");
			options.addHeader("state", Integer.toString(state));
			publishEvent(requested_data, options, response, metrics);
			break;

		case 4:
			break;
			
		case 5:
			logger.info("case-5: geo search for an item group");
			options.addHeader("state", Integer.toString(state));
			publishEvent(requested_data, options, response, metrics);
			break;			
			
		}
	}

	private void count(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();

		JsonObject requested_data = new JsonObject();
		DeliveryOptions options = new DeliveryOptions();
		requested_data = routingContext.getBodyAsJson();
		api = "count";
		
		metrics = new JsonObject();
		metrics.put("time", ZonedDateTime.now( ZoneOffset.UTC ).format( DateTimeFormatter.ISO_INSTANT));
		
		switch (decoderequest(requested_data)) {

		case 4:
			logger.info("case-4: count for time-series data for an item in group");
			options.addHeader("state", Integer.toString(state));
			options.addHeader("options", "count");
			publishEvent(requested_data, options, response, metrics);
			break;
			
		case 6:
			logger.info("case-6: count for geo search for an item group");
			options.addHeader("state", Integer.toString(state));
			options.addHeader("options", "count");
			publishEvent(requested_data, options, response, metrics);
			break;
		}
	}
	
	private int decoderequest(JsonObject requested_data) {

		state = 0;

		if (api.equalsIgnoreCase("search") && requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("resource-id")) {
			state = 1; 
		}

		else if (api.equalsIgnoreCase("search") && requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& !requested_data.containsKey("resource-id")) {
			state = 2;
		}

		else if (api.equalsIgnoreCase("search") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("resource-id") && requested_data.containsKey("time")
				&& requested_data.containsKey("TRelation")) {
			state = 3;
		}

		else if (api.equalsIgnoreCase("count") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("resource-id") && requested_data.containsKey("time")
				&& requested_data.containsKey("TRelation")) {
			state = 4;
		}

		else if (api.equalsIgnoreCase("search") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("lat") && requested_data.containsKey("lon")
				&& requested_data.containsKey("radius")) {
			state = 5;
		}

		else if (api.equalsIgnoreCase("count") && !requested_data.containsKey("options") && requested_data.containsKey("resource-group-id")
				&& requested_data.containsKey("lat") && requested_data.containsKey("lon")
				&& requested_data.containsKey("radius")) {
			state = 6;
		}
		
		
		return state;
	}
	
	private void publishEvent(JsonObject requested_data, DeliveryOptions options, HttpServerResponse response, JsonObject metrics ) {
		vertx.eventBus().send("search", requested_data, options, replyHandler -> {
			if (replyHandler.succeeded()) {
				handle200(response, replyHandler);
				updatemetrics(requested_data, metrics);
			} else {
				response.setStatusCode(400).end();
			}
		});
	}

	private void handle200(HttpServerResponse response, AsyncResult<Message<Object>> replyHandler) {
		response.setStatusCode(200).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
				.end(replyHandler.result().body().toString());
	}

	private void updatemetrics(JsonObject requested_data, JsonObject metrics) {
		metrics.put("api", api);
		metrics.put("state", state);
		metrics.put("resource-group-id", requested_data.getString("resource-group-id"));

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

		System.out.println(metrics);
		
		vertx.eventBus().send("metrics", metrics, replyHandler -> {
			
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
	
	private void subscriptionsRouter(RoutingContext context)
	{
		HttpServerResponse	response	=	context.response();
		HttpServerRequest	request		=	context.request();
		
		JsonObject body;
		
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
		String username	=	request.headers().get("username");
		
		//Input validation
		if	(	(username	==	null)
							||
				(!body.containsKey("resource-ids"))
							||
				(!body.containsKey("type"))
			)
		{
			response.setStatusCode(400).end("Missing fields in header or body");
			return;
		}
		
		String idList				=	body.getString("resource-ids");
		idList						=	idList.substring(1,idList.length()-1);
		List<String> resourceIds	=	Arrays.asList(idList.split(","));
		
		if(body.getString("type").equals("callback"))
		{
			if(!body.containsKey("callback_url"))
			{
				response.setStatusCode(400).end("Missing callback_url");
				return;
			}
			
			String callbackUrl			=	body.getString("callback_url");
			callback(username, resourceIds, callbackUrl);
		}
		
		else if(body.getString("type").equals("stream"))
		{
			stream(username, resourceIds);
		}
	}
	
	private void stream(String username, List<String> resourceIds)
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
	}
	
	private void callback(String username, List<String> resourceIds, String callbackUrl)
	{
		//Same steps as stream. The last step will publish to callback_url instead of to the broker
	}
		
}
