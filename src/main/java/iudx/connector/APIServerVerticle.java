package iudx.connector;

import java.util.HashMap;
import java.util.logging.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
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
	private String api;
	private HashMap<String, String> upstream;

	@Override
	public void start() {

		Router router = Router.router(vertx);
		router.route().handler(BodyHandler.create());

		router.post(basepath + "/search").handler(this::search);
		router.post(basepath + "/count").handler(this::count);
		router.post(basepath + "/subscription").handler(this::search);
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

		switch (decoderequest(requested_data)) {

		case 0:
			break;

		case 1:

			if (requested_data.getString("options").contains("latest")) {
				logger.info("case-1: latest data for an item in group");
				options.addHeader("state", "1");
				options.addHeader("options", "latest");
				publishEvent(requested_data, options, response);
			}

			else if (requested_data.getString("options").contains("status")) {
				logger.info("case-1: status for an item in group");
				options.addHeader("state", "1");
				options.addHeader("options", "status");
				publishEvent(requested_data, options, response);
			}

			break;

		case 2:
			logger.info("case-2: latest data for all the items in group");
			options.addHeader("state", "2");
			publishEvent(requested_data, options, response);
			break;

		case 3:
			logger.info("case-3: time-series data for an item in group");
			options.addHeader("state", "3");
			publishEvent(requested_data, options, response);
			break;

		case 4:
			break;
			
		case 5:
			logger.info("case-5: geo search for an item group");
			options.addHeader("state", "5");
			publishEvent(requested_data, options, response);
			break;			
			
		}
	}

	private void count(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();

		JsonObject requested_data = new JsonObject();
		DeliveryOptions options = new DeliveryOptions();
		requested_data = routingContext.getBodyAsJson();
		api = "count";

		switch (decoderequest(requested_data)) {

		case 4:
			logger.info("case-4: count for time-series data for an item in group");
			options.addHeader("state", "4");
			options.addHeader("options", "count");
			publishEvent(requested_data, options, response);
			break;
			
		case 6:
			logger.info("case-6: count for geo search for an item group");
			options.addHeader("state", "6");
			options.addHeader("options", "count");
			publishEvent(requested_data, options, response);
			break;
		}
	}
	
	private int decoderequest(JsonObject requested_data) {

		int state = 0;

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
	
	private void publishEvent(JsonObject requested_data, DeliveryOptions options, HttpServerResponse response ) {
		vertx.eventBus().send("search", requested_data, options, replyHandler -> {
			if (replyHandler.succeeded()) {
				handle200(response, replyHandler);
			} else {
				response.setStatusCode(400).end();
			}
		});
	}
	
	private void handle200(HttpServerResponse response, AsyncResult<Message<Object>> replyHandler) {
		response.setStatusCode(200).putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
				.end(replyHandler.result().body().toString());
	}

}
