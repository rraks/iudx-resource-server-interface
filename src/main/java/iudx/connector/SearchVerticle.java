package iudx.connector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;

public class SearchVerticle extends AbstractVerticle {

	private static final Logger logger = LoggerFactory.getLogger(SearchVerticle.class);
	private static String options;
	private static String resource_group_id;
	private static String resource_id;
	private static String time;
	private static String[] timeStamp;
	private static String startTime;
	private static String endTime;
	private static String TRelation;

	private JsonObject query, isotime;
	
	private MongoClient mongo;
	private JsonObject	mongoconfig;
	private String 		database_host;
	private int 		database_port;
	private String 		database_user;
	private String 		database_password;
	private String 		database_name;
	private String 		auth_database;
	private String 		connectionStr;

	@Override
	public void start() throws Exception {
		logger.info("Search Verticle started!");

		vertx.eventBus().consumer("search", message -> {
			search(message);
		});

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
	        auth_database		=	prop.getProperty("auth_database");
	        	        

	        logger.debug("database_user 	: " + database_user);
	        logger.debug("database_password	: " + database_password);
	        logger.debug("database_host 	: " + database_host);
	        logger.debug("database_port 	: " + database_port);
	        logger.debug("database_name		: " + database_name);
	        logger.debug("auth_database		: " + auth_database);
	        
	        
	        input.close();
	        
	    } 
	    catch (Exception e) 
	    {
	        e.printStackTrace();
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

	private void search(Message<Object> message) {

		int state = Integer.parseInt(message.headers().get("state"));
		options = message.headers().get("options");
		JsonObject requested_body = new JsonObject(message.body().toString());

		logger.info("state : " + state);
		logger.info("requested_body : " + requested_body);
		logger.info("options : " + options);
		JsonObject query = constructQuery(state, message);

		logger.info(query.toString());

		JsonObject fields = new JsonObject();

		if (requested_body.containsKey("attribute-filter")) {
			System.out.println(requested_body.getJsonArray("attribute-filter"));
			JsonArray attributeFilter = requested_body.getJsonArray("attribute-filter");
			for (int i = 0; i < attributeFilter.size(); i++) {
				String field = attributeFilter.getString(i);
				if (field.charAt(0) == '$') {
					field = "_$_" + field.substring(1);
				}
				fields.put(field, 1);
			}
		}

		searchDatabase(state, "archive", query, fields, message);
	}

	private JsonObject constructQuery(int state, Message<Object> message) {

		JsonObject request = (JsonObject) message.body();
		query = new JsonObject();
		isotime = new JsonObject();

		switch (state) {
		case 1:
			options = request.getString("options");
			resource_group_id = request.getString("resource-group-id");
			resource_id = request.getString("resource-id");

			query.put("__resource-id", resource_id);
			break;

		case 2:
			options = request.getString("options");
			resource_group_id = request.getString("resource-group-id");
			return query;

		case 3:
			query = constructTimeSeriesQuery(request);
			return query;

		case 4:
			query = constructTimeSeriesQuery(request);
			return query;

		case 5:
			query = constructGeoCircleQuery(request);
			return query;

		case 6:
			query = constructGeoCircleQuery(request);
			return query;
		}

		return query;
	}

	double MetersToDecimalDegrees(double meters, double latitude) {
		return meters / (111.32 * 1000 * Math.cos(latitude * (Math.PI / 180)));
	}

	private JsonObject constructTimeSeriesQuery(JsonObject request) {

		resource_group_id = request.getString("resource-group-id");
		resource_id = request.getString("resource-id");
		time = request.getString("time");
		TRelation = request.getString("TRelation");

		if (TRelation.contains("during")) {
			timeStamp = time.split("/");
			startTime = timeStamp[0];
			endTime = timeStamp[1];

			query.put("__resource-id", resource_id);
			isotime.put("$gte", startTime);
			isotime.put("$lte", endTime);
			query.put("LASTUPDATEDATETIME", isotime);
		}

		else if (TRelation.contains("before")) {

			query.put("__resource-id", resource_id);
			isotime.put("$lte", time);
			query.put("LASTUPDATEDATETIME", isotime);
		}

		else if (TRelation.contains("after")) {

			query.put("__resource-id", resource_id);
			isotime.put("$gte", time);
			query.put("LASTUPDATEDATETIME", isotime);
		}

		else if (TRelation.contains("TEquals")) {

			query.put("__resource-id", resource_id);
			isotime.put("$eq", time);
			query.put("LASTUPDATEDATETIME", isotime);

		}

		return query;

	}

	private JsonObject constructGeoCircleQuery(JsonObject request) {

		resource_group_id = request.getString("resource-group-id");
		resource_id = request.getString("resource-id");
		double latitude = Double.parseDouble(request.getString("lat"));
		double longitude = Double.parseDouble(request.getString("lon"));
		double rad = MetersToDecimalDegrees(Double.parseDouble(request.getString("radius")), latitude);

		query = new JsonObject();
		query.put("location", new JsonObject().put("$geoWithin", new JsonObject().put("$center",
				new JsonArray().add(new JsonArray().add(longitude).add(latitude)).add(rad))));

		return query;

	}

	private void searchDatabase(int state, String COLLECTION, JsonObject query, JsonObject attributeFilter,
			Message<Object> message) {

		JsonObject sortFilter;
		FindOptions findOptions;
		String api;

		switch (state) {

		case 1:
			attributeFilter.put("_id", 0);

			sortFilter = new JsonObject();
			sortFilter.put("__time", -1);

			findOptions = new FindOptions();
			findOptions.setFields(attributeFilter);
			findOptions.setLimit(1);
			findOptions.setSort(sortFilter);

			if (options.contains("latest")) {
				api = "latest";
				mongoFind(api, state, COLLECTION, query, findOptions, message);
			}

			else if (options.contains("status")) {
				api = "status";
				mongoFind(api, state, COLLECTION, query, findOptions, message);
			}

			break;

		case 2:
		case 3:
			api = "search";
			attributeFilter.put("_id", 0);

			findOptions = new FindOptions();
			findOptions.setFields(attributeFilter);
			mongoFind(api, state, COLLECTION, query, findOptions, message);
			break;

		case 4:
			api = "count";
			mongoCount(state, COLLECTION, query, message);
			break;

		case 5:
			api = "search";
			attributeFilter.put("_id", 0);

			findOptions = new FindOptions();
			findOptions.setFields(attributeFilter);
			mongoFind(api, state, COLLECTION, query, findOptions, message);
			break;

		case 6:
			api = "count";
			mongoCount(state, COLLECTION, query, message);
			break;

		}
	}

	private void mongoFind(String api, int state, String COLLECTION, JsonObject query, FindOptions findOptions,
			Message<Object> message) {
		String[] hiddenFields = { "__resource-id", "__time", "__geoJsonLocation", "_id", "__resource-group" };

		mongo.findWithOptions(COLLECTION, query, findOptions, database_response -> {
			if (database_response.succeeded()) {
				JsonArray response = new JsonArray();

				for (JsonObject j : database_response.result()) {

					if (api.equalsIgnoreCase("status")) {
						JsonObject status = new JsonObject();
						System.out.println(j);
						JsonObject __time = j.getJsonObject("__time");
						String time = __time.getString("$date");
						DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS[XXX][X]");
						LocalDateTime sensedDateTime = LocalDateTime.parse(time, format);
						LocalDateTime currentDateTime = LocalDateTime.now();
						long timeDifference = Duration.between(sensedDateTime, currentDateTime).toHours();

						logger.info("Last Status Update was at : " + sensedDateTime.toString());
						logger.info("Current Time is : " + currentDateTime.toString());
						logger.info("Time Difference is : " + timeDifference);

						if (timeDifference <= 24) {

							status.put("status", "live");
						}

						else {

							status.put("status", "down");
						}

						response.add(status);

					}

					else {

						for (String hidden : hiddenFields) {
							if (j.containsKey(hidden)) {
								j.remove(hidden);
							}
						}
						response.add(j);
					}
				}

				logger.info("Database Reply is : " + database_response.result().toString());
				logger.info("Response is : " + response.toString());

				message.reply(response);

			} else {
				message.fail(1, "item-not-found");
			}
		});

	}

	private void mongoCount(int state, String COLLECTION, JsonObject query, Message<Object> message) {
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
