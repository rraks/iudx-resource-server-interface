package iudx.connector;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
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

	private JsonObject query, isotime, dateTime, startDateTime, endDateTime;
	private Instant instant, startInstant, endInstant;
	
	private MongoClient mongo;
	private JsonObject	mongoconfig;
	private String 		database_host;
	private int 		database_port;
	private String 		database_user;
	private String 		database_password;
	private String 		database_name;
	private String 		auth_database;
	private String 		connectionStr;
	private static final String COLLECTION = "archive";
	private JsonObject resourceQuery,finalQuery;

    
    private String geometry="", relation="", coordinatesS="";
    private String[] coordinatesArr;
    private JsonArray coordinates;
    private JsonArray expressions;

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
		finalQuery = new JsonObject();
		resourceQuery = new JsonObject();
		isotime = new JsonObject();
		resource_group_id = request.getString("resource-group-id");
		resource_id = request.getString("resource-id");
		resourceQuery.put("__resource-id",resource_id);
		resourceQuery.put("__resource-group",resource_group_id);

		switch (state) {
		case 1:
			options = request.getString("options");
			//resource_group_id = request.getString("resource-group-id");
			//resource_id = request.getString("resource-id");
			//resourceQuery.put("__resource-id", resource_id);
			return resourceQuery;

		case 2:
			options = request.getString("options");
			//resource_group_id = request.getString("resource-group-id");
			query=resourceQuery;
			return query;

		case 3:
			query = constructTimeSeriesQuery(request);
			break;

		case 4:
			query = constructTimeSeriesQuery(request);
			break;

		case 5:
			query = constructGeoCircleQuery(request);
			break;

		case 6:
			query = constructGeoCircleQuery(request);
			break;

        case 7:
			query = constructGeoBboxQuery(request);
            break;

        case 8:
            query = constructGeoPoly_LineQuery(request);
            break;
        
        case 9:
            query = constructGeoBboxQuery(request);
            break;
        
        case 10:
            query = constructGeoPoly_LineQuery(request);
            break;

		case 11:
			query = constructAttributeQuery(request);
			break;

		case 12:
			query = constructAttributeQuery(request);
			break;
		}
		expressions=new JsonArray();
		expressions.add(resourceQuery).add(query);
		finalQuery.put("$and",expressions);
		System.out.println("FINAL QUERY: "+finalQuery.toString());
		return finalQuery;
	}

	double MetersToDecimalDegrees(double meters, double latitude) {
		return meters / (111.32 * 1000 * Math.cos(latitude * (Math.PI / 180)));
	}

	private JsonObject constructTimeSeriesQuery(JsonObject request) {

		resource_group_id = request.getString("resource-group-id");
		resource_id = request.getString("resource-id");
		time = request.getString("time");
		TRelation = request.getString("TRelation");
		JsonObject attributeQuery = new JsonObject();
		JsonObject timeQuery = new JsonObject();
		JsonArray expressions = new JsonArray();
		if (TRelation.contains("during")) {

			timeStamp = time.split("/");
			startTime = timeStamp[0];
			endTime = timeStamp[1];
			
			if(startTime.contains("Z")) 
			{
				startInstant = Instant.parse(startTime);
			}
			
			else 
			{
				OffsetDateTime start = OffsetDateTime.parse( startTime );
				startInstant = start.toInstant();
			}
			
			startDateTime = new JsonObject();
			startDateTime.put("$date", startInstant);
			
			if(endTime.contains("Z")) 
			{
				endInstant = Instant.parse(endTime);
			}
			
			else 
			{
				OffsetDateTime end = OffsetDateTime.parse( endTime );
				endInstant = end.toInstant();
			}
	
			endDateTime = new JsonObject();
			endDateTime.put("$date", endInstant);
			
			isotime.put("$gte", startDateTime);
			isotime.put("$lte", endDateTime);
			timeQuery.put("__time", isotime);
		}

		else if (TRelation.contains("before")) {

			if(time.contains("Z")) 
			{
				instant = Instant.parse(time);
			}
			
			else 
			{
				OffsetDateTime start = OffsetDateTime.parse( time );
				instant = start.toInstant();
			}

			dateTime = new JsonObject();
			dateTime.put("$date", instant);

			isotime.put("$lte", dateTime);
			timeQuery.put("__time", isotime);
		}

		else if (TRelation.contains("after")) {
			
			if(time.contains("Z")) 
			{
				instant = Instant.parse(time);
			}
			
			else 
			{
				OffsetDateTime start = OffsetDateTime.parse( time );
				instant = start.toInstant();
			}

			dateTime = new JsonObject();
			dateTime.put("$date", instant);
			
			//timeQuery.put("__resource-id", resource_id);
			isotime.put("$gte", dateTime);
			timeQuery.put("__time", isotime);
		}

		else if (TRelation.contains("TEquals")) {

			if(time.contains("Z")) 
			{
				instant = Instant.parse(time);
			}
			
			else 
			{
				OffsetDateTime start = OffsetDateTime.parse( time );
				instant = start.toInstant();
			}

			dateTime = new JsonObject();
			dateTime.put("$date", instant);

			//query.put("__resource-id", resource_id);
			isotime.put("$eq", dateTime);
			query.put("__time", isotime);

		}
		if(request.containsKey("attribute-name") && request.containsKey("attribute-value")){
			attributeQuery = constructAttributeQuery(request);
			expressions.add(timeQuery).add(attributeQuery);
			query.put("$and",expressions);
			logger.info("TIME QUERY + ATTRIBUTE QUERY");
		}else
			query = timeQuery;

		System.out.println(query);
		
		return query;

	}

	private JsonObject constructGeoCircleQuery(JsonObject request) {
		double latitude = Double.parseDouble(request.getString("lat"));
		double longitude = Double.parseDouble(request.getString("lon"));
		double rad = MetersToDecimalDegrees(Double.parseDouble(request.getString("radius")), latitude);

		query = new JsonObject();
		query.put("__geoJsonLocation", new JsonObject().put("$geoWithin", new JsonObject().put("$center",
				new JsonArray().add(new JsonArray().add(longitude).add(latitude)).add(rad))));

		return query;

	}

    private JsonObject constructGeoBboxQuery(JsonObject request){
        geometry="bbox";
        JsonObject geoQuery = new JsonObject();
		JsonArray expressions = new JsonArray();
		coordinates = new JsonArray();
        relation = request.containsKey("relation")?request.getString("relation").toLowerCase():"intersects";
        boolean valid = validateRelation(geometry, relation);
        if(valid){
            coordinatesS = request.getString("bbox");
            coordinatesArr = coordinatesS.split(",");
            JsonArray temp = new JsonArray();
            JsonArray y1x1 = new JsonArray().add(getDoubleFromS(coordinatesArr[1])).add(getDoubleFromS(coordinatesArr[0]));
            JsonArray y1x2 = new JsonArray().add(getDoubleFromS(coordinatesArr[1])).add(getDoubleFromS(coordinatesArr[2]));
            JsonArray y2x2 = new JsonArray().add(getDoubleFromS(coordinatesArr[3])).add(getDoubleFromS(coordinatesArr[2]));
            JsonArray y2x1 = new JsonArray().add(getDoubleFromS(coordinatesArr[3])).add(getDoubleFromS(coordinatesArr[0]));
            temp.add(y1x1).add(y1x2).add(y2x2).add(y2x1).add(y1x1);
            coordinates.add(temp);
            geoQuery = buildGeoQuery("Polygon",coordinates,relation);
        
        } else
            geoQuery=null;

        if (request.containsKey("attribute-name") && request.containsKey("attribute-value")){
			JsonObject attributeQuery = constructAttributeQuery(request);
			expressions.add(geoQuery).add(attributeQuery);
			query.put("$and",expressions);
		} else
			query=geoQuery;

        return query;
    }

    private JsonObject constructGeoPoly_LineQuery(JsonObject request){

		JsonObject geoQuery = new JsonObject();
		JsonArray expressions = new JsonArray();
		coordinates = new JsonArray();
        //Polygon or LineString
        if(request.containsKey("geometry")){
            if(request.getString("geometry").toUpperCase().contains("Polygon".toUpperCase()))
                geometry = "Polygon";
        else if(request.getString("geometry").toUpperCase().contains("lineString".toUpperCase()))
            geometry = "LineString";
        }

        relation = request.containsKey("relation")?request.getString("relation").toLowerCase():"intersects";
        boolean valid = validateRelation(geometry, relation);
        if(valid){
            switch(geometry){
                case "Polygon":
                    coordinatesS = request.getString("geometry");
                    coordinatesS = coordinatesS.replaceAll("[a-zA-Z()]","");
                    coordinatesArr = coordinatesS.split(",");
                    JsonArray extRing = new JsonArray();
                    for (int i = 0 ; i<coordinatesArr.length;i+=2){
                        JsonArray points = new JsonArray();
                        points.add(getDoubleFromS(coordinatesArr[i+1])).add(getDoubleFromS(coordinatesArr[i]));
                        extRing.add(points);
                    }
                    coordinates.add(extRing);
                    System.out.println("QUERY: " + coordinates.toString());
                    query = buildGeoQuery(geometry,coordinates,relation);
                    break;

                case "LineString":
                    coordinatesS = request.getString("geometry");
                    coordinatesS = coordinatesS.replaceAll("[a-zA-Z()]","");
                    coordinatesArr = coordinatesS.split(",");
                    for (int i = 0 ; i<coordinatesArr.length;i+=2){
                        JsonArray points = new JsonArray();
                        points.add(getDoubleFromS(coordinatesArr[i+1])).add(getDoubleFromS(coordinatesArr[i]));
                        coordinates.add(points);
                    }
                    query = buildGeoQuery(geometry,coordinates,relation);
                    break;
				default:
					throw new IllegalStateException("Unexpected value: " + geometry);
			}
        }else 
            geoQuery=null;

		if (request.containsKey("attribute-name") && request.containsKey("attribute-value")){
			JsonObject attributeQuery = constructAttributeQuery(request);
			expressions.add(geoQuery).add(attributeQuery);
			query.put("$and",expressions);
		} else
			query=geoQuery;

        return query;
    }

	private JsonObject constructAttributeQuery(JsonObject request){

		JsonObject query = new JsonObject();
		String attribute_name = request.getString("attribute-name");
		String attribute_value = request.getString("attribute-value");
		String comparison_operator;
		comparison_operator=request.getString("comparison-operator").toLowerCase();
		System.out.println("ATTRIBUTE SEARCH");
		Double attr_v_n=0.0;
		if(isNumeric(attribute_value))
			attr_v_n=Double.parseDouble(attribute_value);

			switch (comparison_operator){

				case "propertyisequalto":
					query.put(attribute_name,attribute_value);
					break;

				case "propertyisnotequalto":
					query = numericQuery(attribute_name,attr_v_n,"$ne");
					break;

				case "propertyislessthan":
					query = numericQuery(attribute_name,attr_v_n,"$lt");
					break;

				case "propertyisgreaterthan":
					query = numericQuery(attribute_name,attr_v_n,"$gt");
					break;

				case "propertyislessthanequalto":
					query = numericQuery(attribute_name,attr_v_n,"$lte");
					break;

				case "propertyisgreaterthanequalto":
					query = numericQuery(attribute_name,attr_v_n,"$gte");
					break;

				case "propertyislike":
					query.put(attribute_name,new JsonObject().put("$regex",attribute_value)
																.put("$options","i"));
					break;

				case "propertyisbetween":
					String[] attr_arr = attribute_value.split(",");
					query.put("$expr",new JsonObject()
										.put("$and",new JsonArray().add(new JsonObject()
													.put("$gt",new JsonArray().add(new JsonObject().put("$convert", new JsonObject().put("input","$"+attribute_name)
																													.put("to","double")
																													.put("onError","No numeric value available (NA/Unavailable)")
																													.put("onNull","No value available")))
																				.add(getDoubleFromS(attr_arr[0]))))
													.add(new JsonObject()
															.put("$lt",new JsonArray().add(new JsonObject().put("$convert", new JsonObject().put("input","$"+attribute_name)
																													.put("to","double")
																													.put("onError","No numeric value available (NA/Unavailable)")
																													.put("onNull","No value available")))
																					.add(getDoubleFromS(attr_arr[1]))))));

					break;

			}
		return query;
	}

	/**
	 * Helper function to determine if the attribute-value has a numeric value as String
	 **/

	private boolean isNumeric(String s){

		try {
				double d = Double.parseDouble(s);
		}catch (NumberFormatException | NullPointerException e){
			return false;
		}
		return true;
	}

    private JsonObject buildGeoQuery(String geometry, JsonArray coordinates, String relation){

	JsonObject query = new JsonObject();

	switch(relation){

		case "equals": query = new JsonObject()
						.put("__geoJsonLocation.coordinates",coordinates );
				break;

		case "disjoint": break;

		case "touches": query = searchGeoIntersects(geometry,coordinates);
				break;

		case "overlaps": query = searchGeoIntersects(geometry,coordinates);
				 break;

		case "crosses": query = searchGeoIntersects(geometry,coordinates);
				break;

		case "contains": break;

		case "intersects": query = searchGeoIntersects(geometry,coordinates);
				            break;

		case "within": query = searchGeoWithin(geometry,coordinates);
				        break;

        default: break;
	}

    return query;
  }

  /**
   * Helper function to convert string values to Double
   */
  private Double getDoubleFromS(String s){
    Double d = Double.parseDouble(s);
    return d;
  }

  /**
   * Helper function to generate query in mongo to compare Numeric values encoded
   * as Strings with Numbers
   **/
  private JsonObject numericQuery(String attrName, Double attrValue, String comparisonOp){

  	JsonObject query = new JsonObject();
  	query.put("$expr", new JsonObject()
						.put(comparisonOp,new JsonArray()
									.add(new JsonObject()
											.put("$convert", new JsonObject().put("input","$"+attrName)
																.put("to","double")
																.put("onError","No numeric value available (NA/Unavailable)")
																.put("onNull","No value available")))
									.add(attrValue)));
  	return query;
  }


  /**
   * Performs Mongo-GeoIntersects operation
   * */

  private JsonObject searchGeoIntersects(String geometry, JsonArray coordinates){

	JsonObject query = new JsonObject();

	query.put("__geoJsonLocation", new JsonObject()
				.put("$geoIntersects", new JsonObject()
					.put("$geometry",new JsonObject()
						.put("type",geometry)
						.put("coordinates",coordinates))));
	System.out.println("GeoIntersects: "+query.toString());
    return query;
  }

  /**
   * Performs Mongo-GeoWithin operation
   * */

  private JsonObject searchGeoWithin(String geometry, JsonArray coordinates){

    JsonObject query = new JsonObject();
    query.put("__geoJsonLocation", new JsonObject()
            .put("$geoWithin", new JsonObject()
                .put("$geometry",new JsonObject()
                    .put("type",geometry)
                    .put("coordinates",coordinates))));
    System.out.println("GeoWithin: " + query.toString());
    return query;
  }

  /**
   *Helper function to validate relation for specified geometry.
   */

  private boolean validateRelation(String geometry, String relation){

	if(geometry.equalsIgnoreCase("bbox") && (relation.equalsIgnoreCase("equals")
							||relation.equalsIgnoreCase("disjoint")
							|| relation.equalsIgnoreCase("touches")
							|| relation.equalsIgnoreCase("overlaps")
							|| relation.equalsIgnoreCase("crosses")
							|| relation.equalsIgnoreCase("intersects")
							|| relation.equalsIgnoreCase("within") )){

		return true;
	}

    else if(geometry.equalsIgnoreCase("linestring") && (relation.equalsIgnoreCase("equals")
							||relation.equalsIgnoreCase("disjoint")
							|| relation.equalsIgnoreCase("touches")
							|| relation.equalsIgnoreCase("overlaps")
							|| relation.equalsIgnoreCase("crosses")
							|| relation.equalsIgnoreCase("intersects") )){

		return true;
	}
	else if(geometry.equalsIgnoreCase("polygon") && (relation.equalsIgnoreCase("equals")
							||relation.equalsIgnoreCase("disjoint")
							|| relation.equalsIgnoreCase("touches")
							|| relation.equalsIgnoreCase("overlaps")
							|| relation.equalsIgnoreCase("crosses")
							|| relation.equalsIgnoreCase("intersects")
							|| relation.equalsIgnoreCase("within") )){

		return true;
	}

    else
	   return false;

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
			sortFilter = new JsonObject();
			sortFilter.put("__time", -1);

			findOptions = new FindOptions();
			findOptions.setFields(attributeFilter);
			findOptions.setSort(sortFilter);
			mongoFind(api, state, COLLECTION, query, findOptions, message);
			break;

		case 4:
			api = "count";
			mongoCount(state, COLLECTION, query, message);
			break;

		case 5:
			api = "search";
			attributeFilter.put("_id", 0);
			sortFilter = new JsonObject();
			sortFilter.put("__time", -1);

			findOptions = new FindOptions();
			findOptions.setFields(attributeFilter);
			findOptions.setSort(sortFilter);
			mongoFind(api, state, COLLECTION, query, findOptions, message);
			break;

		case 6:
			api = "count";
			mongoCount(state, COLLECTION, query, message);
			break;

		case 7:
			api="search";
			attributeFilter.put("_id", 0);
			sortFilter = new JsonObject();
			sortFilter.put("__time", -1);
			findOptions = new FindOptions();
			findOptions.setFields(attributeFilter);
			findOptions.setSort(sortFilter);
			mongoFind(api, state, COLLECTION, query, findOptions, message);
			break;

		case 8:
			api="search";
			attributeFilter.put("_id", 0);
			sortFilter = new JsonObject();
			sortFilter.put("__time", -1);
			findOptions = new FindOptions();
			findOptions.setFields(attributeFilter);
			findOptions.setSort(sortFilter);
			mongoFind(api, state, COLLECTION, query, findOptions, message);
			break;

		case 9:
			api="count";
			mongoCount(state,COLLECTION,query,message);
			break;

		case 10:
			api="count";
			mongoCount(state,COLLECTION,query,message);
			break;

		case 11:
			api="search";
			attributeFilter.put("_id", 0);
			JsonObject obj = (JsonObject) message.body();
			String requestOptions = obj.containsKey("options")?obj.getString("options"):null;
			findOptions = new FindOptions();
			if(requestOptions!=null){
				JsonObject sortFil = new JsonObject().put("__time",-1);
				findOptions.setSort(sortFil);
				findOptions.setLimit(1);
			}
			findOptions.setFields(attributeFilter);
			mongoFind(api, state, COLLECTION, query, findOptions, message);
			break;

		case 12:
			api="count";
			mongoCount(state,COLLECTION,query,message);
			break;
		}
	}

	private void mongoFind(String api, int state, String COLLECTION, JsonObject query, FindOptions findOptions,
			Message<Object> message) {
		String[] hiddenFields = { "__resource-id", "__time", "__geoJsonLocation", "_id", "__resource-group" };
		JsonObject requested_body = new JsonObject(message.body().toString());
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
					
					else if(requested_body.containsKey("token")) {


						for (String hidden : hiddenFields) {
							if (j.containsKey(hidden)) {
								j.remove(hidden);
							}
						}

						response.add(j);
						
					}

					else {

						for (String hidden : hiddenFields) {
							if (j.containsKey(hidden)) {
								j.remove(hidden);
							}
						}

						if(j.containsKey("Images"))
						{
						   j.remove("Images");
						}
						
						response.add(j);
					}
				}

				logger.info("Database Reply is : " + database_response.result().toString());
				logger.info("Response is : " + response.toString());

				message.reply(response);

			} else {
				logger.info("Database query has FAILED!!!");
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
