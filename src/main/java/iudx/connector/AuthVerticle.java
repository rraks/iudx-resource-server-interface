package iudx.connector;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class AuthVerticle extends AbstractVerticle {

	private static final Logger logger = LoggerFactory.getLogger(AuthVerticle.class.getName());

	@Override
	public void start() throws Exception {

		logger.info("Auth Verticle started!");

		vertx.eventBus().consumer("token-introspect", message -> {
			logger.info("Got the event : " + message.body().toString());
			JsonObject requested_body = new JsonObject(message.body().toString());
			String token = requested_body.getString("token");
			introspect(token, message);
		});
	}

	private void introspect(String token, Message<Object> message) {

		JsonObject request = new JsonObject(message.body().toString());
		
		logger.info("Token is : " +token);
		
		message.reply("SUCCESS");
		// OR 
		message.fail(0, "FAILURE");
	}
}
