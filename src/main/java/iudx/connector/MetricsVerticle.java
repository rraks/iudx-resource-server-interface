package iudx.connector;

import java.util.logging.Logger;

import io.vertx.core.AbstractVerticle;

public class MetricsVerticle extends AbstractVerticle {
	
	private static final Logger logger = Logger.getLogger(MetricsVerticle.class.getName());

	@Override
	public void start() throws Exception {

		logger.info("Metrics Verticle started!");

		vertx.eventBus().consumer("metrics", message -> {
			logger.info("Got the event : " + message.body().toString());
			message.reply("SUCCESS");
		});

	}
	
}
