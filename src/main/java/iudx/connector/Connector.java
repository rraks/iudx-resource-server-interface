package iudx.connector;

import java.util.logging.Logger;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;

public class Connector {

	private static final Logger logger = Logger.getLogger(Connector.class.getName());

	public static void main(String[] args) {
		int procs = Runtime.getRuntime().availableProcessors();
		Vertx vertx = Vertx.vertx();

		vertx.deployVerticle(APIServerVerticle.class.getName(),
				new DeploymentOptions().setWorker(true).setInstances(procs * 2), event -> {
					if (event.succeeded()) {
						logger.info("IUDX Connector Vert.x API Server is started!");
					} else {
						logger.info("Unable to start IUDX v Vert.x API Server " + event.cause());
					}
				});

		vertx.deployVerticle(SearchVerticle.class.getName(),
				new DeploymentOptions().setWorker(true).setInstances(1), event -> {
					if (event.succeeded()) {
						logger.info("Search Verticle is started!");
					} else {
						logger.info("Unable to start Search Verticle " + event.cause());
					}
				});
		
		vertx.deployVerticle(MetricsVerticle.class.getName(),
				new DeploymentOptions().setWorker(true).setInstances(1), event -> {
					if (event.succeeded()) {
						logger.info("MetricsVerticle started!");
					} else {
						logger.info("Unable to start MetricsVerticle " + event.cause());
					}
				});
	}
}
