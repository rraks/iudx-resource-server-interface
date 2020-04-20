package iudx.connector;

import java.io.FileInputStream;
import java.util.Properties;
import java.io.InputStream;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.codec.BodyCodec;

public class AuthVerticle extends AbstractVerticle {

	private static final Logger logger = LoggerFactory.getLogger(AuthVerticle.class.getName());

    private static final String AUTH_KEYSTORE_PATH = "authserver.jksfile";
    private static final String AUTH_KEYSTORE_PASSWORD = "authserver.jkspasswd";
    private static final String AUTH_URL = "authserver.url";


    // Properties prop = new Properties();
    // InputStream input = null;


    private WebClient client;
    private String url;



	@Override
	public void start() throws Exception {
        
        // input = new FileInputStream("config.properties");
        // prop.load(input);
        
        	
        /** TODO: Use config.properties instead */
        WebClientOptions options = new WebClientOptions()
                                       .setSsl(true)
                                       .setKeyStoreOptions(new JksOptions()
                                           .setPath(config().getString(AUTH_KEYSTORE_PATH))
                                           .setPassword(config().getString(AUTH_KEYSTORE_PASSWORD)));

        client = WebClient.create(vertx, options);
        url = config().getString(AUTH_URL);

		logger.info("Auth Verticle started!");

        /** Assume message is a json-object */
		vertx.eventBus().consumer("authqueue", this::onMessage);
    }

    private void onMessage(Message<JsonObject> message) {
        if (!message.headers().contains("action")) {
            logger.error("No action header specified for message with headers {} and body {}",
                    message.headers(), message.body().encodePrettily());
            message.fail(0, "No action header specified");
            return;
        }
        String action = message.headers().get("action");
        switch (action) {
            case "token-introspect":
                introspect(message);
                  break;
        }
    }

    private void introspect(Message<JsonObject> message) {
        JsonObject resp = new JsonObject();
        String token = message.body().getString("token");

		logger.info("Token is : " +token);
        logger.info("Auth url is " + url);
        client
            .post(443, url, "/auth/v1/token/introspect")
            .putHeader("content-type", "application/json")
            .as(BodyCodec.jsonObject())
            .sendJsonObject(new JsonObject().put("token", token),
            ar -> {
                logger.info("Status Code is " + ar.result().statusCode());
                if (ar.succeeded()) {
                    if (ar.result().statusCode() == 200) {
                        logger.info("Valid token");
                        message.reply(resp.put("valid", true));
                    } else {
                        /** TODO: Replace with Auth server URL */
                        logger.info("Invalid token");
                        message.fail(0, "Invalid token");
                    }
                } else {
                    /** TODO: Replace with Auth server URL */
                    logger.info("Invalid token");
                    message.fail(0, "Invalid token");
                }
            });
	}
}
