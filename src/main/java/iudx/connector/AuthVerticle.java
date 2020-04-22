package iudx.connector;

import java.util.regex.Pattern;

import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Properties;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;



public class AuthVerticle extends AbstractVerticle {

	private static final Logger logger = LoggerFactory.getLogger(AuthVerticle.class.getName());

    private static final String AUTH_KEYSTORE_PATH = "authKeystore";
    private static final String AUTH_KEYSTORE_PASSWORD = "authKeystorePassword";
    private static final String AUTH_URL = "authUrl";

    private WebClient client;
    private String url;



	@Override
	public void start() throws Exception {
        
        InputStream input = new FileInputStream("config.properties");
		Properties prop = new Properties();
        prop.load(input);

        logger.info("JKS File path = " + prop.getProperty(AUTH_KEYSTORE_PATH));
        logger.info("JKS File password = " + prop.getProperty(AUTH_KEYSTORE_PASSWORD));
        logger.info("Auth URL = " + prop.getProperty(AUTH_URL));

        WebClientOptions options = new WebClientOptions()
                                       .setSsl(true)
                                       .setKeyStoreOptions(new JksOptions()
                                           .setPath(prop.getProperty(AUTH_KEYSTORE_PATH))
                                           .setPassword(prop.getProperty(AUTH_KEYSTORE_PASSWORD)));

        client = WebClient.create(vertx, options);
        url = prop.getProperty(AUTH_URL);
		logger.info("Auth Verticle started!");

        /** Assume message is a json-object */
		vertx.eventBus().consumer("auth-queue", this::onMessage);
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
        String token = message.body().getString("token");
        String id = message.body().getString("id");

        String serverToken = message.body().getString("server-token");

	if (serverToken == null)
		serverToken = "true";

        logger.info("Validating token " + token);
        logger.info("For id " + id);
        logger.info("For server-token " + serverToken);

        JsonObject tokenObject = new JsonObject()
					.put("token", token)
					.put("server-token",serverToken);

        client
            .post(443, url, "/auth/v1/token/introspect")
            .ssl(true)
            .putHeader("content-type", "application/json")
            .sendJsonObject(tokenObject,
            ar -> {
                if (ar.succeeded()) {
                    logger.info("Status code for the request is " + String.valueOf(ar.result().statusCode()));
                    if (ar.result().statusCode() == 200) {
                        logger.info("Got response " + ar.result().bodyAsJsonObject().encode());
                        JsonArray validPatterns = ar.result().bodyAsJsonObject()
                                                        .getJsonArray("request");
                        logger.info("Got valid ids " + validPatterns.encode());
                        int validToken = 0;
                        for (int i = 0; i<validPatterns.size(); i++) {
                            Pattern patObj = Pattern.compile(validPatterns
                                    .getJsonObject(i)
                                    .getString("id")
                                    .replace("/", "\\/")
                                    .replace(".", "\\.")
                                    .replace("*", ".*"));

                            if (patObj.matcher(id).matches()) validToken = 1;
                        }
                        if (validToken == 1 ){
                            logger.info("Obtained valid token");
                            message.reply(new JsonObject().put("valid", "true"));
                        } else {
                            /** TODO: Replace with Auth server URL */
                            logger.info("Obtained invalid token");
                            message.fail(0, "Fail");
                        }
                    } else {
                        /** TODO: Replace with Auth server URL */
                        logger.info("Invalid token");
                        message.fail(0, "Fail");
                    }
                } else {
                    /** TODO: Replace with Auth server URL */
                    message.fail(0, "Fail");
                    logger.info("Invalid token");
                }
            });
	}
}
