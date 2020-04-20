package iudx.connector;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import io.vertx.core.json.JsonObject;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@RunWith(VertxUnitRunner.class)
public class AuthVerticleTest {

    private static final Logger logger = LoggerFactory.getLogger(AuthVerticle.class.getName());
    private Vertx vertx;

    @Before
    public void setUp(TestContext tc) {
        vertx = Vertx.vertx();

        JsonObject conf = new JsonObject()
            .put("authserver.jksfile", "config/authkeystore_example.jks")
            .put("authserver.jkspasswd", "1!Rbccps-voc@123")
            .put("authserver.url", "auth.iudx.org.in");

        vertx.deployVerticle(AuthVerticle.class.getName(), new DeploymentOptions().setConfig(conf),
                tc.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext tc) {
        vertx.close(tc.asyncAssertSuccess());
    }


    @Test
    public void testValidateToken(TestContext tc) {

        logger.info("Starting test");
	    DeliveryOptions	options = new DeliveryOptions();
        options.addHeader("action", "token-introspect");
        JsonObject requestedData = new JsonObject().put("token", "auth.iudx.org.in/rakshitr@iisc.ac.in/0d48e0184d690d2dd8fbe73a0b3b45e8");
		vertx.eventBus().request("authqueue", requestedData, options, replyHandler -> {
			if (replyHandler.succeeded())
			{
                logger.info("Succeded test");
				// logger.info(replyHandler.result().body().toString());
			} else {
                logger.info("Failed");
			}
		});

    }
}
