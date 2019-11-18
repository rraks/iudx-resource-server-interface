package iudx.connector;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class Connector extends AbstractVerticle {

	public	final static Logger logger = LoggerFactory.getLogger(Connector.class);
	
	@Override
	public void start(Future<Void> startFuture)throws Exception
	{
		deployHelper(APIServerVerticle.class.getName())
		.setHandler(apiserver ->
		{
			if(!apiserver.succeeded())
			{
				logger.debug(apiserver.cause());
				startFuture.fail(apiserver.cause().toString());
			}
			
			deployHelper(SearchVerticle.class.getName())
			.setHandler(search -> {
				
			if(!search.succeeded())
			{
				logger.debug(search.cause());
				startFuture.fail(search.cause().toString());
			}
				
			deployHelper(MetricsVerticle.class.getName())
			.setHandler(metrics -> {
					
			if(!metrics.succeeded())
			{
				logger.debug(metrics.cause());
				startFuture.fail(metrics.cause().toString());
			}
			
			deployHelper(SubscriptionVerticle.class.getName())
			.setHandler(subscription -> {
				
			if(!subscription.succeeded())
			{
			    logger.debug(metrics.cause());
			    startFuture.fail(subscription.cause().toString());
			}
			
			deployHelper(ValidityVerticle.class.getName())
			.setHandler(validity -> {
				
			if(!validity.succeeded())
			{
			    logger.debug(validity.cause());
			    startFuture.fail(validity.cause().toString());
			}
			
			startFuture.complete();
			
						});
					});
				});
			});
		});
	}
	
	private Future<Void> deployHelper(String name)
	{
		   final Future<Void> future = Future.future();
		   int procs = Runtime.getRuntime().availableProcessors();
		   
		   if("iudx.connector.APIServerVerticle".equals(name) || "iudx.connector.SearchVerticle".equals(name))
		   {
			   vertx.deployVerticle(name, new DeploymentOptions()
					   					  .setWorker(true)
					   					  .setInstances(procs * 2), res -> {
			   if(res.succeeded()) 
			   {
				   logger.info("Deployed Verticle " + name);
				   future.complete();
			   }
			   else
			   {
				   logger.fatal("Failed to deploy verticle " + res.cause());
				   future.fail(res.cause());
			   }
					   						  
			});
		   }
		   else
		   {
			   vertx.deployVerticle(name, res -> 
			   {
			      if(res.failed())
			      {
			         logger.fatal("Failed to deploy verticle " + name + " Cause = "+res.cause());
			         future.fail(res.cause());
			      } 
			      else 
			      {
			    	 logger.info("Deployed Verticle " + name);
			         future.complete();
			      }
			   });
		   }
		   
		   return future;
	}
}
