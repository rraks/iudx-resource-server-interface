package iudx.connector;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Launcher;
import io.vertx.core.Promise;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Connector extends AbstractVerticle {

	public	final static Logger logger = LoggerFactory.getLogger(Connector.class);
	private Set<String> items;
	
	public static void main(String[] args) {
		Launcher.executeCommand("run", Connector.class.getName());
	}
	
	public static void main(String[] args) {
		Launcher.executeCommand("run", Connector.class.getName());
	}
	
	
	@Override
	public void start(Future<Void> startFuture)throws Exception
	{
		Future<Void> future;

		future=readItemsFile();
		future.setHandler(res -> {
			if(res.succeeded()){
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
			else{
				startFuture.fail(res.cause().toString());
			}
		});


	}

	private Future<Void> readItemsFile(){
		Promise promise=Promise.promise();
		items=new HashSet<>();
		FileSystem vertxFileSystem = vertx.fileSystem();
		vertxFileSystem.readFile("items.json", readFile -> {
			if (readFile.succeeded()) {
				System.out.println("Read the file");
				JsonArray itemsArray = readFile.result().toJsonArray();

				for(int i=0;i<itemsArray.size();i++) {
					JsonObject jo = itemsArray.getJsonObject(i);
					String __id = (String) jo.getString("id");
					items.add(__id);
				}

				ItemsSingleton iS = ItemsSingleton.getInstance();
				//
				iS.setItems(items);
				logger.info("Updated items list. Totally loaded " + items.size() + " items");
				promise.complete();
			}
		});

		return promise.future();
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
