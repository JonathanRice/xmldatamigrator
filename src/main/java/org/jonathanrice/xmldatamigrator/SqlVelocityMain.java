package org.jonathanrice.xmldatamigrator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jonathanrice.xmldatamigrator.consumer.HeaderDetailVelocityConsumer;
import org.jonathanrice.xmldatamigrator.producer.HeaderSqlProducer;
import org.jonathanrice.xmldatamigrator.util.PropertyUtil;

/**
 * This class serves as the main point of entry for the velocity template generator.  It will initialize the properties
 *   create the producer thread, create the consumer threads, and start everything rolling
 * @author jrice
 *
 */
public class SqlVelocityMain {

	private static Logger logger = Logger.getLogger(SqlVelocityMain.class);
	
	public SqlVelocityMain() {
	}

	/**
	 * @param args pass a single argument, the name of the property file you wish to use
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
		//initialize the properties
		Properties props = new Properties();
		props.load(new FileInputStream("log4j.properties"));
		PropertyConfigurator.configure(props);
		
		logger.debug("Starting conversion main");
		String propFileName = "reserve.properties";
		if (args.length > 0) {
			propFileName = args[0];
		}
		PropertyUtil.setPropFileName(propFileName);
		
		//initialize the producer and communication queue
		BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<String>();
		// Creating Producer and Consumer Thread
		HeaderSqlProducer producer = new HeaderSqlProducer(sharedQueue);
		Thread prodThread = new Thread(producer);
		
		// initialize the consumer threads
		int numConsumerThreads = Integer.parseInt(PropertyUtil.getProperties().getProperty("num_consumer_threads"));
		ArrayList<Thread> consThreads = new ArrayList<Thread>();
		for(int i = 0; i < numConsumerThreads; i++) {
			Thread consThread = new Thread(new HeaderDetailVelocityConsumer(sharedQueue, producer));
			consThreads.add(consThread);
		}

		// Starting producer and Consumer thread
		prodThread.start();
		
		for(Thread consThread : consThreads) {
			consThread.start();
		}		
		
		prodThread.join();
		logger.debug("Producer finished");
		for(Thread consThread : consThreads) {
			consThread.join();
			logger.debug("Consumer finished");
		}		
	}

}
