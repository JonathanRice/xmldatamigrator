package org.jonathanrice.xmldatamigrator.producer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.jonathanrice.xmldatamigrator.util.DBConnectionFactory;
import org.jonathanrice.xmldatamigrator.util.PropertyUtil;

/**
 * This is the producer thread.  It will execute the producer SQL and pass each value on to the consumers via the shared queue
 * @author jrice
 *
 */
public class HeaderSqlProducer implements Runnable {

	private static Logger logger = Logger.getLogger(HeaderSqlProducer.class);
	private final BlockingQueue<String> sharedQueue;
	private boolean producerActive;

	public HeaderSqlProducer(BlockingQueue<String> sharedQueue) {
		this.sharedQueue = sharedQueue;
		this.producerActive = true;
	}

	@Override
	public void run() {
		Connection conn = DBConnectionFactory.getDBConnection();

		ResultSet rset = null;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();

			String producerSql = PropertyUtil.getProperties().getProperty(
					"producer.sql");			
			rset = stmt.executeQuery(producerSql);

			while (rset.next()) {
				String headerKey = rset.getString(1);
				logger.debug("Producer placing " + headerKey + " on the processing queue");
				sharedQueue.put(headerKey);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (rset != null) {
				try {
					rset.close();
				} catch (SQLException e) {
					logger.warn("Unable to close producer result set safely", e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.warn("Unable to close producer statment safely", e);
				}
			}
			try {
				conn.close();
			} catch (SQLException e) {
				logger.warn("Unable to close producer connection safely", e);
			}
		}
		producerActive = false;
	}
	/**
	 * After run is complete is will set producerActive to false which will inform the consumer tostop 
	 * @return
	 */
	public boolean isProducerActive() {
		return producerActive;
	}
}
