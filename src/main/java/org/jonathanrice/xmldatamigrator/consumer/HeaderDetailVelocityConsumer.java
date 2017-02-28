package org.jonathanrice.xmldatamigrator.consumer;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.jonathanrice.xmldatamigrator.producer.HeaderSqlProducer;
import org.jonathanrice.xmldatamigrator.transmit.Transmitter;
import org.jonathanrice.xmldatamigrator.util.DBConnectionFactory;
import org.jonathanrice.xmldatamigrator.util.PropertyUtil;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
/**
 * This is the consumer thread.  It will read values that are produced.  It will fire off the parameterized header SQL
 *   then fire off the parameterized detail SQL using the parameter supplied by the producer thread.  The values returned
 *   by the header and detail SQLs are placed in HashMap and passed into a velocity template.  
 * @author jrice
 *
 */
public class HeaderDetailVelocityConsumer implements Runnable {

	private static Logger logger = Logger.getLogger(HeaderDetailVelocityConsumer.class);
	private String headerSql;
	private String detailSql;
	private String updateSql;
	private VelocityEngine velocityEngine;
	private Template velocityTemplate;

	private Connection conn = null;
	private final BlockingQueue<String> sharedQueue;
	private final HeaderSqlProducer producer;

	public HeaderDetailVelocityConsumer(BlockingQueue<String> sharedQueue,
			HeaderSqlProducer producer) {
		this.sharedQueue = sharedQueue;
		this.producer = producer;
		getProperties();
	}

	protected void getProperties() {
		headerSql = PropertyUtil.getProperties().getProperty("header.sql");
		detailSql = PropertyUtil.getProperties().getProperty("detail.sql");
		updateSql = PropertyUtil.getProperties().getProperty("update.sql");
		String templateName = PropertyUtil.getProperties().getProperty(
				"velocity.template.name");
		
		Properties velocityAdditionalProperties = new Properties();
		velocityAdditionalProperties.put("runtime.log.logsystem.class", "org.apache.velocity.runtime.log.NullLogSystem");
		velocityEngine = new VelocityEngine();
		try {
			velocityEngine.init(velocityAdditionalProperties);
			velocityTemplate = velocityEngine.getTemplate(templateName);
		} catch (Exception e) {
			logger.error(e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void run() {
		while (producer.isProducerActive() || !sharedQueue.isEmpty()) {
			String queueItem = "";
			try {
				queueItem = sharedQueue.poll(10, TimeUnit.MICROSECONDS);
				if (queueItem == null) {
					continue;
				}
				process(queueItem);
			} catch (Exception e) {
				logger.error("Error with: " + queueItem + e.getMessage(), e);
			}
		}
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			logger.error("Unable to close connection: " + e);
		}
	}

	protected void process(String headerKey) throws ResourceNotFoundException,
			ParseErrorException, Exception {
		logger.debug("Consumer reading " + headerKey + " off the processing queue");
		String xmlMessage = generateXMLFromHeaderKey(headerKey);
		
		Transmitter transmitter = new Transmitter();
		boolean success = transmitter.transmitXMLMessage(headerKey, xmlMessage);
		if (success) {
			updateRecord(headerKey);
		}
	}


	private void updateRecord(String headerKey) {
		if (updateSql == null || updateSql.equals("")) {
			//do not bother running the update if the update doesn't exist
			return;
		}
		try {
			if (conn == null || !conn.isValid(20)) {
				conn = DBConnectionFactory.getDBConnection();
			}
		} catch (SQLException e) {
			logger.error("Unable to get connection. " + headerKey + " has failed", e);
			return;
		}
		if (conn == null) {
			logger.error("Unable to get connection. " + headerKey + " has failed");
		}
		PreparedStatement updateStmt = null;
		
		try {
			updateStmt = conn.prepareStatement(updateSql);
			updateStmt.setString(1, headerKey);
			updateStmt.executeUpdate();
			conn.commit();
		} catch (SQLException e) {
			logger.error("Unable to run update. " + headerKey + " has failed", e);
		}
	}

	@SuppressWarnings("unchecked")
	protected String generateXMLFromHeaderKey(String headerKey)
			throws ResourceNotFoundException, ParseErrorException, Exception {
		if (conn == null || !conn.isValid(20)) {
			logger.debug("New connection for " + headerKey + " requested");
			conn = DBConnectionFactory.getDBConnection();
		}
		if (conn == null) {
			logger.error("Unable to get connection. " + headerKey + " has failed");
		}
		PreparedStatement headerStmt = null;
		ResultSet headerResult = null;
		String xml = "";
		try {
			headerStmt = conn.prepareStatement(headerSql);
			headerStmt.setString(1, headerKey);
			headerResult = headerStmt.executeQuery();
			HashMap headerRow = null;
			if (headerResult.next()) {
				headerRow = DBConnectionFactory.resultSetRowToHashMap(headerResult);
				generateSubObjectData(conn, headerKey, "details", headerRow);
			}
			xml = getXMLFromVelocity(headerRow);		
		} catch (Exception e) {
			logger.error("Exception while processing: " + headerKey, e);
		} finally {
			if (headerResult != null) {
				try {
					headerResult.close();
				} catch (SQLException e) {
					logger.warn("Unable to close consumer result set safely", e);
				}					
			}
			if (headerStmt != null) {
				try {
					headerStmt.close();
				} catch (SQLException e) {
					logger.warn("Unable to close consumer statement safely", e);
				}				
			}
			if (conn != null) {
				try {
					conn.commit();
					conn.close();
					logger.debug("Closing connection");
					//conn = null;
				} catch (SQLException e) {
					logger.warn("Unable to commit consumer connection safely", e);
				}
			}
		}
		return xml;
	}

	@SuppressWarnings("unchecked")
	private void generateSubObjectData(Connection conn, String headerKey, String subObjectName, HashMap headerRow) throws SQLException {
		if (detailSql == null || detailSql.equals("")) {
			return;
		}
		PreparedStatement detailStmt = null;
		ResultSet detailResult = null;
		try {
			detailStmt = conn.prepareStatement(detailSql);
			detailStmt.setString(1, headerKey);
			detailResult = detailStmt.executeQuery();
			List detailList = DBConnectionFactory.resultSetToArrayList(detailResult);
			headerRow.put(subObjectName, detailList);
		} catch (Exception e) {
			logger.error("Exception while processing: " + headerKey, e);
		} finally {			
			if (detailResult != null) {
				try {
					detailResult.close();
				} catch (SQLException e) {
					logger.warn("Unable to close consumer result set safely", e);
				}
			}
			if (detailStmt != null) {
				try {
					detailStmt.close();
				} catch (SQLException e) {
					logger.warn("Unable to close consumer statement safely", e);
				}					
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected String getXMLFromVelocity(HashMap header)
			throws ResourceNotFoundException, ParseErrorException, Exception {

		// Create a new velocity context for each XML
		VelocityContext context = new VelocityContext();
		context.put("header", header);
		StringWriter writer = new StringWriter();

		velocityTemplate.merge(context, writer);
		return writer.toString();
	}
}
