package atscale.biconnector.extractor;

import alation.sdk.core.stream.Stream;
import atscale.api.AtScaleServerClient;
import atscale.api.SOAPQuery;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.utils.SOAPResultSet;
import org.apache.log4j.Logger;

import java.net.SocketException;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class IMetadataExtractor {

    private static final Logger LOGGER = Logger.getLogger(IMetadataExtractor.class);

    abstract String getQuery();

    abstract void convertResultsetToAtScaleObjects(ResultSet resultSet) throws SQLException;

    abstract void convertAtScaleObjectToAlation();

    public void extractMetadata(AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, Stream alationStream, String extraProperties) {
        try {
            extractMetadata(atScaleServerClient, configuration, alationStream, true, extraProperties);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    public void extractMetadata(AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, Stream alationStream, boolean retry, String extraProperties)
            throws Exception {
        try {
            String query = getQuery();
            if (extraProperties == null) {
                LOGGER.info("SQL Query = " + query);
            } else {
                LOGGER.info("SQL Query (filtered for " + extraProperties.trim() + ") = " + query);
            }

            SOAPResultSet resultSet = SOAPQuery.runSOAPQuery(atScaleServerClient, configuration, query, extraProperties);
            if (resultSet == null) return;

            convertResultsetToAtScaleObjects(resultSet);
            convertAtScaleObjectToAlation();
        } catch (Exception ex) {
            LOGGER.error("Error while extracting metadata: ", ex);
            if (ex instanceof SocketException) {
                LOGGER.info("Socket Exception occurred");
                if (retry) {
                    atScaleServerClient.reconnect();
                    extractMetadata(atScaleServerClient, configuration, alationStream, false, extraProperties);
                }
            }
        }
    }
}
