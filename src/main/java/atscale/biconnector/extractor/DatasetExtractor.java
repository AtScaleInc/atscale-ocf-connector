package atscale.biconnector.extractor;

import alation.sdk.bi.mde.models.Connection;
import alation.sdk.bi.mde.models.Datasource;
import alation.sdk.core.stream.Stream;
import atscale.api.AtScaleServerClient;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.models.ConnectionDetails;
import atscale.biconnector.models.Dataset;
import atscale.biconnector.utils.Tools;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static atscale.biconnector.utils.Constants.DATASET_QUERY;
import static atscale.biconnector.utils.Constants.DATASET_SELECTIVE_QUERY;

public class DatasetExtractor extends IMetadataExtractor {
    private static final Logger LOGGER = Logger.getLogger(DatasetExtractor.class);

    private Set<Dataset> datasets = new HashSet<>();
    private final Set<Datasource> biDatasources = new HashSet<>();
    private final Set<Connection> biConnections = new HashSet<>();
    private boolean isIncludeFilter = false;
    private Map<String, ConnectionDetails> connectionDetailsMap;
    private Set<String> catalogNames;
    private Map<String, Dataset> datasetMap;

    public DatasetExtractor(Map<String, ConnectionDetails> connectionDetailsMap) {
        this.connectionDetailsMap = connectionDetailsMap;
        this.datasetMap = new HashMap<>();
    }

    public Set<Datasource> getBiDatasources() {
        return biDatasources;
    }

    public Map<String, Dataset> getDatasetMap() {
        return datasetMap;
    }

    public String getQuery() {
        return isIncludeFilter
                ? String.format(DATASET_SELECTIVE_QUERY, String.join(",", catalogNames))
                : DATASET_QUERY;
    }

    public void extractDatasets(Set<String> catalogNames, AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, Stream alationStream) {
        LOGGER.info("Starting Dataset extraction process.");

        // AtScale requires that we include the catalog property so we need to loop
        // through all the catalogs
        try {
            for (String projectName : catalogNames) {
                String extra = "<Catalog>" + projectName + "</Catalog>\n";

                extractMetadata(atScaleServerClient, configuration, alationStream, extra);
            }

            // Stream datasets and tables to Alation
            if (!biConnections.isEmpty()) {
                LOGGER.info("Posting " + biConnections.size() + " table(s) to Alation connections");
                for (Connection biConn : biConnections) {
                    alationStream.stream(biConn);
                }
            } else {
                LOGGER.info("No tables found to post");
            }
            if (!biDatasources.isEmpty()) {
                LOGGER.info("Posting " + biDatasources.size() + " dataset(s) to Alation datasources");
                for (Datasource biDS : biDatasources) {
                    alationStream.stream(biDS);
                }
            } else {
                LOGGER.info("No datasets found to post");
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

        LOGGER.info("Dataset extraction completed.");
    }

    public void convertResultsetToAtScaleObjects(ResultSet resultSet) throws SQLException {
        Tools.printHeader("convertResultsetToAtScaleObjects for datasets", 2);
        datasets = new HashSet<>();
        while (resultSet.next()) {
            try {
                Dataset dataset = new Dataset();
                dataset.setDatasetName(resultSet.getString("DATASET_NAME"));
                dataset.setCatalogName(resultSet.getString("CATALOG_NAME"));
                dataset.setCubeGUID(resultSet.getString("CUBE_GUID"));
                dataset.setDatabase(resultSet.getString("DATABASE"));
                dataset.setTable(resultSet.getString("TABLE"));
                dataset.setSchema(resultSet.getString("SCHEMA"));
                dataset.setExpression(resultSet.getString("EXPRESSION"));
                dataset.setConnection(resultSet.getString("CONNECTION_ID"));

                if(dataset.getSchema().isEmpty()) {
                    dataset.setSchema(dataset.getDatasetName());
                }
                datasets.add(dataset);
            } catch (Exception e) {
                LOGGER.error("Error while creating Dataset object for row id : " + resultSet.getRow());
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    @Override
    void convertAtScaleObjectToAlation() {
        LOGGER.info("Converting AtScale Datasets to BIDatasources");
        datasets.forEach(
                ds -> {
                    try {
                        ConnectionDetails connectionDetails = connectionDetailsMap.get(ds.getConnection());

                        if (connectionDetails != null) {
                            ArrayList<String> connectionIds = new ArrayList<>();

                            if (!ds.getSchema().isEmpty() && !ds.getTable().isEmpty()) {
                                Connection conn = createConnection(ds);
                                biConnections.add(conn);
                                connectionIds.add(conn.getId());
                            }

                            String id = StringUtils.joinWith(
                                    ".",
                                    ds.getCatalogName(),
                                    ds.getDatasetName());

                            Datasource biDatasource = new Datasource(id, ds.getDatasetName(), "Dataset");
                            if (!connectionIds.isEmpty()) {
                                biDatasource.setConnectionIds(connectionIds);
                            }
                            biDatasource.setParentFolderId(ds.getCatalogName());
                            if (biDatasource.getDescription() == null) {
                                String expression = ds.getExpression();
                                if (expression != null) {
                                    biDatasource.setDescription(expression);
                                }
                            }
                            biDatasources.add(biDatasource);

                            datasetMap.put(ds.getCatalogName() + "." + ds.getDatasetName(), ds);
                        }
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
    }

    public Connection createConnection(Dataset ds) {
        ConnectionDetails connectionDetails = connectionDetailsMap.get(ds.getConnection());

        if (connectionDetails == null) {
            LOGGER.warn("Connection not found for dataset '" + ds.getDatasetName()
                    + (Tools.isEmpty(ds.getSchema()) ? "" : "' with database.schema.table: " + ds.getDatabase() + "." + ds.getSchema() + "." + ds.getTable())
                    + " so dataset will not be added");
            return null;
        }

        String id = StringUtils.joinWith(".", ds.getSchema(), ds.getTable());
        if (!Tools.isEmpty(ds.getDatabase())) {
            id = StringUtils.joinWith(".", ds.getDatabase(), ds.getSchema(), ds.getTable());
        }

        Connection biConnection = new Connection(id, ds.getTable(), "Table");

        biConnection.setDatabaseType(connectionDetails.getType());
        biConnection.setHost(connectionDetails.getHost());
        biConnection.setPort(connectionDetails.getPort());
        biConnection.setDbSchema(ds.getSchema());
        biConnection.setDbTable(ds.getTable());
        biConnection.setDisplayConnectionType("TABLE");
        biConnection.setConnectionType(Connection.Type.TABLE);
        return biConnection;
    }
}
