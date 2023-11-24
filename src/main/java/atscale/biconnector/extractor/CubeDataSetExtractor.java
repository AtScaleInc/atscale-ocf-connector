package atscale.biconnector.extractor;

import alation.sdk.bi.mde.models.Connection;
import alation.sdk.bi.mde.models.Datasource;
import alation.sdk.core.stream.Stream;
import atscale.biconnector.models.ConnectionDetails;
import atscale.biconnector.models.Dataset;
import atscale.biconnector.utils.Tools;
import atscale.biconnector.utils.Utilities;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author tanmay.gargav
 */
public class CubeDataSetExtractor {
    private static final Logger LOGGER = Logger.getLogger(CubeDataSetExtractor.class);
    private Map<String, ConnectionDetails> connectionDetailsMap;
    private final Map<String, Dataset> datasetMap;
    private final Map<String, Set<String>> catalogVsCubeName;
    Map<String, Set<String>> cubeDatasetLevelMeasure;
    Stream alationStream = null;

    /**
     * @param connectionDetailsMap
     * @param datasetMap
     * @param cubeDatasetLevelMeasure
     * @param catalogVsCubeName
     */
    public CubeDataSetExtractor(Map<String, ConnectionDetails> connectionDetailsMap, Map<String, Dataset> datasetMap, Map<String, Set<String>> cubeDatasetLevelMeasure, Map<String, Set<String>> catalogVsCubeName) {
        this.connectionDetailsMap = connectionDetailsMap;
        this.datasetMap = datasetMap;
        this.cubeDatasetLevelMeasure = cubeDatasetLevelMeasure;
        this.catalogVsCubeName = catalogVsCubeName;
    }

    /**
     * @param alationStream
     * @param catalogNames
     */
    public void createCubeDateSets(Stream alationStream, Set<String> catalogNames) {
        LOGGER.info("Starting extraction process for cube level datasets.");
        this.alationStream = alationStream;

        for (String projectName : catalogNames) {
            processProject(projectName, catalogVsCubeName);
        }

        LOGGER.info("Cube level datasets extraction completed.");
    }

    /**
     * @param projectName
     * @param projectVsCubeMap
     */
    private void processProject(String projectName, Map<String, Set<String>> projectVsCubeMap) {
        try {
            for (String cubeName : projectVsCubeMap.get(projectName)) {
                processCube(projectName, cubeName);
            }
        } catch (Exception e) {
            LOGGER.error("Error while extracting cube level datasets for project");
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * @param projectName
     * @param cubeName
     */
    private void processCube(String projectName, String cubeName) {
        if (cubeDatasetLevelMeasure.containsKey(cubeName)) {
            for (String cubeDatasetName : cubeDatasetLevelMeasure.get(cubeName)) {
                processCubeDataset(projectName, cubeName, cubeDatasetName);
            }
        }
    }

    /**
     * @param projectName
     * @param cubeName
     * @param cubeDatasetName
     */
    private void processCubeDataset(String projectName, String cubeName, String cubeDatasetName) {
        try {
            Dataset ds = datasetMap.get(projectName + "." + cubeDatasetName);
            ConnectionDetails connectionDetails = connectionDetailsMap.get(ds.getConnection());

            if (connectionDetails != null) {
                ArrayList<String> connectionIds = new ArrayList<>();

                if (!ds.getSchema().isEmpty() && !ds.getTable().isEmpty()) {
                    Connection conn = createConnection(ds);
                    connectionIds.add(conn.getId());
                }

                String id = StringUtils.joinWith(".", projectName, cubeName, cubeDatasetName);
                Datasource biDatasource = new Datasource(id, cubeDatasetName, "Dataset");
                if (!connectionIds.isEmpty()) {
                    biDatasource.setConnectionIds(connectionIds);
                }
                biDatasource.setParentFolderId(projectName + "." + cubeName);
                alationStream.stream(biDatasource);
            }
        } catch (Exception e) {
            LOGGER.error("Error while processing cube dataset");
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * @param ds
     * @return
     */
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
