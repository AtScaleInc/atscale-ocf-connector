package atscale.biconnector.extractor;

import alation.sdk.bi.mde.models.DatasourceColumn;
import alation.sdk.core.stream.Stream;
import atscale.api.AtScaleServerClient;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.models.Column;
import atscale.biconnector.models.Dataset;
import atscale.biconnector.models.Dependency;
import atscale.biconnector.models.ConnectionDetails;
import atscale.biconnector.utils.Tools;

import static atscale.biconnector.utils.Constants.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class ColumnExtractor extends IMetadataExtractor {
    private final Set<DatasourceColumn> datasourceColumns = new HashSet<>();
    private boolean isIncludeFilter = false;
    private Map<String, ConnectionDetails> connectionDetailsMap;
    private Set<Column> columns = new HashSet<>();
    private Map<String, Dataset> datasetMap;
    private Map<Integer, String> dataTypeVsNameMap;
    private Set<String> catalogNames;
    private List<Dependency> dependencies;

    private static final Logger LOGGER = Logger.getLogger(ColumnExtractor.class);

    public ColumnExtractor(Map<String, ConnectionDetails> connectionDetailsMap, Map<String, Dataset> datasetMap,
                           Map<Integer, String> dataTypeVsNameMap, List<Dependency> dependencies) {
        this.connectionDetailsMap = connectionDetailsMap;
        this.datasetMap = datasetMap;
        this.dataTypeVsNameMap = dataTypeVsNameMap;
        this.dependencies = dependencies;
        LOGGER.debug(String.format("connectionDetailsMap: %s", this.connectionDetailsMap));
        LOGGER.debug(String.format("dependencies: %s", this.dependencies));
    }

    public String getQuery() {
        return isIncludeFilter
                ? String.format(COLUMN_SELECTIVE_QUERY, String.join(",", catalogNames))
                : COLUMN_QUERY;
    }

    public void extractColumns(Set<String> catalogNames, AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, Stream alationStream) {
        LOGGER.info("Starting Column extraction process.");

        // AtScale requires that we include the catalog property, so we need to loop
        // through all the catalogs
        try {
            for (String projectName : catalogNames) {
                String extra = "<Catalog>" + projectName + "</Catalog>\n";

                extractMetadata(atScaleServerClient, configuration, alationStream, extra);
            }
            if (!datasourceColumns.isEmpty()) {
                LOGGER.info("Posting " + datasourceColumns.size() + " physical column(s) to Alation datasource columns");
                for (DatasourceColumn dsCol : datasourceColumns) {
                    alationStream.stream(dsCol);
                }
            } else {
                LOGGER.info("No physical columns found to post");
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

        LOGGER.info("Column extraction completed.");
    }

    public void convertResultsetToAtScaleObjects(ResultSet resultSet) throws SQLException {
        Tools.printHeader("convertResultsetToAtScaleObjects for datasets", 2);
        columns = new HashSet<>();
        while (resultSet.next()) {
            try {
                Column column = new Column();
                column.setCatalogName(resultSet.getString("CATALOG_NAME"));
                column.setDatasetName(resultSet.getString("DATASET_NAME"));
                column.setColumnName(resultSet.getString("COLUMN_NAME"));
                column.setDataType(dataTypeVsNameMap.get(resultSet.getInt("DATA_TYPE")));
                column.setExpression(resultSet.getString("EXPRESSION"));
                column.setConnectionID(resultSet.getString("CONNECTION_ID"));
                column.setDataset(datasetMap.get(column.getCatalogName() + "." + column.getDatasetName()));
                columns.add(column);
            } catch (Exception e) {
                LOGGER.error("Error while creating Column object for row id : " + resultSet.getRow());
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    void convertAtScaleObjectToAlation() {
        LOGGER.info("Converting AtScale physical columns to DatasourceColumns");
        Set<String> missingDatasets = new HashSet<>();

        for (Column col : columns) {
            Dataset dataset = datasetMap.get(col.getCatalogName() + "." + col.getDatasetName());
            if (dataset == null) {
                missingDatasets.add(col.getCatalogName() + "." + col.getDatasetName());
                continue;
            }
            try {
                String schema = Tools.isEmpty(dataset.getSchema()) ? "" : dataset.getSchema() + ".";
                String table = Tools.isEmpty(dataset.getTable()) ? "" : dataset.getTable() + ".";
                String database = Tools.isEmpty(dataset.getDatabase()) ? "" : dataset.getDatabase() + ".";

                String dsColID = database + schema + table + col.getColumnName();
                String dsColName = database + schema + table + col.getColumnName();

                DatasourceColumn dsCol = new DatasourceColumn(dsColID, dsColName, "Physical Column");
                dsCol.setDataType(col.getDataType());
                dsCol.setDatasourceId(dataset.getCatalogName() + "." + dataset.getDatasetName());
                dsCol.setExpression(col.getExpression());
                this.datasourceColumns.add(dsCol);
            } catch (Exception e) {
                LOGGER.error("Error while converting the atscale obj to alation: ", e);
            }
        }

        if (!missingDatasets.isEmpty()) {
            List<String> missingDatasetsList = new ArrayList<>(missingDatasets);
            missingDatasetsList.sort(String::compareTo);
            LOGGER.warn(String.format("Can't find the following <project>.<dataset> in datasetMap so not including contained columns in DatasourceColumns:%n%s ", String.join("\n", missingDatasetsList)));
        }
    }


    // Key for regular columns is database/schema/table/column or schema/table/column if database not available
    // For calculated columns it's project/dataset/column
    public Map<String, Column> populateColumnMap() {
        Map<String, Column> columnMap = new HashMap<>();
        for (Column col : columns) {
            if (Tools.isEmpty(col.getExpression())) {
                columnMap.put(StringUtils.joinWith("/", col.getCatalogName(), col.getDatasetName(), col.getColumnName()), col);
            } else { // Populate with format to use for 2021.1 release and format for future releases that have the column name instead of expression
                addToMapFirstEltOptional(columnMap, "/", getDatabase(col), StringUtils.joinWith("/",
                        getSchema(col), getTable(col), col.getColumnName()), col);
                addToMapFirstEltOptional(columnMap, "/", getDatabase(col), StringUtils.joinWith("/",
                        col.getConnectionID(), getSchema(col), getTable(col), col.getColumnName()), col);
            }
        }
        return columnMap;
    }

    private static String getTable(Column col) {
        return col.getDataset() != null ? col.getDataset().getTable() : null;
    }

    private static String getSchema(Column col) {
        return col.getDataset() != null ? col.getDataset().getSchema() : null;
    }

    private static String getDatabase(Column col) {
        return col.getDataset() != null ? col.getDataset().getDatabase() : null;
    }

    public static void addToMapFirstEltOptional(Map<String, Column> mapIn, String delimit, String first, String remaining, Column col) {
        if (first == null || first.isEmpty()) {
            mapIn.put(remaining, col);
        } else {
            mapIn.put(first + delimit + remaining, col);
        }
    }

}
