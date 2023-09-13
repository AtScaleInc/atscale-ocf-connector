package atscale.biconnector.extractor;

import alation.sdk.bi.mde.models.Report;
import alation.sdk.core.stream.Stream;
import atscale.api.AtScaleServerClient;
import atscale.api.SOAPQuery;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.models.Dimension;
import atscale.biconnector.utils.Constants;
import atscale.biconnector.utils.SOAPResultSet;
import atscale.biconnector.utils.Tools;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static atscale.biconnector.utils.Constants.*;

public class DimensionExtractor extends IMetadataExtractor {

    private static final Logger LOGGER = Logger.getLogger(DimensionExtractor.class);
    private boolean isIncludeFilter = false;
    private Set<Dimension> dimensionSet = new HashSet<>();
    private final Set<Report> biDashboards = new HashSet<>();
    private final Map<String, String> cubeNameVsId;
    private final Set<String> cubeNames = new HashSet<>();
    private final Map<String, String> dimensionNameVsId = new HashMap<>();
    private Map<String, Set<String>> dimToDatasetsMap;

    public DimensionExtractor(
            Map<String, String> cubeNameVsId,
            Map<String, Set<String>> dimToDatasetsMap) {
        this.cubeNameVsId = cubeNameVsId;
        this.dimToDatasetsMap = dimToDatasetsMap;
    }

    public Map<String, String> getDimensionNameVsId() {
        return dimensionNameVsId;
    }

    public void extractDimensions(AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, Stream alationStream, Set<String> catalogNames) {
        LOGGER.info("Starting dimension extraction process.");

        cubeNameVsId.forEach(
                (cube, id) -> this.cubeNames.add(String.format("'%s'", cube.split("~~")[1])));

        // AtScale requires that we include the catalog property, so we need to loop
        // through all the catalogs
        try {
            for (String projectName : catalogNames) {
                this.cubeNames.clear();
                for (Map.Entry<String, String> entry : cubeNameVsId.entrySet()) {
                    if (entry.getValue().startsWith(projectName + ".")) {
                        this.cubeNames.add(String.format("'%s'", entry.getKey().split("~~")[1]));
                    }
                }
                String extra = "<Catalog>" + projectName + "</Catalog>\n";

                extractMetadata(atScaleServerClient, configuration, alationStream, extra);
            }
            if (!biDashboards.isEmpty()) {
                LOGGER.info("Posting " + biDashboards.size() + " dimension(s) to Alation dashboards");
                for (Report biReport : biDashboards) {
                    alationStream.stream(biReport);
                }
            } else {
                LOGGER.info("No dimensions found to post");
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

        LOGGER.info("Dimension extraction completed.");
    }

    public String getQuery() {
        return isIncludeFilter
                ? String.format(DIMENSION_SELECTIVE_QUERY, String.join(",", cubeNames))
                : DIMENSION_QUERY;
    }

    /**
     * Creating AtScale Dimension Object from result set
     *
     * @param resultSet - result set from executing SQL query.
     */
    public void convertResultsetToAtScaleObjects(ResultSet resultSet) throws SQLException {
        Tools.printHeader("convertResultsetToAtScaleObjects for dimensions", 2);
        dimensionSet = new HashSet<>();
        while (resultSet.next()) {
            try {
                Dimension dimension = new Dimension();
                dimension.setRowId(resultSet.getRow());
                dimension.setTableNumber(resultSet.getRow());
                dimension.setImportDate(new Date().toString());
                dimension.setCatalogName(resultSet.getString(CATALOG_NAME));
                dimension.setSchemaName(resultSet.getString("SCHEMA_NAME"));
                dimension.setCubeName(resultSet.getString("CUBE_NAME"));
                dimension.setCubeGUID(resultSet.getString("CUBE_GUID"));
                dimension.setDimensionName(resultSet.getString("DIMENSION_NAME"));
                dimension.setDimensionUniqueName(resultSet.getString("DIMENSION_UNIQUE_NAME"));
                dimension.setDimensionGUID(resultSet.getString("DIMENSION_GUID"));
                dimension.setDimensionCaption(resultSet.getString("DIMENSION_CAPTION"));
                dimension.setDimensionOrdinal(resultSet.getInt("DIMENSION_ORDINAL"));
                dimension.setType(resultSet.getInt("DIMENSION_TYPE"));
                dimension.setDimensionCardinality(resultSet.getInt("DIMENSION_CARDINALITY"));
                dimension.setDefaultHierarchy(resultSet.getString("DEFAULT_HIERARCHY"));
                dimension.setDescription(resultSet.getString("DESCRIPTION"));
                dimension.setVirtual(resultSet.getBoolean("IS_VIRTUAL"));
                dimension.setReadWrite(resultSet.getBoolean("IS_READWRITE"));
                dimension.setDimensionUniqueSettings(resultSet.getInt("DIMENSION_UNIQUE_SETTINGS"));
                dimension.setDimensionMasterName(resultSet.getString("DIMENSION_MASTER_NAME"));
                dimension.setVisible(resultSet.getBoolean("DIMENSION_IS_VISIBLE"));
                // OVERRODE THESE VALUES WITH CATALOG
                dimension.setSourceDBServerName(resultSet.getString(CATALOG_NAME));
                dimension.setSourceDBInstanceName(resultSet.getString(CATALOG_NAME));
                dimension.setSourceDBID(resultSet.getString(CATALOG_NAME).hashCode());
                dimensionSet.add(dimension);
            } catch (Exception e) {
                LOGGER.error("Error while creating Dimension object for row id : " + resultSet.getRow());
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Constructing BI Report from atscale dimension obj
     */
    @Override
    void convertAtScaleObjectToAlation() {
        LOGGER.info("Converting AtScale Dimensions to BIDashboards");
        dimensionSet.forEach(
                dimension -> {
                    try {
                        String id =
                                StringUtils.joinWith(
                                        ".",
                                        dimension.getCatalogName(),
                                        dimension.getCubeName(),
                                        dimension.getDimensionUniqueName());
                        String dimensionName = dimension.getDimensionName();
                        if (dimensionName == null) {
                            dimensionName = getTableName(dimension);
                        }
                        Report report = new Report(id, dimensionName, "Dimension");
                        report.setDescription(dimension.getDescription());
                        report.setType(Report.Type.DASHBOARD);
                        report.setDatasourceIds(getDatasetListForDim(dimension.getCatalogName(), dimension.getCubeName(), dimension.getDimensionName()));

                        String parentFolderId = StringUtils.joinWith(".", dimension.getCatalogName(), dimension.getCubeName());
                        report.setParentFolderId(parentFolderId);

                        biDashboards.add(report);
                        dimensionNameVsId.put(
                                StringUtils.joinWith(
                                        "~~",
                                        dimension.getCatalogName(),
                                        dimension.getCubeName(),
                                        dimension.getDimensionUniqueName()),
                                report.getId());
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
    }

    // Need to go through levels to get list of datasets for any in the current dimension
    private ArrayList<String> getDatasetListForDim(String projName, String cubeName, String dimName) {
        ArrayList<String> returnList = new ArrayList<>();
        Set<String> datasets = dimToDatasetsMap.get(projName + "." + dimName);
        if (Tools.setIsEmpty(datasets)) {
            LOGGER.warn("No datasets found for dimension '" + dimName + "' in '" + projName + "." + cubeName + "'");
            return returnList;
        }
        for (String dsName : datasets) {
            returnList.add(projName + "." + dsName);
        }
        return returnList;
    }

    /**
     * Constructing connection object from dimension as connection information is given inside atscale
     * dimension
     *
     * @param dimension
     */

    public String getTableName(Dimension dimension) {
        String name =
                dimension
                        .getDimensionUniqueName()
                        .substring(1, dimension.getDimensionUniqueName().length() - 1);
        String schemaName = dimension.getSourceDBInstanceName();
        if (name.startsWith(schemaName)) {
            name = name.replace(schemaName, "");
        }
        return name;
    }

    public void getDims(AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration,
                        Set<String> catalogNames, Set<String> foundObjects) {
        LOGGER.debug("Retrieving all dimensions for lookups");

        for (String projectName : catalogNames) {
            String query = String.format(Constants.DIMENSION_ON_CATALOG_QUERY, projectName);
            LOGGER.info("SQL Query for DIMENSIONS on catalog '" + projectName + "' = " + query);
            SOAPResultSet resultSet = SOAPQuery.runSOAPQuery(atScaleServerClient, configuration, query);
            try {
                while (resultSet.next()) {
                    updateFoundObject(foundObjects, resultSet);
                }
            } catch (Exception e) {
                LOGGER.error("Error while getting the dimension: ", e);
            }
        }
    }

    /**
     * @param foundObjects
     * @param resultSet
     */
    public static void updateFoundObject(Set<String> foundObjects, SOAPResultSet resultSet) {
        try {
            foundObjects.add(resultSet.getString(CATALOG_NAME)
                    + "." + resultSet.getString("DIMENSION_NAME"));

        } catch (Exception e) {
            LOGGER.error("Error while extracting the dimension: ", e);
        }
    }
}
