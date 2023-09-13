package atscale.biconnector.extractor;

import alation.sdk.bi.mde.models.Report;
import alation.sdk.core.stream.Stream;
import alation.sdk.core.stream.StreamException;
import atscale.api.AtScaleServerClient;
import atscale.api.SOAPQuery;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.models.Hierarchy;
import atscale.biconnector.utils.SOAPResultSet;
import atscale.biconnector.utils.Tools;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static atscale.biconnector.utils.Constants.*;

public class HierarchiesExtractor extends IMetadataExtractor {

    private static final Logger LOGGER = Logger.getLogger(HierarchiesExtractor.class);

    private final Map<String, String> cubeNameVsId;
    private Set<Hierarchy> hierarchies = new HashSet<>();
    private final Set<Report> reports = new HashSet<>();
    private boolean isIncludeFilter = false;
    private final Set<String> cubeNames = new HashSet<>();
    private Set<String> ignoreHierarchySet;
    private Map<String, Set<String>> dimToDatasetsMap;
    private Set<String> foundObjects;

    public void setDimensionNameVsId(Map<String, String> dimensionNameVsId) {
        this.dimensionNameVsId = dimensionNameVsId;
    }

    private Map<String, String> dimensionNameVsId = new HashMap<>();

    public Set<Report> getReports() {
        return reports;
    }

    public HierarchiesExtractor(
            Map<String, String> cubeNameVsId,
            Set<String> ignoreHierarchySet,
            Map<String, Set<String>> dimToDatasetsMap,
            Set<String> foundObjects) {
        this.cubeNameVsId = cubeNameVsId;
        this.ignoreHierarchySet = ignoreHierarchySet;
        this.dimToDatasetsMap = dimToDatasetsMap;
        this.foundObjects = foundObjects;
    }

    public String getQuery() {
        return isIncludeFilter
                ? String.format(HIERARCHY_SELECTIVE_QUERY, String.join(",", cubeNames))
                : HIERARCHY_QUERY;
    }

    /**
     * Creating AtScale Hierarchy Object from result set
     *
     * @param resultSet - result set from executing SQL query.
     */
    public void convertResultsetToAtScaleObjects(ResultSet resultSet) throws SQLException {
        Tools.printHeader("convertResultsetToAtScaleObjects for hierarchies", 2);
        hierarchies = new HashSet<>();
        while (resultSet.next()) {
            try {
                Hierarchy hierarchy = new Hierarchy();
                hierarchy.setRowId(resultSet.getRow());
                hierarchy.setTableNumber(resultSet.getRow());
                hierarchy.setImportDate(new Date().toString());
                hierarchy.setCatalogName(resultSet.getString("CATALOG_NAME"));
                hierarchy.setSchemaName(resultSet.getString("SCHEMA_NAME"));
                hierarchy.setCubeName(resultSet.getString("CUBE_NAME"));
                hierarchy.setDimensionUniqueName(resultSet.getString("DIMENSION_UNIQUE_NAME"));
                hierarchy.setHierarchyName(resultSet.getString("HIERARCHY_NAME"));
                hierarchy.setHierarchyUniqueName(resultSet.getString("HIERARCHY_UNIQUE_NAME"));
                hierarchy.setHierarchyGUID(resultSet.getString("HIERARCHY_GUID"));
                hierarchy.setHierarchyCaption(resultSet.getString("HIERARCHY_CAPTION"));
                hierarchy.setDimensionType(resultSet.getInt("DIMENSION_TYPE"));
                hierarchy.setHierarchyCardinality(resultSet.getInt("HIERARCHY_CARDINALITY"));
                hierarchy.setDefaultMember(resultSet.getString("DEFAULT_MEMBER"));
                hierarchy.setAllMember(resultSet.getString("ALL_MEMBER"));
                hierarchy.setDescription(resultSet.getString("DESCRIPTION"));
                hierarchy.setStructure(resultSet.getInt("STRUCTURE"));
                hierarchy.setVirtual(resultSet.getBoolean("IS_VIRTUAL"));
                hierarchy.setReadWrite(resultSet.getBoolean("IS_READWRITE"));
                hierarchy.setDimensionUniqueSettings(resultSet.getInt("DIMENSION_UNIQUE_SETTINGS"));
                hierarchy.setDimensionMasterName(resultSet.getString("DIMENSION_MASTER_UNIQUE_NAME"));
                hierarchy.setDimensionIsVisible(resultSet.getBoolean("DIMENSION_IS_VISIBLE"));
                hierarchy.setHierarchyOrigin(resultSet.getInt("HIERARCHY_ORIGIN"));
                hierarchy.setHierarchyDisplayFolder(resultSet.getString("HIERARCHY_DISPLAY_FOLDER"));
                hierarchy.setInstanceSelection(resultSet.getInt("INSTANCE_SELECTION"));
                hierarchy.setGroupingBehaviour(resultSet.getInt("GROUPING_BEHAVIOR"));
                hierarchy.setStructureType(resultSet.getString("STRUCTURE_TYPE"));
                hierarchy.setDimensionIsShared(resultSet.getBoolean("DIMENSION_IS_SHARED"));
                hierarchy.setHierarchyIsVisible(resultSet.getBoolean("HIERARCHY_IS_VISIBLE"));
                hierarchy.setHierarchyOrdinal(resultSet.getInt("HIERARCHY_ORDINAL"));

                // Confirm dim is visible/in DMV query. If not, don't include hierarchy
                if (foundObjects.contains(hierarchy.getCatalogName()
                        + "." + hierarchy.getDimensionUniqueName().replace("[", "").replace("]", ""))) {
                    hierarchies.add(hierarchy);
                } else {
                    LOGGER.warn("Not including hierarchy '" + hierarchy.getCatalogName()
                            + "." + hierarchy.getHierarchyUniqueName().replace("[", "").replace("]", "")
                            + "' because parent dimension is invisible or not returned by DMV query");
                }
            } catch (Exception e) {
                LOGGER.error("Error while creating Hierarchy for row id : " + resultSet.getRow());
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Constructing BI Report Columns from atscale hierarchy obj parent report for columns are atscale
     * dimension
     */
    @Override
    void convertAtScaleObjectToAlation() {
        Set<String> hiersNoConnection = new HashSet<>();

        hierarchies.forEach(
                hierarchy -> {
                    if (!ignoreHierarchySet.contains(hierarchy.getHierarchyUniqueName())) {
                        try {
                            String id =
                                    StringUtils.joinWith(
                                            ".",
                                            hierarchy.getCatalogName(),
                                            hierarchy.getCubeName(),
                                            hierarchy.getHierarchyUniqueName());

                            Report report =
                                    new Report(id, hierarchy.getHierarchyCaption(), "Hierarchy");
                            report.setDescription(hierarchy.getDescription());
                            report.setType(Report.Type.SIMPLE); // as opposed to DASHBOARD
                            String dimensionKey =
                                    dimensionNameVsId.get(
                                            StringUtils.joinWith(
                                                    "~~",
                                                    hierarchy.getCatalogName(),
                                                    hierarchy.getCubeName(),
                                                    hierarchy.getDimensionUniqueName()));

                            if (dimensionKey != null) {
                                report.setParentReportIds(Collections.singletonList(dimensionKey));
                            }

                            report.setDatasourceIds(getDatasetListForHierarchy(hierarchy.getCatalogName(), hierarchy.getCubeName(),
                                    hierarchy.getHierarchyUniqueName().replace("[", "").replace("]", "")));

                            reports.add(report);
                        } catch (Exception e) {
                            LOGGER.error(e.getMessage(), e);
                        }
                    }
                });
        if (!hiersNoConnection.isEmpty()) {
            ArrayList<String> ary = new ArrayList<>(hiersNoConnection);
            Collections.sort(ary);
            // There are 2 potential causes here. 1. The DMV queries for dims/hiers/levels are returning elements that are not used by the cube.
            // 2. Overlapping Role-play prefixes are not yet supported.
            LOGGER.warn("No connections found for the following <project>.<cube>.<dimension>.<hierarchy> list so datasource IDs will not be set: " + Tools.printListInLines(ary, ""));
        }
    }

    // Need to go through levels to get list of datasets for any in the current dimension
    private ArrayList<String> getDatasetListForHierarchy(String projName, String cubeName, String hierUniqueName) {
        ArrayList<String> returnList = new ArrayList<>();
        Set<String> datasets = dimToDatasetsMap.get(projName + "." + hierUniqueName);
        if (Tools.setIsEmpty(datasets)) {
            LOGGER.warn("No datasets found for hierarchy '" + hierUniqueName + "' in '" + projName + "." + cubeName + "'");
        } else {
            for (String dsName : datasets) {
                returnList.add(projName + "." + dsName);
            }
        }
        return returnList;
    }

    public void extractHierarchies(Set<String> catalogNames, AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, Stream alationStream) throws StreamException {
        LOGGER.info("Starting Hierarchy extraction process.");
        cubeNameVsId.forEach(
                (cube, id) -> this.cubeNames.add(String.format("'%s'", cube.split("~~")[1])));

        // AtScale requires that we include the catalog property so we need to loop
        // through all the catalogs
        catalogNames.forEach(
                catalogName -> {
                    try {
                        String catalogProperty = "<Catalog>" + catalogName + "</Catalog>\n";
                        extractMetadata(atScaleServerClient, configuration, alationStream, catalogProperty);
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }
        );
        if (!reports.isEmpty()) {
            LOGGER.info("Posting " + reports.size() + " hierarchy(ies) to Alation reports");
            for (Report report : reports) {
                alationStream.stream(report);
            }
        } else {
            LOGGER.info("No hierarchies found to post");
        }
        LOGGER.info("Hierarchy extraction completed.");
    }

    public void getHiers(AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration,
                         Set<String> catalogNames, Set<String> foundObjects) {
        LOGGER.debug("Retrieving all hierarhcies for lookups");

        for (String projectName : catalogNames) {
            String query = String.format(HIERARCHY_ON_CATALOG_QUERY, projectName);
            LOGGER.info("SQL Query for HIERARCHIES on catalog '" + projectName + "' = " + query);
            SOAPResultSet resultSet = SOAPQuery.runSOAPQuery(atScaleServerClient, configuration, query);
            try {
                while (resultSet.next()) {
                    updateFoundObject(foundObjects, resultSet);
                }
            } catch (Exception e) {
                LOGGER.error("Error while getting the hierarchy: ", e);
            }
        }
    }

    /**
     * @param foundObjects
     * @param resultSet
     */
    private static void updateFoundObject(Set<String> foundObjects, SOAPResultSet resultSet) {
        try {
            foundObjects.add(resultSet.getString("CATALOG_NAME")
                    + "." + resultSet.getString("DIMENSION_UNIQUE_NAME").replace("[", "").replace("]", "")
                    + "." + resultSet.getString("HIERARCHY_NAME"));

        } catch (Exception e) {
            LOGGER.error("Error while extracting the hierarchy: ", e);
        }
    }
}
