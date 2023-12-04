package atscale.biconnector.extractor;

import alation.sdk.bi.mde.models.ReportColumn;
import alation.sdk.core.stream.Stream;
import alation.sdk.core.stream.StreamException;
import atscale.api.AtScaleServerClient;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.models.Dataset;
import atscale.biconnector.models.Level;
import atscale.biconnector.utils.Tools;
import atscale.biconnector.utils.Utilities;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static atscale.biconnector.utils.Constants.*;

public class LevelsExtractor extends IMetadataExtractor {

    private static final Logger LOGGER = Logger.getLogger(LevelsExtractor.class);

    private final Map<String, String> cubeNameVsId;
    private Set<Level> levels = new HashSet<>();
    private final Set<ReportColumn> reportColumns = new HashSet<>();
    private boolean isIncludeFilter = false;
    private final Set<String> cubeNames = new HashSet<>();
    private final Map<String, Dataset> datasetMap;
    private final Set<String> foundObjects;
    private Set<String> ignoreHierarchySet;
    private Map<String, Set<String>> dimToDatasetsMap;
    Map<String, Set<String>> cubeDatasetLevel =  new HashMap<>();
    public Set<ReportColumn> getReportColumns() {
        return reportColumns;
    }

    public LevelsExtractor(
            Map<String, String> cubeNameVsId,
            Map<String, Dataset> datasetMap, Set<String> foundObjects) {
        this.cubeNameVsId = cubeNameVsId;
        this.datasetMap = datasetMap;
        this.foundObjects = foundObjects;
        this.ignoreHierarchySet = new HashSet<>();
        this.dimToDatasetsMap = new HashMap<>();
    }

    public Set<String> getIgnoreHierarchySet() {
        return ignoreHierarchySet;
    }

    public Map<String, Set<String>> getDimToDatasetsMap() {
        return dimToDatasetsMap;
    }

    public Map<String, Set<String>> getCubeDatasetLevel() {
        return cubeDatasetLevel;
    }

    public String getQuery() {
        return isIncludeFilter
                ? String.format(LEVEL_SELECTIVE_QUERY, String.join(",", cubeNames))
                : LEVEL_QUERY;
    }

    /**
     * Creating AtScale Hierarchy Object from result set
     *
     * @param resultSet - result set from executing SQL query.
     */
    public void convertResultsetToAtScaleObjects(ResultSet resultSet) throws SQLException {
        Tools.printHeader("convertResultsetToAtScaleObjects for levels", 2);
        levels = new HashSet<>();
        while (resultSet.next()) {
            try {
                // Check if cube name is the cube defined in the project. If using perspectives, don't want to list all versions of the cube.
                Level level = new Level();
                level.setRowId(resultSet.getRow());
                level.setImportDate(new Date().toString());
                level.setCatalogName(resultSet.getString("CATALOG_NAME"));
                level.setSchemaName(resultSet.getString("SCHEMA_NAME"));
                level.setCubeName(resultSet.getString("CUBE_NAME"));
                level.setCubeGUID(resultSet.getString("CUBE_GUID"));
                level.setDatasetName(resultSet.getString("DATASET_NAME"));
                level.setDimensionUniqueName(resultSet.getString("DIMENSION_UNIQUE_NAME"));
                level.setHierarchyUniqueName(resultSet.getString("HIERARCHY_UNIQUE_NAME"));
                level.setLevelUniqueName(resultSet.getString("LEVEL_UNIQUE_NAME"));
                level.setLevelGUID(resultSet.getString("LEVEL_GUID"));
                if (level.getLevelGUID().contains("+")) { // To work around issue with GUID's in DMV query
                    level.setLevelGUID(level.getLevelGUID().substring(0, level.getLevelGUID().indexOf("+")));
                }
                level.setLevelCaption(resultSet.getString("LEVEL_CAPTION"));
                level.setLevelName(resultSet.getString("LEVEL_NAME"));
                level.setLevelNumber(resultSet.getInt("LEVEL_NUMBER"));
                level.setNameColumn(resultSet.getString("LEVEL_NAME_SQL_COLUMN_NAME"));
                level.setKeyColumns(resultSet.getString("LEVEL_KEY_SQL_COLUMN_NAME"));
                level.setSortColumn(resultSet.getString("LEVEL_SORT_SQL_COLUMN_NAME"));
                level.setNameDataType(resultSet.getString("LEVEL_DBTYPE_NAME_COLUMN"));
                level.setSortDataType(resultSet.getString("LEVEL_DBTYPE_SORT_COLUMN"));
                level.setParentLevelGUID(resultSet.getString("PARENT_LEVEL_ID"));
                level.setPrimary(resultSet.getBoolean("IS_PRIMARY"));
                level.setDescription(resultSet.getString("DESCRIPTION"));

                // Confirm dim and hier are both visible/in DMV query. If not, don't include
                if (foundObjects.contains(level.getCatalogName()
                        + "." + level.getDimensionUniqueName().replace("[", "").replace("]", ""))
                        && foundObjects.contains(level.getCatalogName()
                        + "." + level.getHierarchyUniqueName().replace("[", "").replace("]", ""))) {
                    levels.add(level);
                } else {
                    LOGGER.warn("Not including level '" + level.getCatalogName()
                            + "." + level.getLevelUniqueName().replace("[", "").replace("]", "")
                            + "' because a parent is invisible or not returned by DMV query");
                }
                if (StringUtils.isNotEmpty(level.getDatasetName())) {
                    Utilities.addMultiUniqueValuesToMap(cubeDatasetLevel, level.getCubeName(),level.getDatasetName());
                }
            } catch (Exception e) {
                LOGGER.error("Error while creating Level for row id : " + resultSet.getRow());
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Constructing BI Report Columns from atscale level obj parent report for columns are atscale
     * dimension
     */
    @Override
    void convertAtScaleObjectToAlation() {
        Set<String> levelsNoConnection = new HashSet<>();
        ArrayList<String> missingLevels = new ArrayList<>();

        for (Level level : levels) {
            try {
                ReportColumn reportColumn;

                String id =
                        StringUtils.joinWith(
                                ".",
                                level.getCatalogName(),
                                level.getCubeName(),
                                level.getLevelUniqueName());

                if (Tools.isEmpty(level.getParentLevelGUID())) {
                    // Level Attribute
                    reportColumn = new ReportColumn(id, level.getLevelCaption(), "Level Attribute");
                    reportColumn.setRole("Level Attribute");
                    reportColumn.setExpression("Query name: " + level.getLevelUniqueName());
                    reportColumn.setBiObjectType("Level " + level.getLevelNumber());
                    reportColumn.setDataType("Level " + level.getLevelNumber()); // Displays as type in table in UI

                    String hierarchyKey =
                            StringUtils.joinWith(
                                    ".",
                                    level.getCatalogName(),
                                    level.getCubeName(),
                                    level.getHierarchyUniqueName());
                    reportColumn.setReportId(hierarchyKey);

                } else {
                    // Secondary attribute
                    Level associatedLevel = level.getAssociatedLevel(levels, missingLevels);

                    if (associatedLevel == null) {
                        // Associated level is invisible
                        reportColumn = new ReportColumn(id, level.getLevelCaption(), SECONDARY_ATTRIBUTE);
                        reportColumn.setBiObjectType("Secondary of invisible level attribute");
                        reportColumn.setDataType("Secondary of invisible level attribute"); // Displays as type in table in UI

                        reportColumn.setReportId(StringUtils.joinWith(
                                ".",
                                level.getCatalogName(),
                                level.getCubeName(),
                                level.getHierarchyUniqueName()));
                    } else {
                        // Has associated level
                        ignoreHierarchySet.add(level.getHierarchyUniqueName()); // Don't load hierarchies for secondary attributes

                        id = StringUtils.joinWith(
                                ".",
                                level.getCatalogName(),
                                level.getCubeName(),
                                associatedLevel.getHierarchyUniqueName() + ".[" + level.getLevelName() + "]");
                        reportColumn = new ReportColumn(id, level.getLevelCaption(), "Secondary Attribute");
                        reportColumn.setBiObjectType("Secondary of " + associatedLevel.getLevelCaption());
                        reportColumn.setDataType("Secondary of " + associatedLevel.getLevelCaption() + " - Level " + associatedLevel.getLevelNumber()); // Displays as type in table in UI
                        reportColumn.setReportId(StringUtils.joinWith(
                                ".",
                                level.getCatalogName(),
                                level.getCubeName(),
                                associatedLevel.getHierarchyUniqueName()));
                    }
                    reportColumn.setRole("Secondary Attribute");
                    reportColumn.setExpression("Query name: " + level.getLevelUniqueName());
                }
                reportColumn.setDescription(level.getDescription());
                reportColumn.setValues(Collections.singletonList(level.getDescription()));

                setDatasourceColumnIds(levelsNoConnection, level, reportColumn);
                reportColumns.add(reportColumn);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        if (!levelsNoConnection.isEmpty()) {
            ArrayList<String> ary = new ArrayList<>(levelsNoConnection);
            Collections.sort(ary);
            LOGGER.warn("No connections found for the following <project>.<dataset>.<level> list. It may not be used in the cube, or overlapping Role-play prefixes are not yet supported: " + Tools.printListInLines(ary, ""));
        }
        if (!missingLevels.isEmpty()) {
            LOGGER.warn("No level attribute found associated with the following secondary attribute(s). They may be invisible: " + Tools.printList(missingLevels, ""));
        }
    }

    /**
     * @param levelsNoConnection
     * @param level
     * @param reportColumn
     */
    private void setDatasourceColumnIds(Set<String> levelsNoConnection, Level level, ReportColumn reportColumn) {
        String dsName = level.getDatasetName();
        if (datasetMap.get(level.getCatalogName() + "." + dsName) == null) {
            dsName = updateDSName(level.getCatalogName(), level.getDatasetName());
            if (dsName.equals("")) {
                levelsNoConnection.add(level.getCatalogName() + "." + level.getDatasetName() + "." + level.getLevelCaption());
            } else {
                LOGGER.info("Level '" + level.getLevelUniqueName() + "' may be in multiple datasets as part of a shared degenerate dimension using ID '" + level.getDatasetName() + "'. Only dataset '" + dsName + "' will be used for metadata extraction");
            }
        } else if (!Tools.isEmpty(datasetMap.get(level.getCatalogName() + "." + dsName).getTable())) {
            reportColumn.setDatasourceColumnIds(level.getSourceColumnIDs(datasetMap.get(level.getCatalogName() + "." + dsName)));
        }
    }

    // In the case of shared degenerate dimensions, DMV queries return a list of dataset names like:
    // "qds_for_c93dcc85_22e7_46ea_86b,Gldetail,GLACCOUNTBALANCE". For now only 1 dataset will be used in this case
    public String updateDSName(String catalogName, String dsName) {
        for (String oneDS : dsName.split(",")) {
            if (datasetMap.get(catalogName + "." + oneDS) != null) {
                return oneDS;
            }
        }
        return "";
    }

    public void extractLevels(Set<String> catalogNames, AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, Stream alationStream) throws StreamException {
        LOGGER.info("Starting Level extraction process.");
        cubeNameVsId.forEach(
                (cube, id) -> this.cubeNames.add(String.format("'%s'", cube.split("~~")[1])));

        // AtScale requires that we include the catalog property, so we need to loop
        // through all the catalogs
        catalogNames.forEach(
                catalogName -> {
                    try {
                        String catalogProperty = "<Catalog>" + catalogName + "</Catalog>\n";
                        extractMetadata(atScaleServerClient, configuration, alationStream, catalogProperty);
                        populateDimToDatasetsMap();
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }
        );

        if (!reportColumns.isEmpty()) {
            LOGGER.info("Posting " + reportColumns.size() + " level(s) to Alation report columns");
            for (ReportColumn reportColumnrt : reportColumns) {
                alationStream.stream(reportColumnrt);
            }
        } else {
            LOGGER.info("No levels found to post");
        }
        LOGGER.info("Level extraction completed.");
    }

    public void populateDimToDatasetsMap() {
        Set<String> errorSet = new HashSet<>();
        for (Level level : levels) {
            // Add keys for dimensions
            addIdToMapWithList(level.getCatalogName() + "." + level.getDimensionUniqueName().replace("[", "").replace("]", ""), level.getDatasetName(), errorSet);

            // Add keys for hierarchies
            addIdToMapWithList(level.getCatalogName() + "." + level.getHierarchyUniqueName().replace("[", "").replace("]", ""), level.getDatasetName(), errorSet);
        }
        if (!errorSet.isEmpty()) {
            LOGGER.warn("Dataset(s) with following values are not well formed so will not be joined: " + Tools.printSetWithSingleQuotes(errorSet, ""));
        }
    }

    private void addIdToMapWithList(String key, String valToAdd, Set<String> errorSet) {
        if (valToAdd.contains(",")) {
            errorSet.add(valToAdd);
            return;
        }
        Set<String> temp;
        if (dimToDatasetsMap.containsKey(key)) {
            temp = dimToDatasetsMap.get(key);
        } else {
            temp = new HashSet<>();
        }
        temp.add(valToAdd);
        dimToDatasetsMap.put(key, temp);
    }
}
