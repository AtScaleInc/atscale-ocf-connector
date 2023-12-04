package atscale.biconnector.extractor;

import alation.sdk.bi.mde.models.ReportColumn;
import alation.sdk.core.stream.Stream;
import alation.sdk.core.stream.StreamException;
import atscale.api.AtScaleServerClient;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.models.Column;
import atscale.biconnector.models.Dataset;
import atscale.biconnector.models.Measure;
import atscale.biconnector.utils.MeasureUtils;
import atscale.biconnector.utils.Tools;
import atscale.biconnector.utils.Utilities;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static atscale.biconnector.utils.Constants.MEASURE_QUERY;
import static atscale.biconnector.utils.Constants.MEASURE_SELECTIVE_QUERY;

public class MeasureExtractor extends IMetadataExtractor {

    private static final Logger LOGGER = Logger.getLogger(MeasureExtractor.class);

    private Set<Measure> measures = new HashSet<>();
    private Set<Measure> allMeasures = new HashSet<>();
    private final Set<ReportColumn> reportColumns = new HashSet<>();
    private boolean isIncludeFilter = false;
    private final Set<String> filterCubeNames = new HashSet<>();
    private Map<Integer, String> dataTypeVsNameMap;
    private Map<String, String> cubeNameVsId = new HashMap<>();
    private Map<String, Dataset> datasetMap;
    private Map<String, Column> columnMap;
    Map<String, Set<String>> cubeDatasetMeasure = new HashMap<>();

    public MeasureExtractor(
            Map<Integer, String> dataTypeVsNameMap, Map<String, Dataset> datasetMap,
            Map<String, Column> columnMap) {
        this.dataTypeVsNameMap = dataTypeVsNameMap;
        this.datasetMap = datasetMap;
        this.columnMap = columnMap;
    }

    public void setCubeNameVsId(Map<String, String> cubeNameVsId) {
        this.cubeNameVsId = cubeNameVsId;
    }

    public Map<String, Set<String>> getCubeDatasetMeasure() {
        return cubeDatasetMeasure;
    }

    public String getQuery() {
        return isIncludeFilter
                ? String.format(MEASURE_SELECTIVE_QUERY, String.join(",", filterCubeNames))
                : MEASURE_QUERY;
    }

    /**
     * Creating AtScale Measure Object from result set
     *
     * @param resultSet - result set from executing SQL query.
     */
    public void convertResultsetToAtScaleObjects(ResultSet resultSet) throws SQLException {
        Tools.printHeader("convertResultsetToAtScaleObjects for measures", 2);
        measures = new HashSet<>();
        while (resultSet.next()) {
            try {
                // Check if cube name is the cube defined in the project. If using perspectives, don't want to list all versions of the cube.
                Measure measure = new Measure();
                measure.setRowId(resultSet.getRow());
                measure.setTableNumber(resultSet.getRow());
                measure.setImportDate(new Date().toString());
                measure.setCatalogName(resultSet.getString("CATALOG_NAME"));
                measure.setSchemaName(resultSet.getString("SCHEMA_NAME"));
                measure.setCubeName(resultSet.getString("CUBE_NAME"));
                measure.setCubeGUID(resultSet.getString("CUBE_GUID"));
                measure.setMeasureName(resultSet.getString("MEASURE_NAME"));
                measure.setMeasureUniqueName(resultSet.getString("MEASURE_UNIQUE_NAME"));
                measure.setMeasureGUID(resultSet.getString("MEASURE_GUID"));
                measure.setMeasureCaption(resultSet.getString("MEASURE_CAPTION"));
                measure.setMeasureAggregator(resultSet.getInt("MEASURE_AGGREGATOR"));
                measure.setNumericPrecision(resultSet.getInt("NUMERIC_PRECISION"));
                measure.setNumericScale(resultSet.getInt("NUMERIC_SCALE"));
                measure.setMeasureUnits(resultSet.getInt("MEASURE_UNITS"));
                measure.setDescription(resultSet.getString("DESCRIPTION"));
                measure.setExpression(resultSet.getString("EXPRESSION"));
                measure.setVisible(resultSet.getBoolean("MEASURE_IS_VISIBLE"));
                measure.setLevelList(resultSet.getString("MEASURE_IS_VISIBLE"));
                measure.setMeasureNameSQLColumnName(resultSet.getString("MEASURE_NAME_SQL_COLUMN_NAME"));
                measure.setMeasureUnqualifiedCaption(resultSet.getString("MEASURE_UNQUALIFIED_CAPTION"));
                measure.setMeasureGroupName(resultSet.getString("MEASUREGROUP_NAME"));
                measure.setMeasureDisplayFolder(resultSet.getString("MEASURE_DISPLAY_FOLDER"));
                measure.setDefaultFormatString(resultSet.getString("DEFAULT_FORMAT_STRING"));
                measure.setDatasetName(resultSet.getString("DATASET_NAME"));
                measure.setDataset(datasetMap.get(measure.getCatalogName() + "." + measure.getDatasetName()));
                measure.setDataType(dataTypeVsNameMap.get(resultSet.getInt("DATA_TYPE")));
                measure.setColumnDataType(dataTypeVsNameMap.get(resultSet.getInt("COLUMN_DATA_TYPE")));
                measure.setColumnName(resultSet.getString("COLUMN_NAME"));

                measure.setMetricalAttribute(resultSet.getBoolean("IS_METRICAL_ATTRIBUTE"));
                measure.setParentLevelId(resultSet.getString("PARENT_LEVEL_ID"));
                measure.setParentLevelName(resultSet.getString("PARENT_LEVEL_NAME"));

                // Dataset will be null for calculated measures
                if (!Tools.isEmpty(measure.getExpression()) && measure.getDataset() != null) {
                    Column column = columnMap.get(StringUtils.joinWith("/", measure.getDataset().getDatabase(),
                            measure.getDataset().getSchema(), measure.getDataset().getTable(), measure.getColumnName()));
                    if (column == null) {
                        LOGGER.warn("Can't find column from DMV query for '" + measure.getDatasetName() + "." + measure.getColumnName() + "'");
                    }
                    if (column != null) {
                        measure.setColumnSQL(column.getExpression());
                    }
                }
                if (StringUtils.isNotEmpty(measure.getDatasetName())) {
                    Utilities.addMultiUniqueValuesToMap(cubeDatasetMeasure, measure.getCubeName(), measure.getDatasetName());
                }
                measures.add(measure);
            } catch (Exception e) {
                LOGGER.error("Error while creating Measure for row id : " + resultSet.getRow());
                LOGGER.error(e.getMessage(), e);
            }
        }
        allMeasures.addAll(measures);
    }

    /**
     * Constructing BI Report Columns from atscale measure obj parent report for columns are atscale measure
     * groups
     */
    @Override
    void convertAtScaleObjectToAlation() {
        measures.forEach(
                measure1 -> {
                    try {
                        String id =
                                StringUtils.joinWith(
                                        ".",
                                        measure1.getCatalogName(),
                                        measure1.getCubeName(),
                                        measure1.getMeasureName());

                        ReportColumn reportColumn =
                                new ReportColumn(id, measure1.getMeasureCaption(), "Measure");

                        reportColumn.setDescription(measure1.getDescription());
                        reportColumn.setDataType(measure1.getDataType());

                        if (measure1.getExpression().length() > 0) {
                            reportColumn.setExpression(measure1.getMeasureUniqueName() + " = " + measure1.getExpression());
                            reportColumn.setRole("Calculated Measure");

                            // Alation doesn't use the list of referenced DatasourceColumnIds, so not pulling it from DMV yet
                        } else {
                            // If QDS, don't populate datasource column IDs
                            if (measure1.getDataset() != null && !Tools.isEmpty(measure1.getDataset().getTable())) {
                                reportColumn.setDatasourceColumnIds(measure1.getSourceColumnIDs(measure1.getDataset()));
                            }
                            if (measure1.isMetricalAttribute()) {
                                LOGGER.info("Metrical attribute '" + measure1.getMeasureCaption() + "' found in cube '" + measure1.getCubeName() + "'. Will be listed as a dimension attribute");
                                reportColumn.setRole("Dimension");
                                reportColumn.setBiObjectType("Metrical Attribute");
                            } else {
                                reportColumn.setRole("Measure");
                                reportColumn.setExpression(getMeasureExpression(measure1));
                            }
                        }

                        reportColumn.setReportId(
                                StringUtils.joinWith(
                                        ".",
                                        measure1.getCatalogName(),
                                        measure1.getCubeName(),
                                        measure1.getCubeName()
                                ));

                        reportColumns.add(reportColumn);
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
    }

    /**
     * format the expression for measure.
     * @param measure1
     * @return
     */
    private static String getMeasureExpression(Measure measure1) {
        String expression = String.format("%s: %s, %s: %s", "Aggregation", MeasureUtils.getMeasureAggName(measure1.getMeasureAggregator()),  "Query name", measure1.getMeasureUniqueName());
        if(StringUtils.isNotEmpty(measure1.getDefaultFormatString())){
            expression = String.format("%s, %s: %s", expression, "Format", MeasureUtils.getFormatWithName(measure1.getDefaultFormatString()));
        }
        return expression;
    }

    public void extractMeasures(Set<String> catalogNames, AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, Stream alationStream) throws StreamException {
        LOGGER.info("Starting Measure extraction process.");
        cubeNameVsId.forEach(
                (cube, id) -> this.filterCubeNames.add(String.format("'%s'", cube.split("~~")[1])));

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
        if (!reportColumns.isEmpty()) {
            LOGGER.info("Posting " + reportColumns.size() + " measure(s) to Alation report columns");
            for (ReportColumn reportColumnrt : reportColumns) {
                alationStream.stream(reportColumnrt);
            }
        } else {
            LOGGER.info("No measures found to post");
        }
        LOGGER.info("Measure extraction completed.");
    }

    public Map<String, Set<String>> populateMgToDatasetsMap() {
        Map<String, Set<String>> mgToDatasetsMap = new HashMap<>();
        Set<String> errorSet = new HashSet<>();

        for (Measure meas : allMeasures) {
            addIdToMapWithList(mgToDatasetsMap, meas.getCatalogName() + "." + meas.getCubeName(), meas.getDatasetName(), errorSet);
        }
        if (!errorSet.isEmpty()) {
            LOGGER.warn("Dataset(s) for measure group(s) with following values are not well formed so will not be joined: " + Tools.printSetWithSingleQuotes(errorSet, ""));
        }
        return mgToDatasetsMap;
    }

    private void addIdToMapWithList(Map<String, Set<String>> map, String key, String valToAdd, Set<String> errorSet) {
        if (valToAdd.equals("")) {
            return;
        }
        if (valToAdd.contains(",")) {
            errorSet.add(valToAdd);
            return;
        }
        if (map.containsKey(key)) {
            Set<String> temp = map.get(key);
            temp.add(valToAdd);
            map.put(key, temp);
        } else {
            Set<String> temp = new HashSet<>();
            temp.add(valToAdd);
            map.put(key, temp);
        }
    }
}
