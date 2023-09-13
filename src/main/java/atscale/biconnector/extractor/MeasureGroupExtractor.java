package atscale.biconnector.extractor;

import alation.sdk.bi.mde.models.Report;
import alation.sdk.core.stream.ConversionException;
import alation.sdk.core.stream.Stream;
import alation.sdk.core.stream.StreamException;
import atscale.api.AtScaleServerClient;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.models.MeasureGroup;
import atscale.biconnector.utils.Tools;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static atscale.biconnector.utils.Constants.*;

public class MeasureGroupExtractor extends IMetadataExtractor {

    private static final Logger LOGGER = Logger.getLogger(MeasureGroupExtractor.class);
    private Set<MeasureGroup> measureGroups = new HashSet<>();
    private final Set<Report> biDashboards = new HashSet<>();
    private boolean isIncludeFilter = false;
    private final Set<String> cubeNames = new HashSet<>();
    private final Map<String, String> cubeNameVsId;
    private final Map<String, String> measureGroupVsId = new HashMap<>();
    private Map<String, Set<String>> mgToDatasetsMap;

    public MeasureGroupExtractor(
            Map<String, String> cubeNameVsId,
            Map<String, Set<String>> mgToDatasetsMap) {
        this.cubeNameVsId = cubeNameVsId;
        this.mgToDatasetsMap = mgToDatasetsMap;
    }

    @Override
    public String getQuery() {
        return isIncludeFilter
                ? String.format(MEASUREGROUP_SELECTIVE_QUERY, String.join(",", cubeNames))
                : MEASUREGROUP_QUERY;
    }

    /**
     * Creating AtScale MeasureGroup Object from result set
     *
     * @param resultSet - result set from executing SQL query.
     */
    @Override
    public void convertResultsetToAtScaleObjects(ResultSet resultSet) throws SQLException {
        Tools.printHeader("convertResultsetToAtScaleObjects for measure groups", 2);
        measureGroups = new HashSet<>();
        while (resultSet.next()) {
            try {
                MeasureGroup measureGroup = new MeasureGroup();
                measureGroup.setRowId(resultSet.getRow());
                measureGroup.setTableNumber(resultSet.getRow());
                measureGroup.setImportDate(new Date().toString());
                measureGroup.setCatalogName(resultSet.getString(CATALOG_NAME));
                measureGroup.setSchemaName(resultSet.getString("SCHEMA_NAME"));
                measureGroup.setCubeName(resultSet.getString("CUBE_NAME"));
                measureGroup.setMeasureGroupName(resultSet.getString("MEASUREGROUP_NAME"));
                measureGroup.setMeasureGroupCaption(resultSet.getString("MEASUREGROUP_CAPTION"));
                measureGroup.setDescription(resultSet.getString("DESCRIPTION"));
                measureGroup.setWriteEnabled(resultSet.getBoolean("IS_WRITE_ENABLED"));
                measureGroup.setSourceDBServerName(resultSet.getString(CATALOG_NAME));
                measureGroup.setSourceDBInstanceName(resultSet.getString(CATALOG_NAME));
                measureGroup.setSourceDBID(resultSet.getString(CATALOG_NAME).hashCode());
                measureGroups.add(measureGroup);
            } catch (Exception e) {
                LOGGER.error("Error while creating MeasureGroup for row id : " + resultSet.getRow());
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Constructing BI Report from atscale Measure group obj
     */
    @Override
    void convertAtScaleObjectToAlation() {
        LOGGER.info("Converting AtScale MeasureGroups to BIDashboards");
        measureGroups.forEach(
                measureGroup -> {
                    try {
                        boolean nameAvailable = true;
                        String measureGroupName = measureGroup.getMeasureGroupName();
                        if (measureGroupName == null) {
                            nameAvailable = false;
                            measureGroupName = measureGroup.getMeasureGroupCaption();
                            if (measureGroupName == null) {
                                LOGGER.warn(
                                        "Measure Group name is empty/null for measure group with row id = "
                                                + measureGroup.getRowId());
                                throw new ConversionException("Measure group name can not be empty/null");
                            }
                            if (measureGroupName.contains(measureGroup.getSourceDBInstanceName())) {
                                measureGroupName = measureGroupName.split(" ")[1];
                            }
                        }
                        String id =
                                StringUtils.joinWith(
                                        ".",
                                        measureGroup.getCatalogName(),
                                        measureGroup.getCubeName(),
                                        measureGroupName);

                        Report report = new Report(id, measureGroupName, "MeasureGroup");
                        report.setDescription(measureGroup.getDescription());
                        report.setType(Report.Type.DASHBOARD);
                        String parentFolderId =
                                cubeNameVsId.get(measureGroup.getCatalogName() + "~~" + measureGroup.getCubeName());
                        if (parentFolderId != null) {
                            report.setParentFolderId(parentFolderId);
                        }

                        report.setDatasourceIds(getDatasetListForMG(measureGroup.getCatalogName(), measureGroup.getCubeName()));

                        biDashboards.add(report);
                        if (nameAvailable) {
                            measureGroupVsId.put(
                                    measureGroup.getCatalogName()
                                            + measureGroup.getCubeName()
                                            + measureGroup.getMeasureGroupName(),
                                    id);
                        } else {
                            measureGroupVsId.put(
                                    measureGroup.getCatalogName()
                                            + measureGroup.getCubeName()
                                            + measureGroupName,
                                    id);
                        }
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
    }

    private ArrayList<String> getDatasetListForMG(String projName, String mgName) {
        ArrayList<String> returnList = new ArrayList<>();
        Set<String> datasets = mgToDatasetsMap.get(projName + "." + mgName);
        if (Tools.setIsEmpty(datasets)) {
            LOGGER.warn("No datasets found for measure group '" + mgName + "' in '" + projName + "'");
            return returnList;
        }
        for (String dsName : datasets) {
            returnList.add(projName + "." + dsName);
        }
        return returnList;
    }

    public void extractMeasureGroups(Set<String> catalogNames, AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, Stream alationStream) throws StreamException {
        LOGGER.info("Starting MeasureGroup extraction process.");

        cubeNameVsId.forEach(
                (cube, id) -> this.cubeNames.add(String.format("'%s'", cube.split("~~")[1])));

        // AtScale requires that we include the catalog property, so we need to loop
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
        if (!biDashboards.isEmpty()) {
            LOGGER.info("Posting " + biDashboards.size() + " measure group(s) to Alation dashboards");
            for (Report report : biDashboards) {
                alationStream.stream(report);
            }
        } else {
            LOGGER.info("No measure groups found to post");
        }
        LOGGER.info("MeasureGroup extraction completed.");
    }
}
