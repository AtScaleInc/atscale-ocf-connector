package atscale.biconnector.extractor;

import alation.sdk.bi.mde.models.Folder;
import alation.sdk.core.stream.Stream;
import alation.sdk.core.stream.StreamException;
import atscale.api.AtScaleServerClient;
import atscale.api.SOAPQuery;
import atscale.biconnector.models.Cube;
import atscale.biconnector.utils.SOAPResultSet;
import atscale.biconnector.utils.Tools;
import atscale.biconnector.configuration.AtScaleBIConfiguration;

import static atscale.biconnector.utils.Constants.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.glassfish.pfl.basic.contain.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class CubeExtractor extends IMetadataExtractor {

    private static final Logger LOGGER = Logger.getLogger(CubeExtractor.class);
    private static final String FILTERED_FOR_CATALOG = "(filtered for catalog: ";

    private final AtScaleBIConfiguration configuration;
    private final Map<String, String> cubeNameVsId = new HashMap<>();
    private Set<Cube> cubes = new HashSet<>();
    private Set<Folder> folders = new HashSet<>();
    private Set<String> cubeNames = new HashSet<>();
    private boolean isIncludeFilter = false;
    private Map<String, Pair<String, String>> catalogNameVsId;

    public CubeExtractor(AtScaleBIConfiguration configuration, Map<String, Pair<String, String>> catalogNameVsId) {
        this.configuration = configuration;
        this.catalogNameVsId = catalogNameVsId;
    }

    public Map<String, String> getCubeNameVsId() {
        return cubeNameVsId;
    }

    public String getQuery() {
        return isIncludeFilter
                ? String.format(CUBE_SELECTIVE_QUERY, String.join(",", cubeNames))
                : LISTED_CUBE_QUERY;
    }

    /**
     * Creating AtScale Cube Object from result set
     *
     * @param resultSet - result set from executing SQL query.
     */
    public void convertResultsetToAtScaleObjects(ResultSet resultSet) throws SQLException {
        Tools.printHeader("convertResultsetToAtScaleObjects for cubes", 2);
        cubes = new HashSet<>();
        while (resultSet.next()) {
            try {
                Cube cube = new Cube();
                cube.setRowId(resultSet.getRow());
                cube.setTableNumber(resultSet.getRow());
                cube.setImportDate(new Date().toString());
                cube.setCatalogName(resultSet.getString(CATALOG_NAME));
                cube.setSchemaName(resultSet.getString("SCHEMA_NAME"));
                cube.setCubeName(resultSet.getString(CUBE_NAME));
                cube.setCubeType(resultSet.getString("CUBE_TYPE"));
                cube.setGuid(resultSet.getString("CUBE_GUID"));
                cube.setCreatedOn(resultSet.getString("CREATED_ON"));
                cube.setLastSchemaUpdate(resultSet.getString("LAST_SCHEMA_UPDATE"));
                cube.setLastDataUpdated(resultSet.getString("LAST_DATA_UPDATE"));
                cube.setDataUpdatedBy(resultSet.getString("DATA_UPDATED_BY"));
                cube.setDescription(resultSet.getString("DESCRIPTION"));
                cube.setCubeCaption(resultSet.getString("CUBE_CAPTION"));
                cube.setBaseCubeName(resultSet.getString("BASE_CUBE_NAME"));
                cube.setCubeSource(resultSet.getInt("CUBE_SOURCE"));
                cubes.add(cube);
            } catch (Exception e) {
                LOGGER.error("Error while creating Cube for row id : " + resultSet.getRow());
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Constructing BI Folders from cube obj
     */
    @Override
    void convertAtScaleObjectToAlation() {
        LOGGER.info("Converting Cubes to Folder");

        cubes.forEach(
                cube -> {
                    try {
                        String id = StringUtils.joinWith(".", cube.getCatalogName(), cube.getCubeName());
                        Folder folder = new Folder(id, cube.getCubeCaption(), "Cube");
                        folder.setLastUpdated(cube.getLastDataUpdated());

                        if (catalogNameVsId.containsKey(cube.getCatalogName())) {
                            AtScaleServerClient atScaleServerClient = new AtScaleServerClient(configuration);
                            folder.setSourceUrl(atScaleServerClient.buildAPIURL(
                                    "/org/{orgId}/project/{projectId}/cube/{cubeId}",
                                    catalogNameVsId.get(cube.getCatalogName()).second(),
                                    cube.getGuid(),
                                    "Design Center"));
                        }

                        folder.setIsExtractable(true);
                        String parentFolderId = catalogNameVsId.get(cube.getCatalogName()).first();
                        if (parentFolderId != null) {
                            folder.setParentFolderId(parentFolderId);
                        }
                        folder.setDescription(Tools.coalesce(cube.getDescription(),
                                "AtScale cube on host: " + configuration.getDCHost()));

                        folders.add(folder);
                        cubeNameVsId.put(cube.getCatalogName() + "~~" + cube.getCubeName(), id);
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
    }

    /**
     * Extracting Cube Names for extraction
     *
     * @param excludeCubeNames    - cube names to be excluded for extraction/ "null" for full extraction
     * @param atScaleServerClient - serverClient for establishing database connection
     * @param fullExtraction      - true for full extraction
     */
    public Set<String> extractCubeNameToBeIncluded(
            Set<String> catalogNames, Set<String> excludeCubeNames, AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, boolean fullExtraction) {
        Set<String> cubeNamesIncluded = new HashSet<>();
        String query = LISTED_CUBE_QUERY;

        // For Cubes, AtScale requires that we include the catalog property so we need to loop
        // through all the catalogs
        catalogNames.forEach(
                catalogName -> {
                    try {
                        String catalogProperty = CATALOG_HTML_START + catalogName + CATALOG_HTML_END_WITH_NEW_LINE;
                        LOGGER.info(SQL_QUERY + FILTERED_FOR_CATALOG + catalogName + ") " + query);
                        SOAPResultSet resultSet = SOAPQuery.runSOAPQuery(atScaleServerClient, configuration, query, catalogProperty);
                        if (fullExtraction) {
                            while (resultSet.next()) {
                                cubeNamesIncluded.add(resultSet.getString(CUBE_NAME));
                            }
                        } else {
                            while (resultSet.next()) {
                                if (!excludeCubeNames.contains(resultSet.getString(CUBE_NAME))) {
                                    cubeNamesIncluded.add(resultSet.getString(CUBE_NAME));
                                }
                            }
                        }
                    } catch (Exception ex) {
                        LOGGER.error("Error while extracting catalog names for extraction.");
                        LOGGER.error(ex.getMessage(), ex);
                    }
                }
        );
        return cubeNamesIncluded;
    }

    /**
     * Extracting Catalog Names from cubes for extraction
     *
     * @param atScaleServerClient - serverClient for establishing database connection
     * @param cubeNames           - cube names for which catalog needs to be extracted
     */
    public Set<String> extractCatalogNameFromCubes(AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, Set<String> cubeNames) {
        // Doesn't look like we hit this
        Set<String> catalogNames = new HashSet<>();
        cubeNames.forEach(
                cubeName -> {
                    try {
                        String query = String.format(CUBE_NAME_SELECTIVE_QUERY, cubeName);

                        // For Cubes, AtScale requires that we include the catalog property, so we need to loop
                        // through all the catalogs
                        String queryCatalog = CATALOG_QUERY;
                        LOGGER.info(SQL_QUERY + queryCatalog);
                        SOAPResultSet resultSetCatalog = SOAPQuery.runSOAPQuery(atScaleServerClient, configuration, queryCatalog);

                        while (resultSetCatalog.next()) {
                            String catalogProperty = CATALOG_HTML_START + resultSetCatalog.getString(CATALOG_NAME) + CATALOG_HTML_END_WITH_NEW_LINE;
                            LOGGER.info(SQL_QUERY + FILTERED_FOR_CATALOG + resultSetCatalog.getString(CATALOG_NAME) + ") " + query);
                            SOAPResultSet resultSet = SOAPQuery.runSOAPQuery(atScaleServerClient, configuration, query, catalogProperty);

                            if (resultSet.next()) {
                                catalogNames.add(resultSet.getString(CATALOG_NAME));
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
        return catalogNames;
    }

    /**
     * Extracting Cube Names from catalog for extraction
     *
     * @param atScaleServerClient - serverClient for establishing database connection
     * @param catalogNames        - catalog names from which cube needs to be extracted
     */
    public Set<String> extractCubeNamesFromCatalog(Set<String> catalogNames, AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration) {
        Set<String> cubeNamesfromCatalog = new HashSet<>();
        catalogNames.forEach(
                catalogName -> {
                    try {
                        String query = String.format(CUBE_NAME_FROM_CATALOG_SELECTIVE_QUERY, catalogName);

                        // For Cubes, AtScale requires that we include the catalog property
                        String catalogProperty = CATALOG_HTML_START + catalogName + CATALOG_HTML_END_WITH_NEW_LINE;
                        LOGGER.info(SQL_QUERY + FILTERED_FOR_CATALOG + catalogName + ") " + query);
                        SOAPResultSet resultSet = SOAPQuery.runSOAPQuery(atScaleServerClient, configuration, query, catalogProperty);

                        while (resultSet.next()) {
                            cubeNamesfromCatalog.add(resultSet.getString(CUBE_NAME));
                        }
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
        return cubeNamesfromCatalog;
    }

    /**
     * Extracting AtScale Cubes from SQL Database
     *
     * @param cubeNames           - cube names to be extracted.
     * @param alationStream       - alationStream to send the data
     * @param atScaleServerClient - serverClient for establishing database connection
     */
    public void extractCubes(Set<String> catalogNames, Set<String> cubeNames, AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, Stream alationStream) throws StreamException {
        LOGGER.info("Starting cube extraction process.");
        cubeNames.forEach(cubeName -> this.cubeNames.add(String.format("'%s'", cubeName)));

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
        if (!folders.isEmpty()) {
            LOGGER.info("Posting " + folders.size() + " cube(s) to Alation folders");
            for (Folder folder : folders) {
                alationStream.stream(folder);
            }
        } else {
            LOGGER.info("No cubes to post");
        }

        LOGGER.info("Cube extraction completed.");
    }
}
