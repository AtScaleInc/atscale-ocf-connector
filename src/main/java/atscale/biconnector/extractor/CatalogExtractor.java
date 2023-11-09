package atscale.biconnector.extractor;

import alation.sdk.bi.mde.models.Folder;
import alation.sdk.core.stream.Stream;
import alation.sdk.core.stream.StreamException;
import atscale.api.AtScaleAPI;
import atscale.api.AtScaleServerClient;
import atscale.api.SOAPQuery;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.exception.InternalProcessException;
import atscale.biconnector.exception.ObjectNotFoundException;
import atscale.biconnector.models.Catalog;
import atscale.biconnector.utils.SOAPResultSet;
import atscale.biconnector.utils.Tools;
import atscale.biconnector.utils.Utilities;
import org.apache.log4j.Logger;
import org.glassfish.pfl.basic.contain.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static atscale.biconnector.utils.Constants.*;

public class CatalogExtractor extends IMetadataExtractor {

    private static final Logger LOGGER = Logger.getLogger(CatalogExtractor.class);

    private final AtScaleBIConfiguration configuration;
    private Set<Catalog> catalogs = new HashSet<>();
    private final Set<Folder> folders = new HashSet<>();
    private final Set<String> catalogNames = new HashSet<>();
    private boolean isIncludeFilter = false;
    private final Map<String, Pair<String, String>> catalogNameVsId = new HashMap<>();

    public CatalogExtractor(AtScaleBIConfiguration configuration) {
        this.configuration = configuration;
    }

    public Map<String, Pair<String, String>> getCatalogNameVsId() {
        return catalogNameVsId;
    }

    public String getQuery() {
        return isIncludeFilter
                ? String.format(CATALOG_SELECTIVE_QUERY, String.join(",", catalogNames))
                : CATALOG_QUERY;
    }

    /**
     * Creating AtScale Catalog Object from result set
     *
     * @param resultSet - result set from executing SQL query.
     */
    public void convertResultsetToAtScaleObjects(ResultSet resultSet) throws SQLException {
        Tools.printHeader("convertResultsetToAtScaleObjects for catalogs", 2);
        catalogs = new HashSet<>();
        while (resultSet.next()) {
            try {
                Catalog catalog = new Catalog();
                catalog.setRowId(resultSet.getRow());
                catalog.setTableNumber(resultSet.getRow());
                catalog.setName(resultSet.getString("CATALOG_NAME"));
                catalog.setRole(resultSet.getString("ROLES"));
                catalog.setImportDate(new Date().toString());
                catalog.setLastModified(resultSet.getString("DATE_MODIFIED"));
                catalog.setCompatibilityLevel(resultSet.getInt("COMPATIBILITY_LEVEL"));
                catalog.setType(resultSet.getInt("TYPE"));
                catalog.setVersion(resultSet.getInt("VERSION"));
                catalog.setDatabaseId(resultSet.getString("DATABASE_ID"));
                catalog.setDateQueried(resultSet.getString("DATE_QUERIED"));
                catalog.setCurrentlyUsed(resultSet.getBoolean("CURRENTLY_USED"));
                catalog.setCatalogGUID(resultSet.getString("CATALOG_GUID"));
                catalogs.add(catalog);
            } catch (Exception e) {
                LOGGER.error("Error while creating Catalog object for row id : " + resultSet.getRow());
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Constructing BI Folders from catalog obj
     */
    @Override
    protected void convertAtScaleObjectToAlation() {

        Tools.printHeader("convertAtScaleObjectToAlation - catalog", 1);
        LOGGER.info("Converting Catalogs to Folders");
        catalogs.forEach(
                catalogObj -> {
                    try {
                        String id = catalogObj.getName();
                        Folder folder = new Folder(id, catalogObj.getName(), "Project");
                        folder.setLastUpdated(catalogObj.getLastModified());

                        // Filter catalog if needed
                        if (Utilities.inProjectListForTesting(catalogObj.getName())) {
                            AtScaleServerClient atScaleServerClient = new AtScaleServerClient(configuration);
                            folder.setSourceUrl(atScaleServerClient.buildAPIURL("/org/{orgId}/project/{projectId}", catalogObj.getCatalogGUID(), "", "Design Center"));
                        }


                        folder.setIsExtractable(true);
                        String mdxConnectionString = String.format("%s://%s:%s/xmla/%s", configuration.getProtocol(), configuration.getAPIHost(), configuration.getAPIPort(), configuration.getOrganization());
                        folder.setDescription(Tools.coalesce(catalogObj.getDescription(),
                                "AtScale project imported " + catalogObj.getImportDate() + " and last modified " + catalogObj.getLastModified() + "\nMDX Connection: "+ mdxConnectionString));

                        folders.add(folder);
                        catalogNameVsId.put(catalogObj.getName(), new Pair(id, catalogObj.getCatalogGUID()));

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }

    /**
     * Extracting AtScale Catalogs from SQL Database
     *
     * @param catalogNames        - catalog names to be extracted.
     * @param alationStream       - alationStream to send the data
     * @param atScaleServerClient - serverClient for establishing database connection
     */
    public void extractCatalogs(
            Set<String> catalogNames, Stream alationStream, AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration) throws StreamException {
        LOGGER.info("Starting catalog extraction process.");
        catalogNames.forEach(catalog -> this.catalogNames.add(String.format("'%s'", catalog)));
        extractMetadata(atScaleServerClient, configuration, alationStream, null);

        if (!folders.isEmpty()) {
            if (!catalogNames.isEmpty()) {
                int count = 0;
                for (Folder folder : folders) {
                    // Only stream ones in catalogNames
                    if (catalogNames.contains(folder.getName())) {
                        count++;
                        alationStream.stream(folder);
                    }
                }
                LOGGER.info("Posting " + count + " project(s) to Alation folders");
            } else {
                // No filtering
                for (Folder folder : folders) {
                    alationStream.stream(folder);
                }
                LOGGER.info("Posting " + folders.size() + " project(s) to Alation folders");
            }
        } else {
            LOGGER.info("No projects to post");
        }

        LOGGER.info("Catalog Extraction Completed.");
    }

    /**
     * Extracting Catalog Names for extraction
     *
     * @param excludeCatalogNames - catalog names to be excluded for extraction/ "null" for full
     *                            extraction
     * @param atScaleServerClient - serverClient for establishing database connection
     * @param fullExtraction      - true for full extraction
     */
    public Set<String> extractCatalogNameToBeIncluded(
            Set<String> excludeCatalogNames, AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, boolean fullExtraction) {
        Set<String> catalogNamesIncluded = new HashSet<>();
        String query = CATALOG_QUERY;
        LOGGER.info(SQL_QUERY + query);
        SOAPResultSet resultSet = SOAPQuery.runSOAPQuery(atScaleServerClient, configuration, query);
        try {
            if (resultSet == null) throw (new ObjectNotFoundException("No published projects found to include"));
            if (fullExtraction) {
                LOGGER.info("Consolidating projects that have been published multiple times");
                while (resultSet.next()) {
                    // Need to add filtering out of projects published twice with different names
                    // Only want 1 instance for now. Just have the unique name with which project was published
                    List<String> publishedProjectList = AtScaleAPI.getSinglePublishedProjectNames(atScaleServerClient, configuration);
                    if (publishedProjectList.contains(resultSet.getString(CATALOG_NAME)) && Utilities.inProjectListForTesting(resultSet.getString(CATALOG_NAME))) {
                        catalogNamesIncluded.add(resultSet.getString(CATALOG_NAME));
                    }
                }
                LOGGER.info("Populated list of " + catalogNamesIncluded.size() + " published and deduplicated project(s)");
            } else {
                while (resultSet.next()) {
                    if (!excludeCatalogNames.contains(resultSet.getString(CATALOG_NAME))) {
                        catalogNamesIncluded.add(resultSet.getString(CATALOG_NAME));
                    }
                }
            }
        } catch (Exception ex) {
            throw new InternalProcessException(ex);
        }
        return catalogNamesIncluded;
    }

}
