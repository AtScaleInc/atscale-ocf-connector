package atscale.biconnector.datasource;

import alation.sdk.bi.configuration.BIApplicationConfiguration;
import alation.sdk.bi.datasource.BIDatasource;
import alation.sdk.bi.grpc.mde.AlationUser;
import alation.sdk.bi.mde.datasource.Certification;
import alation.sdk.bi.mde.datasource.MetadataExtraction;
import alation.sdk.bi.mde.models.*;
import alation.sdk.bi.mde.streams.MetadataMessage;
import alation.sdk.core.error.ConnectorException;
import alation.sdk.core.request.auth.Auth;
import alation.sdk.core.stream.Stream;
import atscale.api.AtScaleAPI;
import atscale.api.AtScaleServerClient;
import atscale.biconnector.configuration.AtScaleBIAppConfig;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.exception.ObjectNotFoundException;
import atscale.biconnector.models.Column;
import atscale.biconnector.models.ConnectionDetails;
import atscale.biconnector.models.Dataset;
import atscale.biconnector.models.Dependency;
import atscale.biconnector.extractor.*;
import atscale.biconnector.utils.Constants;
import atscale.biconnector.utils.ModelUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static atscale.biconnector.utils.Constants.*;

/**
 * AtScaleBIDatasource is a data source which simulates streaming sample BI data objects. This is
 * currently used for testing purposes and acts as an entry point of a template for developing BI
 * connectors. The scope of data that this streams could be enhanced based on more end-to-end
 * testing scenarios.
 */
public class AtScaleBIDatasource
        implements BIDatasource<AtScaleBIConfiguration>,
        MetadataExtraction<AtScaleBIConfiguration>,
        Certification<AtScaleBIConfiguration> {

    private static final Logger LOGGER = Logger.getLogger(AtScaleBIDatasource.class);
    private final Map<Integer, String> dataTypeVsNameMap = new HashMap<>();
    private static final String VERSION = "1.1.0";
    private static final String FOLDER_ID_1 = "folder_id_1";
    private static final String TEST_FOLDER_1 = "Test Folder 1";

    /**
     * Returns the source name to include in the Manifest file.
     *
     * @return The name (e.g. Sample) of the datasource supported by this connector.
     */
    @Override
    public String getSourceName() {
        return "AtScale OCF Source v" + VERSION;
    }

    /**
     * Returns connector name to include in the Manifest file.
     *
     * @return The connector's user friendly name to be displayed in UI.
     */
    @Override
    public String getConnectorName() {
        return "AtScale OCF Connector v" + VERSION;
    }

    /**
     * Returns connector version to include in the Manifest file.
     *
     * @return The connector's version string.
     */
    @Override
    public String getConnectorVersion() {
        return VERSION;
    }

    /**
     * Returns connector description to include in the Manifest file.
     *
     * @return A short summary (e.g. supported functionality) about the connector.
     */
    @Override
    public String getDescription() {
        return "This AtScale BI connector supports Metadata Extraction.";
    }

    /**
     * Returns connector configuration to include in the Manifest file.
     *
     * @return The connector configuration to include in the Manifest file.
     */
    @Override
    public AtScaleBIConfiguration getUserConfiguration() {
        return createConfiguration();
    }

    /**
     * Creates a new configuration object initialized with the default values.
     *
     * @return The connector configuration {@link AtScaleBIConfiguration} initialized with the default
     * values.
     */
    @Override
    public AtScaleBIConfiguration createConfiguration() {
        return new AtScaleBIConfiguration();
    }

    /**
     * Validates if the provided request configuration can be successfully used to execute all
     * connector supported features.
     *
     * <p>This method will be executed before each operation like Listing File System, Full Metadata
     * Extraction and Certification Propagation.
     *
     * @param configuration configuration object to be validated
     * @throws ConnectorException when the configuration request object is invalid or where there is a
     *                            type mismatch between expected and actual values.
     */
    @Override
    public void validate(AtScaleBIConfiguration configuration) throws ConnectorException {
        configuration.validate();
    }

    /**
     * Validates if the provided request configuration can be successfully used to execute all
     * connector supported features.
     *
     * @param configuration configuration object to be validated
     * @param auth          Authentication object to be used for checking connection validity
     * @throws ConnectorException when the configuration request object is invalid or where there is a
     *                            type mismatch between expected and actual values.
     */
    @Override
    public void validate(AtScaleBIConfiguration configuration, Auth auth) throws ConnectorException {
        configuration.validate();
    }

    /**
     * Returns application configuration to include in the Manifest file.
     *
     * @return The application configuration {@link AtScaleBIAppConfig} used to tune Alation.
     */
    @Override
    public BIApplicationConfiguration getApplicationConfiguration() {
        return new AtScaleBIAppConfig();
    }

    /**
     * List file system organization of the BI tool.
     *
     * @param configuration contains 1) user overridable connector configuration and 2) Alation wide
     *                      configuration that could affect the extraction output (e.g. the max data size of an
     *                      object).
     * @param stream        is the handle to the channel that can be used to send the data back to the
     *                      caller.
     * @param alationUsers  is the list of Alation users. The intention is to limit extraction to
     *                      metadata accessible to existing Alation users.
     * @throws ConnectorException when metadata extraction faces an unrecoverable error. The exception
     *                            should contain the cause of the error. If possible it should also have the stacktrace and a
     *                            hint on how to resolve it. The RPC caller will receive the exception's message as returned
     *                            by {@link alation.sdk.core.error.ConnectorException#getMessage()}.
     */
    @Override
    public void listFileSystem(
            AtScaleBIConfiguration configuration,
            Stream<MetadataMessage> stream,
            List<AlationUser> alationUsers)
            throws ConnectorException {
        Folder folder1 = ModelUtils.createFolder(FOLDER_ID_1, TEST_FOLDER_1);
        User user = ModelUtils.createUser("user_id_1", "Test User");

        stream.stream(folder1);
        stream.stream(ModelUtils.createWorkbook("folder_id_2", "Test Folder 2"));
        stream.stream(user);
        stream.stream(ModelUtils.createPermission(user.getId(), folder1));

        List<String> publishedProjectNames = AtScaleAPI.getSinglePublishedProjectNames(new AtScaleServerClient(configuration), configuration);
        for (String projectName : publishedProjectNames) {
            stream.stream(ModelUtils.createWorkbook(projectName, projectName));
        }
    }

    /**
     * Add Alation certification to BI object, that could create or update multiple BI objects.
     *
     * @param configuration     contains 1) user overridable connector configuration and 2) Alation wide
     *                          configuration that could affect the extraction output (e.g. the max data size of an
     *                          object).
     * @param certificationNote is the text note added against the BI object during certification.
     * @param stream            is the handle to the channel that can be used to send the data back to the
     *                          caller.
     * @throws ConnectorException when metadata extraction faces an unrecoverable error. The exception
     *                            should contain the cause of the error. If possible it should also have the stacktrace and a
     *                            hint on how to resolve it. The RPC caller will receive the exception's message as returned
     *                            by {@link alation.sdk.core.error.ConnectorException#getMessage()}.
     */
    @Override
    public void certify(
            BIObject biObject,
            AtScaleBIConfiguration configuration,
            String certificationNote,
            Stream<MetadataMessage> stream)
            throws ConnectorException {
        Folder folder1 = ModelUtils.createFolder(FOLDER_ID_1, TEST_FOLDER_1);
        Report report1 =
                ModelUtils.createReport(biObject.getId(), biObject.getName(), folder1.getId());
        ReportColumn reportColumn1 =
                ModelUtils.createReportColumn("report_column_id_1", "Test Report Column", report1.getId());

        stream.stream(report1);
        stream.stream(reportColumn1);
    }

    /**
     * Remove Alation certification from BI object, that could create or update multiple BI objects.
     *
     * @param configuration     contains 1) user overridable connector configuration and 2) Alation wide
     *                          configuration that could affect the extraction output (e.g. the max data size of an
     *                          object).
     * @param certificationNote is the note added against the BI object during de-certification.
     * @param stream            is the handle to the channel that can be used to send the data back to the
     *                          caller.
     * @throws ConnectorException when metadata extraction faces an unrecoverable error. The exception
     *                            should contain the cause of the error. If possible it should also have the stacktrace and a
     *                            hint on how to resolve it. The RPC caller will receive the exception's message as returned
     *                            by {@link alation.sdk.core.error.ConnectorException#getMessage()}.
     */
    @Override
    public void decertify(
            BIObject biObject,
            AtScaleBIConfiguration configuration,
            String certificationNote,
            Stream<MetadataMessage> stream)
            throws ConnectorException {
        LOGGER.info("decertify");
        Folder folder1 = ModelUtils.createFolder(FOLDER_ID_1, TEST_FOLDER_1);
        Report report1 =
                ModelUtils.createReport(biObject.getId(), biObject.getName(), folder1.getId());
        ReportColumn reportColumn1 =
                ModelUtils.createReportColumn("report_column_id_1", "Test Report Column", report1.getId());

        stream.stream(report1);
        stream.stream(reportColumn1);
    }

    /**
     * @param folderList          - folder list to be included or excluded.
     * @param isIncludeFilter     - if isIncludeFilter is "true", include folders from the folderList,
     *                            if "false" exclude folders from folderList
     * @param atScaleServerClient - serverClient for establishing database connection
     * @param configuration       - configuration for getting properties
     */
    public void getCatalogAndCubesToBeExtracted(
            Set<String> catalogNames,
            Set<String> cubeNames,
            List<Folder> folderList,
            boolean isIncludeFilter,
            AtScaleServerClient atScaleServerClient,
            AtScaleBIConfiguration configuration) {

        CatalogExtractor catalogExtractor = new CatalogExtractor(configuration);
        CubeExtractor cubeExtractor = new CubeExtractor(configuration, null);
        Set<String> filteredCatalogNames = new HashSet<>();
        Set<String> filteredCubeNames = new HashSet<>();

        folderList.forEach(
                folder -> {
                    if (folder.getBiObjectType().equalsIgnoreCase("Project") || folder.getBiObjectType().equalsIgnoreCase("Workbook")) {
                        filteredCatalogNames.add(folder.getName());
                    } else if (folder.getBiObjectType().equalsIgnoreCase("Cube")) {
                        filteredCubeNames.add(folder.getName());
                    }
                });

        // if isIncludeFilter is true and folder List is not empty, extract data only for given cubes
        // and catalogs
        if (isIncludeFilter && !folderList.isEmpty()) {
            LOGGER.info("Filtering catalog and cube names from include filter.");
            catalogNames.addAll(
                    cubeExtractor.extractCatalogNameFromCubes(atScaleServerClient, configuration, filteredCubeNames)); // Never populate this
            catalogNames.addAll(filteredCatalogNames);
            cubeNames.addAll(
                    cubeExtractor.extractCubeNamesFromCatalog(filteredCatalogNames, atScaleServerClient, configuration));
            cubeNames.addAll(filteredCubeNames);
        }

        // if isIncludeFilter is false and folder List is not empty, extract all the data excluding for
        // the folders given the list.
        if (!isIncludeFilter && !folderList.isEmpty()) {
            LOGGER.info("Filtering catalog and cube names from exclude filter.");
            catalogNames.addAll(
                    catalogExtractor.extractCatalogNameToBeIncluded(
                            filteredCatalogNames, atScaleServerClient, configuration, false));
            cubeNames.addAll(
                    cubeExtractor.extractCubeNameToBeIncluded(catalogNames, filteredCubeNames, atScaleServerClient, configuration, false));
        }

        // if isIncludeFilter is false and folder List is empty, extract everything
        if (/*!isIncludeFilter &&*/ folderList.isEmpty()) {
            LOGGER.info("Extracting all the catalogs and cubes.");
            catalogNames.addAll(
                    catalogExtractor.extractCatalogNameToBeIncluded(null, atScaleServerClient, configuration, true));
            if (catalogNames.isEmpty()) throw new ObjectNotFoundException("No published projects found for import");
            cubeNames.addAll(cubeExtractor.extractCubeNameToBeIncluded(catalogNames, null, atScaleServerClient, configuration, true));
        }
        LOGGER.info("Catalogs : " + String.join(", ", catalogNames));
        LOGGER.info("Cubes : " + String.join(", ", cubeNames));
    }

    // Details of data types mapping: https://docs.microsoft.com/en-us/openspecs/sql_server_protocols/ms-ssas/aee32107-fde6-43bd-beeb-46e82851abf4
    private void populateDataTypeVsNameMap() {
        dataTypeVsNameMap.put(2, INT);
        dataTypeVsNameMap.put(3, INT);
        dataTypeVsNameMap.put(4, FLOAT);
        dataTypeVsNameMap.put(5, FLOAT);
        dataTypeVsNameMap.put(6, FLOAT); // Currency
        dataTypeVsNameMap.put(7, "Double"); // Date values are stored as Double, the whole part of which is the number of days since December 30, 1899, and the fractional part of which is the fraction of a day.
        dataTypeVsNameMap.put(8, STRING); // A pointer to a BSTR, which is a null-terminated character string in which the string length is stored with the string.
        dataTypeVsNameMap.put(11, "Boolean");
        dataTypeVsNameMap.put(14, FLOAT);
        dataTypeVsNameMap.put(16, INT);
        dataTypeVsNameMap.put(17, INT);
        dataTypeVsNameMap.put(18, INT);
        dataTypeVsNameMap.put(19, INT);
        dataTypeVsNameMap.put(20, INT);
        dataTypeVsNameMap.put(21, INT);
        dataTypeVsNameMap.put(72, STRING); // GUID
        dataTypeVsNameMap.put(128, INT); // Binary
        dataTypeVsNameMap.put(129, STRING);
        dataTypeVsNameMap.put(130, STRING);
        dataTypeVsNameMap.put(131, FLOAT);
        dataTypeVsNameMap.put(133, INT); // Date as yyyymmdd
        dataTypeVsNameMap.put(134, INT); // Time as hhmmss
        dataTypeVsNameMap.put(135, INT); // date-time stamp (yyyymmddhhmmss plus a fraction in billionths).
    }

    /**
     * Extract metadata from the BI source.
     *
     * @param configuration   contains 1) user overridable connector configuration and 2) Alation wide
     *                        configuration that could affect the extraction output (e.g. the max data size of an
     *                        object).
     * @param alationFolders  contains a list of folders to limit extraction.
     * @param isIncludeFilter controls the type of filter. If True, then extraction should be limited
     *                        only to the metadata contained in the list of provided folders. Otherwise, the list of
     *                        provided folders should be excluded from extraction.
     * @param stream          is the handle to the channel that can be used to send the data back to the
     *                        caller.
     * @param alationUsers    is the list of Alation users. The intention is to limit extraction to
     *                        metadata accessible to existing Alation users.
     * @throws ConnectorException when metadata extraction faces an unrecoverable error. The exception
     *                            should contain the cause of the error. If possible it should also have the stacktrace and a
     *                            hint on how to resolve it. The RPC caller will receive the exception's message as returned
     *                            by {@link alation.sdk.core.error.ConnectorException#getMessage()}.
     */
    @Override
    public void metadataExtraction(
            AtScaleBIConfiguration configuration,
            List<Folder> alationFolders,
            boolean isIncludeFilter,
            Stream<MetadataMessage> stream,
            List<AlationUser> alationUsers)
            throws ConnectorException {

        // Load folders and users from files for testing
        if (Constants.USE_TEST_FOLDERS_AND_USERS) {
            isIncludeFilter = Constants.IS_INCLUDE_FILTER;
            try {
                alationFolders = ModelUtils.getAlationFoldersFromFile("alationFolders.json");
                alationUsers = ModelUtils.getAlationUsersFromFile("alationUsers.json");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        AtScaleServerClient atScaleServerClient = new AtScaleServerClient(configuration);
        Set<String> catalogNames = new HashSet<>();
        Set<String> cubeNames = new HashSet<>();
        Map<String, ConnectionDetails> connectionDetailsMap;
        List<Dependency> dependencies;

        try {
            getCatalogAndCubesToBeExtracted(
                    catalogNames, cubeNames, alationFolders, isIncludeFilter, atScaleServerClient, configuration);

            connectionDetailsMap = AtScaleAPI.getConnectionDetails(atScaleServerClient, configuration);

            dependencies = AtScaleAPI.retrieveAllDependencies(atScaleServerClient, configuration, catalogNames);

        } catch (Exception e) {
            LOGGER.error(e);
            return;
        }

        populateDataTypeVsNameMap();

        for (AlationUser user : alationUsers) {
            stream.stream(ModelUtils.createUser(user.getUsername(), user.getUsername()));
        }
        for (Folder folder : alationFolders) {
            folder.setIsExtractable(true);
        }

        String catalogNamesStr = String.join(", ", catalogNames);
        String cubeNamesStr = String.join(", ", cubeNames);
        String foldersStr = String.format("%d %s", alationFolders.size(), alationFolders);
        String msg = String.format("Before doing extractions:%n    - catalogNames: %s%n    - cubeNames: %s%n    - folders: %s",
                catalogNamesStr, cubeNamesStr, foldersStr);
        LOGGER.info(msg);


        CatalogExtractor catalogExtractor = new CatalogExtractor(configuration);
        catalogExtractor.extractCatalogs(catalogNames, stream, atScaleServerClient, configuration);

        CubeExtractor cubeExtractor =
                new CubeExtractor(configuration, catalogExtractor.getCatalogNameVsId());
        cubeExtractor.extractCubes(catalogNames, cubeNames, atScaleServerClient, configuration, stream);
        Map<String, String> cubeNameVsId = cubeExtractor.getCubeNameVsId();

        // Datasets need model parsing (projectMap) for database which isn't yet populated in DMV
        DatasetExtractor datasetExtractor = new DatasetExtractor(connectionDetailsMap);
        datasetExtractor.extractDatasets(catalogNames, atScaleServerClient, configuration, stream);
        Map<String, Dataset> datasetMap = datasetExtractor.getDatasetMap();

        ColumnExtractor columnExtractor = new ColumnExtractor(connectionDetailsMap, datasetMap, dataTypeVsNameMap, dependencies);
        columnExtractor.extractColumns(catalogNames, atScaleServerClient, configuration, stream);
        Map<String, Column> columnMap = columnExtractor.populateColumnMap();

        DimensionExtractor dimExtractor = new DimensionExtractor(cubeNameVsId, null);
        HierarchiesExtractor hierExtractor = new HierarchiesExtractor(cubeNameVsId, null, null, null);

        // Need to find hidden dimensions and hierarchies so associated attributes will also be hidden
        Set<String> foundObjects = new HashSet<>();
        dimExtractor.getDims(atScaleServerClient, configuration, catalogNames, foundObjects);
        hierExtractor.getHiers(atScaleServerClient, configuration, catalogNames, foundObjects);

        LevelsExtractor levelsExtractor =
                new LevelsExtractor(cubeNameVsId, datasetMap, foundObjects);
        levelsExtractor.extractLevels(catalogNames, atScaleServerClient, configuration, stream);
        Set<String> ignoreHierarchySet = levelsExtractor.getIgnoreHierarchySet();

        // Dimensions need model parsing (projectMap) for list of connections used
        DimensionExtractor dimensionExtractor =
                new DimensionExtractor(cubeNameVsId, levelsExtractor.getDimToDatasetsMap());
        dimensionExtractor.extractDimensions(atScaleServerClient, configuration, stream, catalogNames);

        // Hierarchies need model parsing (projectMap) for list of connections used
        HierarchiesExtractor hierarchiesExtractor =
                new HierarchiesExtractor(cubeNameVsId, ignoreHierarchySet, levelsExtractor.getDimToDatasetsMap(), foundObjects);
        hierarchiesExtractor.setDimensionNameVsId(dimensionExtractor.getDimensionNameVsId());
        hierarchiesExtractor.extractHierarchies(catalogNames, atScaleServerClient, configuration, stream);

        // Measures need model parsing (projectMap) to get the list of referenced DatasourceColumnIds for Calculated Measures
        MeasureExtractor measureExtractor =
                new MeasureExtractor(dataTypeVsNameMap, datasetMap, columnMap);
        measureExtractor.setCubeNameVsId(cubeNameVsId);
        measureExtractor.extractMeasures(catalogNames, atScaleServerClient, configuration, stream);
        Map<String, Set<String>> mgToDatasetsMap = measureExtractor.populateMgToDatasetsMap();

        // Measure Groups need model parsing (projectMap) for list of connections used
        MeasureGroupExtractor measureGroupExtractor =
                new MeasureGroupExtractor(cubeNameVsId, mgToDatasetsMap);
        measureGroupExtractor.extractMeasureGroups(catalogNames, atScaleServerClient, configuration, stream);

        LOGGER.info("Extraction completed.");
    }
}
