package atscale.api;

import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.models.ConnectionDetails;
import atscale.biconnector.models.Dependency;
import atscale.biconnector.utils.Constants;
import atscale.biconnector.utils.SOAPResultSet;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;

import static atscale.api.APIConstants.*;

/**
 * AtScaleAPI is used for the Atscale API Utility.
 */
public class AtScaleAPI {

    private static final Logger LOGGER = Logger.getLogger(AtScaleAPI.class);
    private static final String PUBLISHED_PROJECTS_ENDPOINT = "/projects/published/orgId/{orgId}"; // Using engine host
    private static final String CONNECTIONS_ENDPOINT = "/connection-groups/orgId/{orgId}"; // Using engine host
    private static final String GOOGLE_API_HOST = "www.googleapis.com";
    private static final String SMALL_USER_QUERY_ROLE = "small_user_query_role";

    private AtScaleAPI() {
        throw new IllegalStateException("AtScaleAPI Utility class");
    }

    /**
     * @param atScaleServerClient
     * @param configuration
     * @return
     */
    public static List<String> getSinglePublishedProjectNames(AtScaleServerClient atScaleServerClient,
                                                              AtScaleBIConfiguration configuration) {
        ArrayList<String> projectList = new ArrayList<>();
        try {
            // The PROJECTS_ENDPOINT gets the list of draft projects.
            String inline = apiConnect(atScaleServerClient, configuration, PUBLISHED_PROJECTS_ENDPOINT);

            JSONParser parse = new JSONParser();
            JSONObject dataObj = (JSONObject) parse.parse(inline);
            JSONArray projList = (JSONArray) dataObj.get("response"); // This is the list of draft projects

            for (Object obj : projList) {
                JSONObject proj = (JSONObject) obj;
                if (proj.get("linkedProjectId").toString().equals(proj.get("id").toString())
                        || proj.get("linkedProjectId").toString().equals("")) {
                    projectList.add(proj.get("name").toString());
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error while getting the published project names: ", e);
        }
        if (projectList.isEmpty()) {
            LOGGER.info("No published projects found via API to return");
        }
        return projectList;
    }

    /**
     * @param atScaleServerClient
     * @param configuration
     * @param apiURL
     * @return
     * @throws KeyManagementException
     * @throws KeyStoreException
     * @throws CertificateException
     * @throws NoSuchAlgorithmException
     * @throws IOException
     * @throws UnirestException
     */
    private static String apiConnect(AtScaleServerClient atScaleServerClient, AtScaleBIConfiguration configuration, String apiURL)
            throws KeyManagementException, KeyStoreException, CertificateException, NoSuchAlgorithmException,
            IOException, UnirestException {
        atScaleServerClient.connect();
        String token = atScaleServerClient.getConnection();
        String url = atScaleServerClient.buildAPIURL(apiURL, "", "", ENGINE);

        Unirest.setHttpClient(configuration.getHttpClient());

        HttpResponse<String> httpResponse = Unirest.get(url).header(AUTHORIZATION, BEARER + token)
                .header(CONTENT_TYPE, APPLICATION_XML).asString();
        StringBuilder inline = new StringBuilder();
        if (httpResponse.getStatus() != 200) {
            LOGGER.error("API call failed with error: " + httpResponse.getStatusText());
        } else if (httpResponse.getBody().length() == 0) {
            LOGGER.error("Empty response body from API call: " + url);
        } else {
            try (Scanner scanner = new Scanner(httpResponse.getBody())) {
                while (scanner.hasNext()) {
                    inline.append(scanner.nextLine());
                }
            }
        }
        return inline.toString();
    }

    /**
     * Populate map from connectionID found in project datasets to platformType,
     * organizationId From the subgroup for "small_user_query_role" get the host,
     * port, name
     *
     * @param atScaleServerClient
     * @param configuration
     * @return
     */
    public static Map<String, ConnectionDetails> getConnectionDetails(AtScaleServerClient atScaleServerClient,
                                                                      AtScaleBIConfiguration configuration) {
        Map<String, ConnectionDetails> connectionMap = new HashMap<>();

        try {
            String inline = apiConnect(atScaleServerClient, configuration, CONNECTIONS_ENDPOINT);
            JSONParser parse = new JSONParser();
            JSONObject dataObj = (JSONObject) parse.parse(inline);
            JSONObject response = (JSONObject) dataObj.get("response");
            JSONObject results = (JSONObject) response.get("results");
            JSONArray connectionList = (JSONArray) results.get("values");

            for (Object obj : connectionList) {
                JSONObject conn = (JSONObject) obj;
                String platformType = conn.get("platformType").toString();
                String organizationId = conn.get("organizationId").toString();
                String connectionId = conn.get("connectionId").toString();
                String connectionName = conn.get("name").toString();
                String database = conn.get("database") != null ? conn.get("database").toString() : "";

                JSONArray subList = (JSONArray) conn.get("subgroups");
                boolean found = validateInteractiveRole(connectionMap, platformType, organizationId, connectionId, connectionName, database, subList);

                if (!found) {
                    LOGGER.warn("No query role found for connection ID '" + connectionId + "' so host and port may not be set");
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error while getting the connection details: ", e);
        }
        if (connectionMap.isEmpty()) {
            LOGGER.info("No connections found via API to return");
        }
        return connectionMap;
    }

    /**
     * @param connectionMap
     * @param platformType
     * @param organizationId
     * @param connectionId
     * @param connectionName
     * @param database
     * @param subList
     * @return
     */
    private static boolean validateInteractiveRole(Map<String, ConnectionDetails> connectionMap, String platformType, String organizationId, String connectionId, String connectionName, String database, JSONArray subList) {
        boolean found = false;
        for (Object sub : subList) {
            JSONObject subGroup = (JSONObject) sub;
            JSONArray rolesList = (JSONArray) subGroup.get("queryRoles");

            for (Object queryRole : rolesList) {
                if (isInteractiveRole(queryRole.toString())) {
                    found = true;
                    String host = subGroup.get("hosts").toString().toLowerCase();
                    host = host.contains(GOOGLE_API_HOST) ? GOOGLE_API_HOST : host;
                    String port = subGroup.get("port").toString();
                    connectionMap.put(connectionId, new ConnectionDetails(connectionName, host, port, platformType, organizationId, connectionId, database));
                    break;
                }
            }
            if (found) {
                break;
            }
        }
        return found;
    }

    private static boolean isInteractiveRole(String role) {
        return role.equals(SMALL_USER_QUERY_ROLE);
    }


    /**
     * @param atScaleServerClient
     * @param configuration
     * @param projects
     * @return it will return the List of Dependency if available else emptyList
     */
    public static List<Dependency> retrieveAllDependencies(AtScaleServerClient atScaleServerClient,
                                                           AtScaleBIConfiguration configuration, Set<String> projects) {
        LOGGER.debug("Retrieving all dependencies...");

        List<Dependency> dependencies = new ArrayList<>();

        for (String projectName : projects) {
            // In DEPENDENCIES, we can't filter using 'WHERE catalog_name ='
            // [TABLE] is actually the dataset so get db/schema/table from it
            String query = Constants.DEPENDENCIES_QUERY; // DEPENDENCIES_SELECTIVE_QUERY
            LOGGER.info("SQL Query for DEPENDENCIES on catalog '" + projectName + "' = " + query);

            try {
                SOAPResultSet resultSet = SOAPQuery.runSOAPQuery(atScaleServerClient, configuration, query,
                        "<Catalog>" + projectName + "</Catalog>");

                while (resultSet != null && resultSet.next()) {
                    setDependencies(dependencies, projectName, resultSet);
                }
            } catch (Exception e) {
                LOGGER.error("Error while fetching result set from AtScaleAPI: ", e);
            }
        }
        LOGGER.debug("Retrieved {" + dependencies.size() + "} dependencies");
        return dependencies;
    }

    private static void setDependencies(List<Dependency> dependencies, String projectName, SOAPResultSet resultSet) {
        try {
            Dependency dependency = new Dependency();
            dependency.setDatabaseName(resultSet.getString("DATABASE_NAME"));
            dependency.setObjectType(resultSet.getString("OBJECT_TYPE"));
            dependency.setTable(resultSet.getString("TABLE"));
            dependency.setObject(resultSet.getString("OBJECT"));
            dependency.setExpression(resultSet.getString("EXPRESSION"));
            dependency.setReferencedObjectType(resultSet.getString("REFERENCED_OBJECT_TYPE"));
            dependency.setReferencedTable(resultSet.getString("REFERENCED_TABLE"));
            dependency.setReferencedObject(resultSet.getString("REFERENCED_OBJECT"));
            dependency.setReferencedExpression(resultSet.getString("REFERENCED_EXPRESSION"));
            dependency.setCatalogName(projectName);
            dependency.setCubeName(resultSet.getString("CUBE_NAME"));
            dependencies.add(dependency);
        } catch (Exception e) {
            LOGGER.error(String.format("Error while creating Dependency object for row id : %d",
                    resultSet.getRow()));
            LOGGER.error(e.getMessage(), e);
        }
    }
}
