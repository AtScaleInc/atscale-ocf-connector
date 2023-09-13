package atscale.api;

import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.exception.AtScaleServerClientException;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.utils.Base64Coder;
import org.apache.log4j.Logger;

import static atscale.api.APIConstants.*;

/**
 *
 */
public class AtScaleServerClient {

    private static final Logger LOGGER = Logger.getLogger(AtScaleServerClient.class);
    private String token;
    private String urlAuthorization;
    private String urlQuery;
    private final String engineHost;
    private final String organization;
    private final String dcHost;
    private final String authHost;
    private final int authorizationPort;
    private final int designCenterPort;
    private final int enginePort;
    private final String username;
    private final String password;
    private AtScaleBIConfiguration configuration;

    /**
     * @return
     */
    public String getUrlAuthorization() {
        return urlAuthorization;
    }

    /**
     * @return
     */
    public String getUrlQuery() {
        return urlQuery;
    }

    /**
     * @param configuration
     */
    public AtScaleServerClient(AtScaleBIConfiguration configuration) {
        this.engineHost = configuration.getAPIHost();
        this.organization = configuration.getOrganization();
        this.dcHost = configuration.getDCHost();
        this.authHost = configuration.getAuthHost();
        this.authorizationPort = Integer.parseInt(configuration.getAuthPort());
        this.designCenterPort = Integer.parseInt(configuration.getDCPort());
        this.enginePort = Integer.parseInt(configuration.getAPIPort());
        this.username = configuration.getUserName();
        this.password = configuration.getPassword();
        this.configuration = configuration;

        try {
            buildAuthorizationURL();
            buildQueryURL();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    public String getProtocol() throws AtScaleServerClientException {
        return (configuration.getSSLMode().equals(AtScaleBIConfiguration.SSL_MODE_NONE)) ? HTTP : HTTPS;
    }

    /**
     * Builds the URL to the AtScale Server for Authorization
     *
     * @throws AtScaleServerClientException
     */
    public void buildAuthorizationURL() throws AtScaleServerClientException {
        try {
            String protocol = getProtocol();
            if (this.authorizationPort > 0) {
                this.urlAuthorization = String.format("%s://%s:%d/%s/auth", protocol, authHost, authorizationPort,
                        organization);
            } else {
                this.urlAuthorization = String.format("%s://%s/%s/auth", protocol, authHost, organization);
            }
            LOGGER.info("Connection URL: " + this.urlAuthorization);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new AtScaleServerClientException("Unable to create AtScale Authorization URL", ex);
        }
    }

    /**
     * Builds the URL to the AtScale Server for Queries
     *
     * @throws AtScaleServerClientException
     */
    public void buildQueryURL() throws AtScaleServerClientException {
        try {
            String protocol = getProtocol();
            if (this.enginePort > 0 && this.enginePort != 443) {
                this.urlQuery = String.format("%s://%s:%d/xmla/%s", protocol, engineHost, enginePort, organization);
            } else {
                this.urlQuery = String.format("%s://%s/xmla/%s", protocol, engineHost, organization);
            }

            LOGGER.info("Query URL: " + this.urlQuery);
        } catch (Exception ex) {
            LOGGER.error("Error while creating AtScale Query URL", ex);
            throw new AtScaleServerClientException("Unable to create AtScale Query URL", ex);
        }
    }

    /**
     * @param endpoint
     * @param projectUUID
     * @param cubeUUID
     * @param portType
     * @return
     * @throws AtScaleServerClientException
     */
    public String buildAPIURL(String endpoint, String projectUUID, String cubeUUID, String portType)
            throws AtScaleServerClientException {
        String retVal;
        try {
            String protocol = getProtocol();
            if (endpoint.startsWith(FORWARD_SLASH)) {
                endpoint = endpoint.substring(1);
            }
            endpoint = endpoint.replace("{orgId}", organization).replace("{projectId}", projectUUID).replace("{cubeId}",
                    cubeUUID);
            if (portType.equals(ENGINE)) {
                retVal = String.format("%s://%s:%d/%s", protocol, engineHost, enginePort, endpoint);
            } else {
                retVal = String.format("%s://%s:%d/%s", protocol, dcHost, designCenterPort, endpoint);
            }
            LOGGER.info("API Endpoint URL: " + endpoint);
        } catch (Exception ex) {
            LOGGER.error("Error while creating AtScale API URL", ex);
            throw new AtScaleServerClientException("Unable to create AtScale API URL", ex);
        }
        return retVal;
    }

    /**
     * Establish SQL Server database connection
     *
     * @throws AtScaleServerClientException
     */
    public void connect() throws AtScaleServerClientException {
        try {

            Unirest.setHttpClient(this.configuration.getHttpClient());

            HttpResponse<String> responseAuth = Unirest.get(getUrlAuthorization())
                    .header(AUTHORIZATION, BASIC + Base64Coder.encodeString(username + ":" + password)).asString();

            this.token = responseAuth.getBody();
            LOGGER.info("Established Connection to AtScale Server");
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new AtScaleServerClientException("Unable to create AtScale connection", ex);
        }
    }

    /**
     * To get the connection for creating statement and executing queries
     *
     * @return connection object
     * @throws AtScaleServerClientException
     */
    public String getConnection() throws AtScaleServerClientException {
        if (token.isEmpty()) {
            connect();
        }
        return token;
    }

    /**
     * To disconnect connection
     */
    public void disconnect() {
        // May be for future implementation.
    }

    /**
     * In case the connection is not working properly, disconnect from the server
     * and establish a new connection
     *
     * @throws AtScaleServerClientException
     */
    public void reconnect() throws AtScaleServerClientException {
        disconnect();
        connect();
    }
}
