package atscale.biconnector.configuration;

import alation.sdk.core.error.ValidationException;
import alation.sdk.core.manifest.FeatureEnum;
import alation.sdk.core.manifest.parameter.*;
import alation.sdk.core.request.AbstractConfiguration;
import alation.sdk.util.jdbc.ssl.TruststoreFactory;
import atscale.biconnector.exception.AtScaleServerClientException;
import atscale.biconnector.utils.GroupEnum;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.utils.Base64Coder;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;

import static atscale.api.APIConstants.*;

/**
 * AtScaleBIConfiguration determines the connector configuration for the sample BI datasource. The
 * configuration parameters to be included in the manifest, apart from application configuration
 * parameters, should be added in {@link AtScaleBIConfiguration#buildParameters()}. Apart from this,
 * {@link AtScaleBIConfiguration#validate()} just performs a validation check on the Server URI,
 * since this is only a sample implementation.
 */
public class AtScaleBIConfiguration extends AbstractConfiguration {

    private static final Logger LOGGER = Logger.getLogger(AtScaleBIConfiguration.class);
    private static final String ERROR_EMPTY_URI = "URI for %s can't be empty";
    private static final String ERROR_INVALID_URI = "Invalid URI %s.";
    private static final String ERROR_EMPTY_PORT = "Port can't be empty";
    private static final String ERROR_INVALID_PORT = "Invalid Port %s.";
    private static final String ERROR_EMPTY_USERNAME = "User Name can't be empty";
    private static final String ERROR_EMPTY_ORGANIZATION = "Organization ID can't be empty";
    private static final String ERROR_EMPTY_PASSWORD = "Password can't be empty";
    public static final String ATSCALE_SERVER_DC_HOST_NAME = "dchost";
    public static final String ATSCALE_SERVER_DC_PORT = "dcport";
    public static final String ATSCALE_ORGANIZATION = "organization";
    public static final String ATSCALE_SERVER_API_HOST_NAME = "apihost";
    public static final String ATSCALE_SERVER_API_PORT = "apiport";
    public static final String ATSCALE_SERVER_AUTH_HOST_NAME = "authhost";
    public static final String ATSCALE_SERVER_AUTH_PORT = "authport";
    public static final String ATSCALE_SERVER_USERNAME = "username";
    public static final String ATSCALE_SERVER_PASSWORD = "password";

    public static final String DC_HOST = "DC";
    public static final String AUTH_HOST = "AUTH";
    public static final String API_HOST = "API";
    public static final String CRED = "CRED";
    public static final String ORG = "ORG";

    public static final String SSL_CERT = "sslcert";
    public static final String SSL_MODE = "sslmode";
    public static final String SSL_MODE_NONE = "Do Not Use SSL";
    public static final String SSL_MODE_JVM = "Use CA Root Certs";
    public static final String SSL_MODE_TRUST = "Trust All Certificates";
    public static final String SSL_MODE_CERT = "Specify SSL Certificate";
    public static final String DIGIT_REGEX = "-?\\d+";
    protected File truststore = null;
    protected char[] truststorePassword = null;

    public AtScaleBIConfiguration() {
        super();
    }

    private static Map<String, String> validationErrorMap = new HashMap<>();

    static {
        validationErrorMap.put(DC_HOST, "Error while connecting to Design Center.");
        validationErrorMap.put(API_HOST, "Error while connecting to API Server.");
        validationErrorMap.put(AUTH_HOST, "Error while connecting to Authorization Server.");
        validationErrorMap.put(CRED, "Invalid/Incorrect Credentials.");
        validationErrorMap.put(ORG, "Invalid/Incorrect Organization.");
    }

    public File getTruststore() throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        if (this.truststore == null) {
            this.truststore = TruststoreFactory.create(this.getSSLCert(), this.getTrustorePassword());
        }
        return this.truststore;
    }

    // FeatureEnum's: BI_GENERAL - , BI_MDE_GBM_V2 - Metadata Extraction, BI_CERT_GBM_V2 - Certification
    @Override
    public Collection<Parameter> buildParameters() {
        List<Parameter> parameterList = new ArrayList<>();
        parameterList.add(
                new TextParam(
                        ATSCALE_SERVER_DC_HOST_NAME,
                        "Design Center Host Name",
                        "Host Name of AtScale Design Center.",
                        FeatureEnum.BI_GENERAL,
                        GroupEnum.GROUP_SERVER_CONNECTION.getGroupName()));
        parameterList.add(
                new TextParam(
                        ATSCALE_SERVER_DC_PORT,
                        "Design Center Port",
                        "Port for AtScale Design Center.",
                        FeatureEnum.BI_GENERAL,
                        "10500",
                        GroupEnum.GROUP_SERVER_CONNECTION.getGroupName()));
        parameterList.add(
                new TextParam(
                        ATSCALE_SERVER_API_HOST_NAME,
                        "API Host Name",
                        "Host Name for AtScale API Server.",
                        FeatureEnum.BI_GENERAL,
                        GroupEnum.GROUP_SERVER_CONNECTION.getGroupName()));
        parameterList.add(
                new TextParam(
                        ATSCALE_SERVER_API_PORT,
                        "API Port",
                        "Port for AtScale API Server.",
                        FeatureEnum.BI_GENERAL,
                        "10502",
                        GroupEnum.GROUP_SERVER_CONNECTION.getGroupName()));
        parameterList.add(
                new TextParam(
                        ATSCALE_SERVER_AUTH_HOST_NAME,
                        "Authorization Host Name",
                        "Host Name for AtScale Authorization Server.",
                        FeatureEnum.BI_GENERAL,
                        GroupEnum.GROUP_SERVER_CONNECTION.getGroupName()));
        parameterList.add(
                new TextParam(
                        ATSCALE_SERVER_AUTH_PORT,
                        "Authorization Port",
                        "Port for AtScale Authorization Server.",
                        FeatureEnum.BI_GENERAL,
                        "10500",
                        GroupEnum.GROUP_SERVER_CONNECTION.getGroupName()));
        parameterList.add(
                new TextParam(
                        ATSCALE_ORGANIZATION,
                        "Organization ID",
                        "AtScale Organization ID for Metadata Extraction (i.e. default).",
                        FeatureEnum.BI_GENERAL,
                        "default",
                        GroupEnum.GROUP_SERVER_CONNECTION.getGroupName()));
        parameterList.add(
                new TextParam(
                        ATSCALE_SERVER_USERNAME,
                        "User Name",
                        "User Name for AtScale API Server.",
                        FeatureEnum.BI_GENERAL,
                        GroupEnum.GROUP_SERVER_CONNECTION.getGroupName()));
        parameterList.add(
                new EncryptedTextParam(
                        ATSCALE_SERVER_PASSWORD,
                        "Password",
                        "Password for AtScale API Server.",
                        FeatureEnum.BI_GENERAL,
                        GroupEnum.GROUP_SERVER_CONNECTION.getGroupName()));

        parameterList.add(
                new RadioParam(SSL_MODE,
                        "SSL Mode",
                        "Specify what SSL Mode to use.\nDo Not Use SSL - Use http\nUse CA Root Certs - Basic HTTPS request\nTrust All Certificates - Encrypt traffic, but bypass validation\nSpecify SSL Certificate - Must upload certificate",
                        FeatureEnum.BI_GENERAL, SSL_MODE_NONE,
                        GroupEnum.GROUP_SERVER_CONNECTION.getGroupName(),
                        Arrays.asList(SSL_MODE_NONE, SSL_MODE_JVM, SSL_MODE_TRUST, SSL_MODE_CERT)));

        parameterList.add(
                new EncryptedBinaryParam(
                        SSL_CERT,
                        "SSL certificate",
                        "SSL certificate used to estable a trusted connection with the database",
                        FeatureEnum.BI_GENERAL,
                        GroupEnum.GROUP_SERVER_CONNECTION.getGroupName()));

        return parameterList;
    }

    public void validate() throws ValidationException {
        validateDC();
        validateAPI();
        validateAuth();
        validateCred();
        validateOrg();
    }

    private void validateDC() throws ValidationException {
        validateDCHost();
        validateDCPort();
        validateConnection(DC_HOST);
    }

    private void validateAPI() throws ValidationException {
        validateAPIHost();
        validateAPIPort();
        validateConnection(API_HOST);
    }

    public void validateAuth() throws ValidationException {
        validateAuthHost();
        validateAuthPort();
        validateConnection(AUTH_HOST);
    }

    public void validateCred() throws ValidationException {
        validateUserName();
        validatePassword();
        validateConnection(CRED);
    }

    public void validateOrg() throws ValidationException {
        validateOrganization();
        validateConnection(ORG);
    }

    public String getProtocol() throws AtScaleServerClientException {
        return (this.getSSLMode().equals(AtScaleBIConfiguration.SSL_MODE_NONE)) ? HTTP : HTTPS;
    }

    public String getToken() throws AtScaleServerClientException {
        String token;
        String url = buildAuthorizationURL();
        try {
            Unirest.setHttpClient(getHttpClient());
            HttpResponse<String> responseAuth = Unirest.get(url)
                    .header(AUTHORIZATION, BASIC + Base64Coder.encodeString(getUserName() + ":" + getPassword())).asString();
            token = responseAuth.getBody();
        } catch (Exception e) {
            LOGGER.error(String.format("Error while connecting to {}", url), e);
            throw new AtScaleServerClientException(String.format("Error while connecting to {}", url), e);
        }
        return token;
    }

    public String buildAuthorizationURL() throws AtScaleServerClientException {
        String urlAuthorization;
        urlAuthorization = String.format("%s://%s:%s/%s/auth", getProtocol(), getAuthHost(), getAuthPort(),
                getOrganization());
        return urlAuthorization;
    }

    public String getURL(String hostType) throws ValidationException{
        String url = StringUtils.EMPTY;
        switch (hostType) {
            case DC_HOST:
                url = String.format("%s://%s:%s", getProtocol(), getDCHost(), getDCPort());
                break;
            case AUTH_HOST:
                url = String.format("%s://%s:%s", getProtocol(), getAuthHost(), getAuthPort());
                break;
            case API_HOST:
                url = String.format("%s://%s:%s/health", getProtocol(), getAPIHost(), getAPIPort());
                break;
            case CRED:
                url = String.format("%s://%s:%s/%s/auth", getProtocol(), getDCHost(), getDCPort(), getOrganization());
                break;
            case ORG:
                url = String.format("%s://%s:%s/organizations/orgId/%s", getProtocol(), getAPIHost(), getAPIPort(), getOrganization());
                break;
            default:
                throw new ValidationException("INVALID_HOST_TYPE");
        }
        return url;
    }

    private String getValidationError(String key) {
        return validationErrorMap.get(key);
    }

    public void validateConnection(String hostType) throws ValidationException {
        String url = getURL(hostType);
        LOGGER.info(String.format("API Endpoint URL: %s ", url));
        try {
            Unirest.setHttpClient(this.getHttpClient());
            HttpResponse<String> response;
            String token;
            if (hostType.equals(ORG)) {
                token = BEARER + getToken();
            } else {
                token = BASIC + Base64Coder.encodeString(getUserName() + ":" + getPassword());
            }
            response = Unirest.get(url).header(AUTHORIZATION, token).asString();
            if (response.getStatus() != 200) {
                throw new ValidationException("Error while connecting " + url);
            }
        } catch (Exception e) {
            LOGGER.error(getValidationError(hostType), e);
            throw new ValidationException(getValidationError(hostType));
        }
    }

    private void validateDCHost() throws ValidationException {
        if (StringUtils.isEmpty(getDCHost()))
            throw new ValidationException(String.format(ERROR_EMPTY_URI, "Design Center"));
        try {
            new URI(getDCHost());
        } catch (URISyntaxException e) {
            throw new ValidationException(String.format(ERROR_INVALID_URI, getDCHost()));
        }
    }

    private void validateDCPort() throws ValidationException {
        if (StringUtils.isEmpty(getDCPort())) throw new ValidationException(ERROR_EMPTY_PORT);
        if (!getDCPort().matches(DIGIT_REGEX))
            throw new ValidationException(String.format(ERROR_INVALID_PORT, getDCPort()));
    }

    private void validateAPIHost() throws ValidationException {
        if (StringUtils.isEmpty(getAPIHost())) throw new ValidationException(String.format(ERROR_EMPTY_URI, "API"));
        try {
            new URI(getAPIHost());
        } catch (URISyntaxException e) {
            throw new ValidationException(String.format(ERROR_INVALID_URI, getAPIHost()));
        }
    }

    private void validateAPIPort() throws ValidationException {
        if (StringUtils.isEmpty(getAPIPort())) throw new ValidationException(ERROR_EMPTY_PORT);
        if (!getAPIPort().matches(DIGIT_REGEX))
            throw new ValidationException(String.format(ERROR_INVALID_PORT, getAPIPort()));
    }

    public void validateAuthHost() throws ValidationException {
        if (StringUtils.isEmpty(getAuthHost()))
            throw new ValidationException(String.format(ERROR_EMPTY_URI, "Authorization"));
        try {
            new URI(getAuthHost());
        } catch (URISyntaxException e) {
            throw new ValidationException(String.format(ERROR_INVALID_URI, getAuthHost()));
        }
    }

    public void validateAuthPort() throws ValidationException {
        if (StringUtils.isEmpty(getAuthPort())) throw new ValidationException(ERROR_EMPTY_PORT);
        if (!getAuthPort().matches(DIGIT_REGEX))
            throw new ValidationException(String.format(ERROR_INVALID_PORT, getAuthPort()));
    }

    public void validateOrganization() throws ValidationException {
        if (StringUtils.isEmpty(getOrganization())) throw new ValidationException(ERROR_EMPTY_ORGANIZATION);
    }

    public void validateUserName() throws ValidationException {
        if (StringUtils.isEmpty(getUserName())) throw new ValidationException(ERROR_EMPTY_USERNAME);
    }

    public void validatePassword() throws ValidationException {
        if (StringUtils.isEmpty(getUserName())) throw new ValidationException(ERROR_EMPTY_PASSWORD);
    }

    public String getDCHost() {
        return this.getTextParam(ATSCALE_SERVER_DC_HOST_NAME);
    }

    public String getOrganization() {
        return this.getTextParam(ATSCALE_ORGANIZATION);
    }

    public String getDCPort() {
        return this.getTextParam(ATSCALE_SERVER_DC_PORT);
    }

    public String getAPIHost() {
        return this.getTextParam(ATSCALE_SERVER_API_HOST_NAME);
    }

    public String getAPIPort() {
        return this.getTextParam(ATSCALE_SERVER_API_PORT);
    }

    public String getAuthHost() {
        return this.getTextParam(ATSCALE_SERVER_AUTH_HOST_NAME);
    }

    public String getAuthPort() {
        return this.getTextParam(ATSCALE_SERVER_AUTH_PORT);
    }

    public String getUserName() {
        return this.getTextParam(ATSCALE_SERVER_USERNAME);
    }

    public String getPassword() {
        return this.getEncryptedTextParam(ATSCALE_SERVER_PASSWORD); // getEncryptedTextParam(PASSWORD) errors
    }

    public byte[] getSSLCert() {
        return this.getEncryptedBinaryParam(SSL_CERT);
    }

    public String getSSLMode() {
        return this.getRadioParam(SSL_MODE);
    }

    public char[] getTrustorePassword() {
        if (this.truststorePassword == null) {
            this.truststorePassword = ("cert" + this.hashCode()).toCharArray();
        }
        return this.truststorePassword;
    }

    public HttpClient getHttpClient() throws KeyManagementException, KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {

        HttpClient httpClient;
        if (this.getSSLMode().equals(SSL_MODE_CERT)) {
            LOGGER.info("******* Leveraging Truststore");
            httpClient = HttpClients.custom()
                    .disableCookieManagement()
                    .setSSLContext(new SSLContextBuilder()
                            .loadTrustMaterial(this.getTruststore(), this.getTrustorePassword())
                            .build())
                    .build();
        } else if (this.getSSLMode().equals(SSL_MODE_TRUST)) {
            httpClient = HttpClients.custom()
                    .disableCookieManagement()
                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
                    .setSSLContext(new SSLContextBuilder()
                            .loadTrustMaterial(null, (x509Certificates, s) -> true)
                            .build())
                    .build();
        } else {
            httpClient = HttpClients.custom()
                    .disableCookieManagement()
                    .build();
        }
        return httpClient;
    }
}

