package atscale.biconnector.configuration;

import alation.sdk.core.error.ValidationException;
import alation.sdk.core.manifest.parameter.Parameter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import utils.AtScaleBIUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class AtScaleBIConfigurationTest {
    AtScaleBIConfiguration configuration;

    @Before
    public void setUp() {
        configuration = AtScaleBIUtils.createConfiguration();
    }

    @Test
    public void testConfigurationKeys() {
        Collection<Parameter> parameterList = configuration.buildParameters();
        List<String> actualKeys =
                parameterList.stream().map(param -> param.getKey()).collect(Collectors.toList());
        List<String> expectedKeys =
                Arrays.asList(
                        AtScaleBIConfiguration.ATSCALE_SERVER_DC_HOST_NAME, AtScaleBIConfiguration.ATSCALE_SERVER_DC_PORT,
                        AtScaleBIConfiguration.ATSCALE_SERVER_API_HOST_NAME, AtScaleBIConfiguration.ATSCALE_SERVER_API_PORT,
                        AtScaleBIConfiguration.ATSCALE_SERVER_AUTH_HOST_NAME, AtScaleBIConfiguration.ATSCALE_SERVER_AUTH_PORT,
                        AtScaleBIConfiguration.ATSCALE_ORGANIZATION, AtScaleBIConfiguration.ATSCALE_SERVER_USERNAME,
                        AtScaleBIConfiguration.ATSCALE_SERVER_PASSWORD, AtScaleBIConfiguration.SSL_MODE,
                        AtScaleBIConfiguration.SSL_CERT);

        assertEquals(expectedKeys, actualKeys);
    }

    @Test
    public void testGetDCHost() {
        assertEquals(AtScaleBIUtils.ATSCALE_SERVER_DC_HOST_NAME, configuration.getDCHost());
    }

    @Test
    public void testGetDCPort() {
        assertEquals(AtScaleBIUtils.ATSCALE_SERVER_DC_PORT, configuration.getDCPort());
    }

    @Test
    public void testGetAPIHost() {
        assertEquals(AtScaleBIUtils.ATSCALE_SERVER_API_HOST_NAME, configuration.getAPIHost());
    }

    @Test
    public void testGetAPIPort() {
        assertEquals(AtScaleBIUtils.ATSCALE_SERVER_API_PORT, configuration.getAPIPort());
    }

    @Test
    public void testGetAUTHHost() {
        assertEquals(AtScaleBIUtils.ATSCALE_SERVER_AUTH_HOST_NAME, configuration.getAuthHost());
    }

    @Test
    public void testGetAUTHPort() {
        assertEquals(AtScaleBIUtils.ATSCALE_SERVER_AUTH_PORT, configuration.getAuthPort());
    }

    @Test
    public void testGetUsername() {
        assertEquals(AtScaleBIUtils.ATSCALE_SERVER_USERNAME, configuration.getUserName());
    }

    @Test
    public void testPassword() {
        assertEquals(AtScaleBIUtils.ATSCALE_SERVER_PASSWORD, configuration.getPassword());
    }

    @Test(expected = ValidationException.class)
    public void testValidateConnection_InvalidHost() throws ValidationException {
        configuration.validateConnection("INVALID_HOST_TYPE");
    }

    @Test
    public void testValidatePassword() throws ValidationException {
        configuration.validatePassword();
    }

    @Test
    public void testValidateUserName() throws ValidationException {
        configuration.validateUserName();
    }

    @Test
    public void testValidateOrganization() throws ValidationException {
        configuration.validateOrganization();
    }

    @Test
    public void testGetTrustorePassword() throws ValidationException {
        configuration.getTrustorePassword();
    }

    @Test
    public void testValidateAuthPort() throws ValidationException {
        configuration.validateAuthPort();
    }

    @Test
    public void testValidateAuthHost() throws ValidationException {
        configuration.validateAuthHost();
    }

    @Test(expected = ValidationException.class)
    public void testValidateOrg() throws ValidationException {
        configuration.validateOrg();
    }

}
