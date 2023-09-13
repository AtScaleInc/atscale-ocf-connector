package atscale.biconnector.datasource;

import alation.sdk.bi.grpc.mde.AlationUser;
import alation.sdk.bi.grpc.mde.BIObjectResponse;
import alation.sdk.bi.mde.models.BIObject;
import alation.sdk.bi.mde.models.Folder;
import alation.sdk.bi.mde.streams.MetadataStream;
import alation.sdk.core.error.ConnectorException;
import alation.sdk.core.error.ValidationException;
import alation.sdk.core.request.auth.Auth;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.utils.ModelUtils;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import org.junit.Before;
import org.junit.Test;
import utils.AtScaleBIUtils;

import java.util.ArrayList;
import java.util.List;

public class AtScaleBIDatasourceTest {

    AtScaleBIDatasource datasource;
    AtScaleBIConfiguration configuration;
    AtScaleBIConfiguration invalidDCHostConfiguration;
    AtScaleBIConfiguration invalidDCPortConfiguration;
    AtScaleBIConfiguration invalidAPIHostConfiguration;
    AtScaleBIConfiguration invalidAPIPortConfiguration;
    AtScaleBIConfiguration invalidAUTHHostConfiguration;
    AtScaleBIConfiguration invalidAUTHPortConfiguration;
    AtScaleBIConfiguration invalidUserNameConfiguration;
    AtScaleBIConfiguration invalidPasswordConfiguration;
    CallStreamObserver<BIObjectResponse> responseObserver;
    MetadataStream stream;
    List<AlationUser> alationUsers;
    List<Folder> folders;
    BIObject biObject;

    @Before
    public void setUp() {
        configuration = AtScaleBIUtils.createConfiguration();
        invalidDCHostConfiguration = AtScaleBIUtils.createInvalidDCHostConfiguration();
        invalidDCPortConfiguration = AtScaleBIUtils.createInvalidDCPortConfiguration();
        invalidAPIHostConfiguration = AtScaleBIUtils.createInvalidAPIHostConfiguration();
        invalidAPIPortConfiguration = AtScaleBIUtils.createInvalidAPIPortConfiguration();
        invalidAUTHHostConfiguration = AtScaleBIUtils.createInvalidAUTHHostConfiguration();
        invalidAUTHPortConfiguration = AtScaleBIUtils.createInvalidAUTHPortConfiguration();
        invalidUserNameConfiguration = AtScaleBIUtils.createInvalidUserNameConfiguration();
        invalidPasswordConfiguration = AtScaleBIUtils.createInvalidPasswordConfiguration();

        datasource = new AtScaleBIDatasource();
        responseObserver =
                new ServerCallStreamObserver<>() {
                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public void setOnCancelHandler(Runnable runnable) {
                    }

                    @Override
                    public void setCompression(String s) {
                    }

                    @Override
                    public boolean isReady() {
                        return true;
                    }

                    @Override
                    public void setOnReadyHandler(Runnable runnable) {
                    }

                    @Override
                    public void disableAutoInboundFlowControl() {
                    }

                    @Override
                    public void request(int i) {
                    }

                    @Override
                    public void setMessageCompression(boolean b) {
                    }

                    @Override
                    public void onNext(BIObjectResponse biObjectResponse) {
                    }

                    @Override
                    public void onError(Throwable throwable) {
                    }

                    @Override
                    public void onCompleted() {
                    }
                };
        stream = new MetadataStream(responseObserver, configuration.getMessageSize());

        alationUsers = new ArrayList<>();
        alationUsers.add(AlationUser.newBuilder().setUsername("test_user").build()); // TestUser

        folders = new ArrayList<>();
        folders.add(ModelUtils.createFolder("Sales Insights", "Sales Insights"));

        biObject = new BIObject("Sales Insights.", "Sales Insights", "Folder");

    }

    @Test(expected = ValidationException.class)
    public void testValidConfiguration() throws Exception {
        datasource.validate(configuration);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidDCHostConfiguration() throws Exception {
        datasource.validate(invalidDCHostConfiguration);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidDCPortConfiguration() throws Exception {
        datasource.validate(invalidDCPortConfiguration);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAPIHostConfiguration() throws Exception {
        datasource.validate(invalidAPIHostConfiguration);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAPIPortConfiguration() throws Exception {
        datasource.validate(invalidAPIPortConfiguration);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAUTHHostConfiguration() throws Exception {
        datasource.validate(invalidAUTHHostConfiguration);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAUTHPortConfiguration() throws Exception {
        datasource.validate(invalidAUTHPortConfiguration);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidUserNameConfiguration() throws Exception {
        datasource.validate(invalidUserNameConfiguration);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidPasswordConfiguration() throws Exception {
        datasource.validate(invalidPasswordConfiguration);
    }

    @Test
    public void testSuccessfulListFileSystem() throws Exception {
        datasource.listFileSystem(configuration, stream, alationUsers);
    }

    @Test
    public void testSuccessfulExtraction() throws Exception {
        datasource.metadataExtraction(configuration, folders, true, stream, alationUsers);
    }

    @Test
    public void testSuccessfulCertification() throws Exception {
        datasource.certify(biObject, configuration, "Test Note", stream);
    }

    @Test
    public void testSuccessfulDecertification() throws Exception {
        datasource.decertify(biObject, configuration, "Test Note", stream);
    }

    @Test
    public void testGetUserConfiguration() {
        datasource.getUserConfiguration();
    }

    @Test(expected = ValidationException.class)
    public void testValidate() throws ConnectorException {
        datasource.validate(new AtScaleBIConfiguration(), new Auth());
    }

    @Test
    public void testGetConnectorVersion() {
        datasource.getConnectorVersion();
    }

    @Test
    public void testGetDescription() {
        datasource.getDescription();
    }
}
