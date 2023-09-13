package utils;

import alation.sdk.grpc.common.*;
import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.models.Dataset;
import atscale.biconnector.models.Dimension;
import com.google.protobuf.ByteString;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AtScaleBIUtils {

    public static final String ATSCALE_SERVER_DC_HOST_NAME = "test-dc-host";
    public static final String ATSCALE_SERVER_API_HOST_NAME = "test-engine-host";
    public static final String ATSCALE_SERVER_AUTH_HOST_NAME = "test-auth-host";
    public static final String ATSCALE_SERVER_DC_PORT = "10500";
    public static final String ATSCALE_SERVER_API_PORT = "10502";
    public static final String ATSCALE_SERVER_AUTH_PORT = "10500";
    public static final String ATSCALE_SERVER_USERNAME = "test_user";
    public static final String ATSCALE_SERVER_PASSWORD = "test_pass";

    public static ProtobufParameter createTextParam(String key, String value) {
        TextParam.Builder paramBuilder = TextParam.newBuilder();
        paramBuilder.setKey(key);
        paramBuilder.setValue(value);
        return ProtobufParameter.newBuilder().setText(paramBuilder).build();
    }

    public static ProtobufParameter createBooleanParam(String key, Boolean value) {
        BooleanParam.Builder paramBuilder = BooleanParam.newBuilder();
        paramBuilder.setKey(key);
        paramBuilder.setValue(value);
        return ProtobufParameter.newBuilder().setBoolean(paramBuilder).build();
    }

    public static ProtobufParameter createEncryptedTextParam(String key, String value) {
        EncryptedTextParam.Builder paramBuilder = EncryptedTextParam.newBuilder();
        paramBuilder.setKey(key);
        paramBuilder.setValue(value);
        return ProtobufParameter.newBuilder().setEncryptedText(paramBuilder).build();
    }

    public static ProtobufParameter createEncryptedBinaryParam(String key, byte[] value) {
        EncryptedBinaryParam.Builder paramBuilder = EncryptedBinaryParam.newBuilder();
        paramBuilder.setKey(key);
        paramBuilder.setValue(ByteString.copyFrom(value));
        return ProtobufParameter.newBuilder().setEncryptedBinary(paramBuilder).build();
    }

    public static AtScaleBIConfiguration createConfiguration(Map<String, ProtobufParameter> parameters) {
        ProtobufConfiguration.Builder builder = ProtobufConfiguration.newBuilder();
        builder.putAllParameters(parameters);
        return (AtScaleBIConfiguration) new AtScaleBIConfiguration().merge(builder.build());
    }

    public static AtScaleBIConfiguration createConfiguration() {
        Map<String, ProtobufParameter> params = new HashMap<>();
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_DC_HOST_NAME, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_DC_HOST_NAME, "test-dc-host"));
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_DC_PORT, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_DC_PORT, "10500"));
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_API_HOST_NAME, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_API_HOST_NAME, "test-engine-host"));
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_API_PORT, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_API_PORT, "10502"));
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_AUTH_HOST_NAME, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_AUTH_HOST_NAME, "test-auth-host"));
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_AUTH_PORT, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_AUTH_PORT, "10500"));
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_USERNAME, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_USERNAME, "test_user"));
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_PASSWORD, createEncryptedTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_PASSWORD, "test_pass")); // Was createEncryptedTextParam
        params.put(AtScaleBIConfiguration.ATSCALE_ORGANIZATION, createTextParam(AtScaleBIConfiguration.ATSCALE_ORGANIZATION, "default"));
        return createConfiguration(params);
    }

    public static AtScaleBIConfiguration createInvalidDCHostConfiguration() {
        Map<String, ProtobufParameter> params = new HashMap<>();
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_DC_HOST_NAME, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_DC_HOST_NAME, "http://<hostname_or_ip>"));
        return createConfiguration(params);
    }

    public static AtScaleBIConfiguration createInvalidDCPortConfiguration() {
        Map<String, ProtobufParameter> params = new HashMap<>();
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_DC_PORT, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_DC_PORT, "abcd"));
        return createConfiguration(params);
    }

    public static AtScaleBIConfiguration createInvalidAPIHostConfiguration() {
        Map<String, ProtobufParameter> params = new HashMap<>();
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_API_HOST_NAME, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_API_HOST_NAME, "http://<hostname_or_ip>"));
        return createConfiguration(params);
    }

    public static AtScaleBIConfiguration createInvalidAPIPortConfiguration() {
        Map<String, ProtobufParameter> params = new HashMap<>();
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_API_PORT, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_API_PORT, "abcd"));
        return createConfiguration(params);
    }

    public static AtScaleBIConfiguration createInvalidAUTHHostConfiguration() {
        Map<String, ProtobufParameter> params = new HashMap<>();
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_AUTH_HOST_NAME, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_AUTH_HOST_NAME, "http://<hostname_or_ip>"));
        return createConfiguration(params);
    }

    public static AtScaleBIConfiguration createInvalidAUTHPortConfiguration() {
        Map<String, ProtobufParameter> params = new HashMap<>();
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_AUTH_PORT, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_AUTH_PORT, "abcd"));
        return createConfiguration(params);
    }

    public static AtScaleBIConfiguration createInvalidUserNameConfiguration() {
        Map<String, ProtobufParameter> params = new HashMap<>();
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_USERNAME, createTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_USERNAME, ""));
        return createConfiguration(params);
    }

    public static AtScaleBIConfiguration createInvalidPasswordConfiguration() {
        Map<String, ProtobufParameter> params = new HashMap<>();
        params.put(AtScaleBIConfiguration.ATSCALE_SERVER_PASSWORD, createEncryptedTextParam(AtScaleBIConfiguration.ATSCALE_SERVER_PASSWORD, ""));
        return createConfiguration(params);
    }

    public static Dataset getDataSets() {
        Dataset dataset = new Dataset();
        dataset.setDatasetName("Dataset1");
        dataset.setCatalogName("Catalog1");
        dataset.setCubeGUID("CubeGuid1");
        dataset.setDatabase("Database1");
        dataset.setTable("Table1");
        dataset.setSchema("Schema1");
        dataset.setExpression("Expression1");
        dataset.setConnection("ConnectionId1");
        return dataset;
    }

    public static Set<Dimension> getDimensions() {

        Set<Dimension> dimensions = new HashSet<>();
        Dimension dimension = new Dimension();
        dimension.setRowId(1);
        dimension.setTableNumber(2);
        dimension.setCatalogName("Catalog1");
        dimension.setCatalogName("Catalog1");
        dimension.setSchemaName("Schema1");
        dimension.setCubeName("CubeName1");
        dimension.setCubeGUID("CubeGuid1");
        dimension.setDimensionName("DimensionName1");
        dimension.setDimensionUniqueName("DimensionUniqueName1");
        dimension.setDimensionGUID("DimensionGuid1");
        dimension.setDimensionCaption("DimensionCaption1");
        dimension.setDimensionOrdinal(1);
        dimension.setDimensionCardinality(3);
        dimension.setDefaultHierarchy("DefaultHierarchy1");
        dimension.setDescription("Description1");
        dimension.setDimensionUniqueSettings(4);
        dimension.setDimensionMasterName("DimensionMasterName1");
        dimension.setSourceDBServerName("SourceDBServerName1");
        dimension.setSourceDBInstanceName("SourceDBInstanceName1");
        dimensions.add(dimension);
        return dimensions;
    }
}
