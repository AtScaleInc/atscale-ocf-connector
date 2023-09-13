package atscale.biconnector.configuration;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

import java.util.Map;

public class AtScaleBIAppConfigTest {

    @Test
    public void testExtractableBIObjectType() {
        AtScaleBIAppConfig config = new AtScaleBIAppConfig();
        String extractableObjectType = config.extractableBIObjectType();
        assertEquals("Projects", extractableObjectType);
    }

    @Test
    public void testNameConfiguration() {
        AtScaleBIAppConfig config = new AtScaleBIAppConfig();
        Map<String, String> nameConfigMap = config.nameConfiguration();
        assertEquals(3, nameConfigMap.size());
        assertEquals("DataSources", nameConfigMap.get("bi_datasource"));
        assertEquals("Projects", nameConfigMap.get("bi_folder"));
        assertEquals("Dimensions", nameConfigMap.get("bi_report"));
    }

    @Test
    public void testCertifiableObjectTypes() {
        AtScaleBIAppConfig config = new AtScaleBIAppConfig();
        List<String> objectTypes = config.certifiableObjectTypes();
        assertEquals(8, objectTypes.size());
        assertEquals("Folder", objectTypes.get(0));
        assertEquals("Connection", objectTypes.get(1));
        assertEquals("ConnectionColumn", objectTypes.get(2));
        assertEquals("Datasource", objectTypes.get(3));
        assertEquals("DatasourceColumn", objectTypes.get(4));
        assertEquals("Report", objectTypes.get(5));
        assertEquals("ReportColumn", objectTypes.get(6));
        assertEquals("User", objectTypes.get(7));
    }
}
