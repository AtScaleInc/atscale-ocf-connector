package atscale.biconnector.configuration;

import alation.sdk.bi.configuration.BIApplicationConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * AtScaleBIAppConfig determines Application configuration for the sample BI datasource. There are no
 * configurations to override, and this class makes the abstract super class concrete.
 */
public class AtScaleBIAppConfig extends BIApplicationConfiguration {
    @Override
    public List<String> certifiableObjectTypes() {
        List<String> objectTypes = new ArrayList<>();
        objectTypes.add("Folder");
        objectTypes.add("Connection");
        objectTypes.add("ConnectionColumn");
        objectTypes.add("Datasource");
        objectTypes.add("DatasourceColumn");
        objectTypes.add("Report");
        objectTypes.add("ReportColumn");
        objectTypes.add("User");
        // Other object types that exist: BIObject, Image, Permission
        return objectTypes;
    }

    @Override
    public Map<String, String> nameConfiguration() {
        Map<String, String> nameConfigMap = new TreeMap<>();
        nameConfigMap.put("bi_datasource", "DataSources");
        nameConfigMap.put("bi_folder", "Projects");
        nameConfigMap.put("bi_report", "Dimensions");
        return nameConfigMap;
    }

    @Override
    public String extractableBIObjectType() {
        return "Projects";
    }

}
