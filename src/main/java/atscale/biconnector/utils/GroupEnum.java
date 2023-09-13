package atscale.biconnector.utils;

/**
 * An enum to group the different BI configuration together. When two BIConfigurationEnum belong to
 * same group, they will be shown in the same section under the UI.
 */
public enum GroupEnum {
    GROUP_ALATION("Application Settings"),
    GROUP_SERVER_CONNECTION("Server Connection"),
    GROUP_POSTGRES_CONNECTION("PostgreSQL Connection"),
    GROUP_ADDITIONAL_SETTINGS("Additional Settings");

    private final String groupName;

    GroupEnum(String groupName) {
        this.groupName = groupName;
    }

    public String getGroupName() {
        return groupName;
    }
}
