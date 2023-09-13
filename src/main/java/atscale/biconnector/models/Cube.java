package atscale.biconnector.models;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Cube {

    private int rowId;
    private int tableNumber;
    private String importDate;
    private String catalogName;
    private String schemaName;
    private String cubeName;
    private String cubeType;
    private String guid;
    private String createdOn;
    private String lastSchemaUpdate;
    private String lastSchemaUpdatedBy;
    private String schemaUpdatedBy;
    private String lastDataUpdated;
    private String dataUpdatedBy;
    private String description;
    private String cubeCaption;
    private String baseCubeName;
    private int cubeSource;
    private String preferredQueryPatterns;
    private boolean isDrillThroughEnabled;
    private boolean isLinkable;
    private boolean isWriteEnabled;
    private boolean isSQLEnabled;

    public int getRowId() {
        return rowId;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    public int getTableNumber() {
        return tableNumber;
    }

    public void setTableNumber(int tableNumber) {
        this.tableNumber = tableNumber;
    }

    public String getImportDate() {
        return importDate;
    }

    public void setImportDate(String importDate) {
        this.importDate = importDate;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public String getCubeType() {
        return cubeType;
    }

    public void setCubeType(String cubeType) {
        this.cubeType = cubeType;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getCreatedOn() {
        return createdOn;
    }

    public void setCreatedOn(String createdOn) {
        this.createdOn = createdOn;
    }

    public String getLastSchemaUpdate() {
        return lastSchemaUpdate;
    }

    public void setLastSchemaUpdate(String lastSchemaUpdate) {
        this.lastSchemaUpdate = lastSchemaUpdate;
    }

    public String getLastSchemaUpdatedBy() {
        return lastSchemaUpdatedBy;
    }

    public void setLastSchemaUpdatedBy(String lastSchemaUpdatedBy) {
        this.lastSchemaUpdatedBy = lastSchemaUpdatedBy;
    }

    public String getSchemaUpdatedBy() {
        return schemaUpdatedBy;
    }

    public void setSchemaUpdatedBy(String schemaUpdatedBy) {
        this.schemaUpdatedBy = schemaUpdatedBy;
    }

    public String getLastDataUpdated() {
        return lastDataUpdated;
    }

    public void setLastDataUpdated(String lastDataUpdated) {
        this.lastDataUpdated = lastDataUpdated;
    }

    public String getDataUpdatedBy() {
        return dataUpdatedBy;
    }

    public void setDataUpdatedBy(String dataUpdatedBy) {
        this.dataUpdatedBy = dataUpdatedBy;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCubeCaption() {
        return cubeCaption;
    }

    public void setCubeCaption(String cubeCaption) {
        this.cubeCaption = cubeCaption;
    }

    public String getBaseCubeName() {
        return baseCubeName;
    }

    public void setBaseCubeName(String baseCubeName) {
        this.baseCubeName = baseCubeName;
    }

    public int getCubeSource() {
        return cubeSource;
    }

    public void setCubeSource(int cubeSource) {
        this.cubeSource = cubeSource;
    }

    public String getPreferredQueryPatterns() {
        return preferredQueryPatterns;
    }

    public void setPreferredQueryPatterns(String preferredQueryPatterns) {
        this.preferredQueryPatterns = preferredQueryPatterns;
    }

    public boolean isDrillThroughEnabled() {
        return isDrillThroughEnabled;
    }

    public void setDrillThroughEnabled(boolean drillThroughEnabled) {
        isDrillThroughEnabled = drillThroughEnabled;
    }

    public boolean isLinkable() {
        return isLinkable;
    }

    public void setLinkable(boolean linkable) {
        isLinkable = linkable;
    }

    public boolean isWriteEnabled() {
        return isWriteEnabled;
    }

    public void setWriteEnabled(boolean writeEnabled) {
        isWriteEnabled = writeEnabled;
    }

    public boolean isSQLEnabled() {
        return isSQLEnabled;
    }

    public void setSQLEnabled(boolean sqlEnabled) {
        isSQLEnabled = sqlEnabled;
    }
}
