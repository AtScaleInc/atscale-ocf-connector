package atscale.biconnector.models;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Dimension {

    private int tableNumber;
    private int rowId;
    private String importDate;
    private String catalogName;
    private String schemaName;
    private String cubeName;
    private String cubeGUID;
    private String dimensionName;
    private String dimensionUniqueName;
    private String dimensionGUID;
    private String dimensionCaption;
    private int dimensionOrdinal;
    private int type;
    private int dimensionCardinality;
    private String defaultHierarchy;
    private String description;
    private boolean isVirtual;
    private boolean isReadWrite;
    private boolean isVisible;
    private int dimensionUniqueSettings;
    private String dimensionMasterName;
    private String sourceDBServerName;
    private String sourceDBInstanceName;
    private int sourceDBID;

    public int getTableNumber() {
        return tableNumber;
    }

    public void setTableNumber(int tableNumber) {
        this.tableNumber = tableNumber;
    }

    public int getRowId() {
        return rowId;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
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

    public String getCubeGUID() {
        return cubeGUID;
    }

    public void setCubeGUID(String cubeGUID) {
        this.cubeGUID = cubeGUID;
    }

    public String getDimensionName() {
        return dimensionName;
    }

    public void setDimensionName(String dimensionName) {
        this.dimensionName = dimensionName;
    }

    public String getDimensionUniqueName() {
        return dimensionUniqueName;
    }

    public void setDimensionUniqueName(String dimensionUniqueName) {
        this.dimensionUniqueName = dimensionUniqueName;
    }

    public String getDimensionGUID() {
        return dimensionGUID;
    }

    public void setDimensionGUID(String dimensionGUID) {
        this.dimensionGUID = dimensionGUID;
    }

    public String getDimensionCaption() {
        return dimensionCaption;
    }

    public void setDimensionCaption(String dimensionCaption) {
        this.dimensionCaption = dimensionCaption;
    }

    public int getDimensionOrdinal() {
        return dimensionOrdinal;
    }

    public void setDimensionOrdinal(int dimensionOrdinal) {
        this.dimensionOrdinal = dimensionOrdinal;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getDimensionCardinality() {
        return dimensionCardinality;
    }

    public void setDimensionCardinality(int dimensionCardinality) {
        this.dimensionCardinality = dimensionCardinality;
    }

    public String getDefaultHierarchy() {
        return defaultHierarchy;
    }

    public void setDefaultHierarchy(String defaultHierarchy) {
        this.defaultHierarchy = defaultHierarchy;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isVirtual() {
        return isVirtual;
    }

    public void setVirtual(boolean virtual) {
        isVirtual = virtual;
    }

    public boolean isReadWrite() {
        return isReadWrite;
    }

    public void setReadWrite(boolean readWrite) {
        isReadWrite = readWrite;
    }

    public boolean isVisible() {
        return isVisible;
    }

    public void setVisible(boolean visible) {
        isVisible = visible;
    }

    public int getDimensionUniqueSettings() {
        return dimensionUniqueSettings;
    }

    public void setDimensionUniqueSettings(int dimensionUniqueSettings) {
        this.dimensionUniqueSettings = dimensionUniqueSettings;
    }

    public String getDimensionMasterName() {
        return dimensionMasterName;
    }

    public void setDimensionMasterName(String dimensionMasterName) {
        this.dimensionMasterName = dimensionMasterName;
    }

    public String getSourceDBServerName() {
        return sourceDBServerName;
    }

    public void setSourceDBServerName(String sourceDBServerName) {
        this.sourceDBServerName = sourceDBServerName;
    }

    public String getSourceDBInstanceName() {
        return sourceDBInstanceName;
    }

    public void setSourceDBInstanceName(String sourceDBInstanceName) {
        this.sourceDBInstanceName = sourceDBInstanceName;
    }

    public int getSourceDBID() {
        return sourceDBID;
    }

    public void setSourceDBID(int sourceDBID) {
        this.sourceDBID = sourceDBID;
    }
}
