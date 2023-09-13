package atscale.biconnector.models;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MeasureGroup {

    private int tableNumber;
    private int rowId;
    private String importDate;
    private String catalogName;
    private String schemaName;
    private String cubeName;
    private String measureGroupName;
    private String description;
    private boolean isWriteEnabled;
    private String measureGroupCaption;
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

    public String getMeasureGroupName() {
        return measureGroupName;
    }

    public void setMeasureGroupName(String measureGroupName) {
        this.measureGroupName = measureGroupName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isWriteEnabled() {
        return isWriteEnabled;
    }

    public void setWriteEnabled(boolean writeEnabled) {
        isWriteEnabled = writeEnabled;
    }

    public String getMeasureGroupCaption() {
        return measureGroupCaption;
    }

    public void setMeasureGroupCaption(String measureGroupCaption) {
        this.measureGroupCaption = measureGroupCaption;
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
