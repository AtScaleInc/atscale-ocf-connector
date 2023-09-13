package atscale.biconnector.models;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Catalog {

    private int rowId;
    private int tableNumber;
    private String importDate;
    private String name;
    private String catalogGUID;
    private String description;
    private String role;
    private String lastModified;
    private int compatibilityLevel;
    private int type;
    private int version;
    private String databaseId;
    private String dateQueried;
    private boolean isCurrentlyUsed;
    private int popularity;


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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCatalogGUID() {
        return catalogGUID;
    }

    public void setCatalogGUID(String catalogGUID) {
        this.catalogGUID = catalogGUID;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getLastModified() {
        return lastModified;
    }

    public void setLastModified(String lastModified) {
        this.lastModified = lastModified;
    }

    public int getCompatibilityLevel() {
        return compatibilityLevel;
    }

    public void setCompatibilityLevel(int compatibilityLevel) {
        this.compatibilityLevel = compatibilityLevel;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(String databaseId) {
        this.databaseId = databaseId;
    }

    public String getDateQueried() {
        return dateQueried;
    }

    public void setDateQueried(String dateQueried) {
        this.dateQueried = dateQueried;
    }

    public boolean isCurrentlyUsed() {
        return isCurrentlyUsed;
    }

    public void setCurrentlyUsed(boolean currentlyUsed) {
        isCurrentlyUsed = currentlyUsed;
    }

    public int getPopularity() {
        return popularity;
    }

    public void setPopularity(int popularity) {
        this.popularity = popularity;
    }
}
