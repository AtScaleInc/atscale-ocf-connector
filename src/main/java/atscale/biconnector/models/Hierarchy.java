package atscale.biconnector.models;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Hierarchy {

    private int tableNumber;
    private String importDate;
    private String catalogName;
    private int rowId;
    private String schemaName;
    private String cubeName;
    private String cubeGUID;
    private String dimensionUniqueName;
    private String hierarchyName;
    private String hierarchyUniqueName;
    private String hierarchyGUID;
    private String hierarchyCaption;
    private int dimensionType;
    private int hierarchyCardinality;
    private String defaultMember;
    private String allMember;
    private String description;
    private int structure;
    private boolean isVirtual;
    private boolean isReadWrite;
    private boolean dimensionIsVisible;
    private int dimensionUniqueSettings;
    private String dimensionMasterName;
    private int hierarchyOrigin;
    private String hierarchyDisplayFolder;
    private int instanceSelection;
    private int groupingBehaviour;
    private String structureType;
    private boolean dimensionIsShared;
    private boolean hierarchyIsVisible;
    private int hierarchyOrdinal;

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

    public int getRowId() {
        return rowId;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
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

    public String getDimensionUniqueName() {
        return dimensionUniqueName;
    }

    public void setDimensionUniqueName(String dimensionUniqueName) {
        this.dimensionUniqueName = dimensionUniqueName;
    }

    public String getHierarchyName() {
        return hierarchyName;
    }

    public void setHierarchyName(String hierarchyName) {
        this.hierarchyName = hierarchyName;
    }

    public String getHierarchyUniqueName() {
        return hierarchyUniqueName;
    }

    public void setHierarchyUniqueName(String hierarchyUniqueName) {
        this.hierarchyUniqueName = hierarchyUniqueName;
    }

    public String getHierarchyGUID() {
        return hierarchyGUID;
    }

    public void setHierarchyGUID(String hierarchyGUID) {
        this.hierarchyGUID = hierarchyGUID;
    }

    public String getHierarchyCaption() {
        return hierarchyCaption;
    }

    public void setHierarchyCaption(String hierarchyCaption) {
        this.hierarchyCaption = hierarchyCaption;
    }

    public int getDimensionType() {
        return dimensionType;
    }

    public void setDimensionType(int dimensionType) {
        this.dimensionType = dimensionType;
    }

    public int getHierarchyCardinality() {
        return hierarchyCardinality;
    }

    public void setHierarchyCardinality(int hierarchyCardinality) {
        this.hierarchyCardinality = hierarchyCardinality;
    }

    public String getDefaultMember() {
        return defaultMember;
    }

    public void setDefaultMember(String defaultMember) {
        this.defaultMember = defaultMember;
    }

    public String getAllMember() {
        return allMember;
    }

    public void setAllMember(String allMember) {
        this.allMember = allMember;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getStructure() {
        return structure;
    }

    public void setStructure(int structure) {
        this.structure = structure;
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

    public boolean isDimensionIsVisible() {
        return dimensionIsVisible;
    }

    public void setDimensionIsVisible(boolean dimensionIsVisible) {
        this.dimensionIsVisible = dimensionIsVisible;
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

    public int getHierarchyOrigin() {
        return hierarchyOrigin;
    }

    public void setHierarchyOrigin(int hierarchyOrigin) {
        this.hierarchyOrigin = hierarchyOrigin;
    }

    public String getHierarchyDisplayFolder() {
        return hierarchyDisplayFolder;
    }

    public void setHierarchyDisplayFolder(String hierarchyDisplayFolder) {
        this.hierarchyDisplayFolder = hierarchyDisplayFolder;
    }

    public int getInstanceSelection() {
        return instanceSelection;
    }

    public void setInstanceSelection(int instanceSelection) {
        this.instanceSelection = instanceSelection;
    }

    public int getGroupingBehaviour() {
        return groupingBehaviour;
    }

    public void setGroupingBehaviour(int groupingBehaviour) {
        this.groupingBehaviour = groupingBehaviour;
    }

    public String getStructureType() {
        return structureType;
    }

    public void setStructureType(String structureType) {
        this.structureType = structureType;
    }

    public boolean isDimensionIsShared() {
        return dimensionIsShared;
    }

    public void setDimensionIsShared(boolean dimensionIsShared) {
        this.dimensionIsShared = dimensionIsShared;
    }

    public boolean isHierarchyIsVisible() {
        return hierarchyIsVisible;
    }

    public void setHierarchyIsVisible(boolean hierarchyIsVisible) {
        this.hierarchyIsVisible = hierarchyIsVisible;
    }

    public int getHierarchyOrdinal() {
        return hierarchyOrdinal;
    }

    public void setHierarchyOrdinal(int hierarchyOrdinal) {
        this.hierarchyOrdinal = hierarchyOrdinal;
    }
}
