package atscale.biconnector.models;

import atscale.biconnector.utils.Tools;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;

@Data
@NoArgsConstructor
public class Measure {

    private int tableNumber;
    private String importDate;
    private String catalogName;
    private String schemaName;
    private String cubeName;
    private String cubeGUID;
    private String measureName;
    private String measureUniqueName;
    private String measureGUID;
    private int rowId;
    private String measureCaption;
    private int measureAggregator;
    private String dataType; // For the measure
    private String columnDataType;
    private String columnName;
    private String columnSQL;
    private int numericPrecision;
    private int numericScale;
    private int measureUnits;
    private String description;
    private String expression;
    private boolean isVisible;
    private String levelList;
    private String measureNameSQLColumnName;
    private String measureUnqualifiedCaption;
    private String measureGroupName;
    private String measureDisplayFolder;
    private String defaultFormatString;
    private String datasetName;
    private Dataset dataset;
    private boolean isMetricalAttribute;
    private String parentLevelId; // populated for metrical attributes in measures
    private String parentLevelName;

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

    public String getCubeGUID() {
        return cubeGUID;
    }

    public void setCubeGUID(String cubeGUID) {
        this.cubeGUID = cubeGUID;
    }

    public String getMeasureName() {
        return measureName;
    }

    public void setMeasureName(String measureName) {
        this.measureName = measureName;
    }

    public String getMeasureUniqueName() {
        return measureUniqueName;
    }

    public void setMeasureUniqueName(String measureUniqueName) {
        this.measureUniqueName = measureUniqueName;
    }

    public String getMeasureGUID() {
        return measureGUID;
    }

    public void setMeasureGUID(String measureGUID) {
        this.measureGUID = measureGUID;
    }

    public int getRowId() {
        return rowId;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    public String getMeasureCaption() {
        return measureCaption;
    }

    public void setMeasureCaption(String measureCaption) {
        this.measureCaption = measureCaption;
    }

    public int getMeasureAggregator() {
        return measureAggregator;
    }

    public void setMeasureAggregator(int measureAggregator) {
        this.measureAggregator = measureAggregator;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getColumnDataType() {
        return columnDataType;
    }

    public void setColumnDataType(String columnDataType) {
        this.columnDataType = columnDataType;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnSQL() {
        return columnSQL;
    }

    public void setColumnSQL(String columnSQL) {
        this.columnSQL = columnSQL;
    }

    public int getNumericPrecision() {
        return numericPrecision;
    }

    public void setNumericPrecision(int numericPrecision) {
        this.numericPrecision = numericPrecision;
    }

    public int getNumericScale() {
        return numericScale;
    }

    public void setNumericScale(int numericScale) {
        this.numericScale = numericScale;
    }

    public int getMeasureUnits() {
        return measureUnits;
    }

    public void setMeasureUnits(int measureUnits) {
        this.measureUnits = measureUnits;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public boolean isVisible() {
        return isVisible;
    }

    public void setVisible(boolean visible) {
        isVisible = visible;
    }

    public String getLevelList() {
        return levelList;
    }

    public void setLevelList(String levelList) {
        this.levelList = levelList;
    }

    public String getMeasureNameSQLColumnName() {
        return measureNameSQLColumnName;
    }

    public void setMeasureNameSQLColumnName(String measureNameSQLColumnName) {
        this.measureNameSQLColumnName = measureNameSQLColumnName;
    }

    public String getMeasureUnqualifiedCaption() {
        return measureUnqualifiedCaption;
    }

    public void setMeasureUnqualifiedCaption(String measureUnqualifiedCaption) {
        this.measureUnqualifiedCaption = measureUnqualifiedCaption;
    }

    public String getMeasureGroupName() {
        return measureGroupName;
    }

    public void setMeasureGroupName(String measureGroupName) {
        this.measureGroupName = measureGroupName;
    }

    public String getMeasureDisplayFolder() {
        return measureDisplayFolder;
    }

    public void setMeasureDisplayFolder(String measureDisplayFolder) {
        this.measureDisplayFolder = measureDisplayFolder;
    }

    public String getDefaultFormatString() {
        return defaultFormatString;
    }

    public void setDefaultFormatString(String defaultFormatString) {
        this.defaultFormatString = defaultFormatString;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public boolean isMetricalAttribute() {
        return isMetricalAttribute;
    }

    public void setMetricalAttribute(boolean metricalAttribute) {
        isMetricalAttribute = metricalAttribute;
    }

    public String getParentLevelId() {
        return parentLevelId;
    }

    public void setParentLevelId(String parentLevelId) {
        this.parentLevelId = parentLevelId;
    }

    public String getParentLevelName() {
        return parentLevelName;
    }

    public void setParentLevelName(String parentLevelName) {
        this.parentLevelName = parentLevelName;
    }

    private static final Logger LOGGER = Logger.getLogger(Measure.class);


    public Dataset getDataset(List<Dataset> datasets) {
        for (Dataset datasetObj : datasets) {
            if (datasetObj.getDatasetName().equals(datasetName)) {
                return datasetObj;
            }
        }
        LOGGER.warn("No dataset mapped with name '" + getDatasetName() + "' in project '" + getCatalogName() + "'");
        return null;
    }

    // Only adds main column at present. Uses a set for deduplication in case other column references are added later.
    public List<String> getSourceColumnIDs(Dataset dataset) {
        Set<String> colSet = new HashSet<>();
//    colSet.add(StringUtils.joinWith("/", dataset.getDatabase(), dataset.getConnection(), dataset.getTable(), this.columnName)); // this.measureNameSQLColumnName
        colSet = Tools.addToSetFirstEltOptional(colSet, "/", dataset.getDatabase(), StringUtils.joinWith("/", dataset.getSchema(), dataset.getTable(), this.columnName));
        List<String> colList = new ArrayList<>(colSet);
        return !colList.isEmpty() ? colList : Collections.emptyList();
    }
}
