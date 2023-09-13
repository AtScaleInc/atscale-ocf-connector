package atscale.biconnector.models;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;
import java.util.Set;

@Data
@NoArgsConstructor
public class Column {
    private String catalogName;
    private String datasetName;
    private String columnName;
    private String dataType;
    private String expression;
    private String connectionID;
    private Dataset dataset;

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getConnectionID() {
        return connectionID;
    }

    public void setConnectionID(String connectionID) {
        this.connectionID = connectionID;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, columnName, connectionID, datasetName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Column other = (Column) obj;
        return Objects.equals(catalogName, other.catalogName) && Objects.equals(columnName, other.columnName)
                && Objects.equals(connectionID, other.connectionID) && Objects.equals(datasetName, other.datasetName);
    }

    public Dataset getDataset(Set<Dataset> datasets) {
        for (Dataset datasetObj : datasets) {
            if (datasetObj.getDatasetName().equals(datasetName)) {
                return datasetObj;
            }
        }
        return null;
    }
}
