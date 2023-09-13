package atscale.biconnector.models;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Dataset {

    // Need database too but not yet populated in DMV query. Get from parsing for now.
    private String catalogName;
    private String cubeGUID;
    private String datasetName;
    private String table;
    private String schema;
    private String expression;
    private String connection;
    private String database;

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getCubeGUID() {
        return cubeGUID;
    }

    public void setCubeGUID(String cubeGUID) {
        this.cubeGUID = cubeGUID;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getConnection() {
        return connection;
    }

    public void setConnection(String connection) {
        this.connection = connection;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public Dataset(String catalogName, String datasetName) {
        this.catalogName = catalogName;
        this.datasetName = datasetName;
    }

    public Dataset(String catalogName, String datasetName, String schema, String table, String connection) {
        this.catalogName = catalogName;
        this.datasetName = datasetName;
        this.schema = schema;
        this.table = table;
        this.connection = connection;
    }

}
