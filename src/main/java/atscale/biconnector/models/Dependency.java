package atscale.biconnector.models;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Dependency {
    private String databaseName;
    private String objectType;
    private String table;
    private String object;
    private String expression;
    private String referencedObjectType;
    private String referencedTable;
    private String referencedObject;
    private String referencedExpression;
    private String catalogName;
    private String cubeName;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getObjectType() {
        return objectType;
    }

    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getReferencedObjectType() {
        return referencedObjectType;
    }

    public void setReferencedObjectType(String referencedObjectType) {
        this.referencedObjectType = referencedObjectType;
    }

    public String getReferencedTable() {
        return referencedTable;
    }

    public void setReferencedTable(String referencedTable) {
        this.referencedTable = referencedTable;
    }

    public String getReferencedObject() {
        return referencedObject;
    }

    public void setReferencedObject(String referencedObject) {
        this.referencedObject = referencedObject;
    }

    public String getReferencedExpression() {
        return referencedExpression;
    }

    public void setReferencedExpression(String referencedExpression) {
        this.referencedExpression = referencedExpression;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }
}
