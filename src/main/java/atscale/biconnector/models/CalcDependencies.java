package atscale.biconnector.models;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class CalcDependencies {

    private String databaseName;
    private String objectType;
    private String table;
    private String object;
    private String expression;
    private String referencedObjectType;
    private String referencedTable;
    private boolean referencedObject;
    private String referencedExpression;
}
