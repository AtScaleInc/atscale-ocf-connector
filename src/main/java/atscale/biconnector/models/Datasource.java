package atscale.biconnector.models;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Datasource {

    private int rowId;
    private int tableNumber;
    private String importDate;
    private int id;
    private int modelId;
    private String name;
    private String description;
    private int type;
    private String connectionString;
    private int impersonationMode;
    private String account;
    private String password;
    private int maxConnections;
    private int isolation;
    private int timeout;
    private String provider;
    private String lastModified;
    private String sourceDBServerName;
    private String sourceDBInstanceName;
    private int olapDBID;

}
