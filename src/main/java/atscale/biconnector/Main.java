package atscale.biconnector;

import atscale.biconnector.datasource.AtScaleBIDatasource;
import alation.sdk.bi.commands.BICommandExecutor;

/**
 * The Sample BI connector runner. This can perform one of the following operations based on the
 * command name.
 *
 * <p>1. Start the connector gRPC server listening on a specified port supplied through arguments
 *
 * <p>2. Construct the connector manifest
 *
 * <p>3. Fetch the connector source name.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        new BICommandExecutor(new AtScaleBIDatasource()).execute(args);
    }
}
