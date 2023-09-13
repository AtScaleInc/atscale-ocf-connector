package atscale.biconnector.utils;

import alation.sdk.bi.grpc.mde.AlationUser;
import alation.sdk.bi.mde.models.*;
import com.google.gson.Gson;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static atscale.biconnector.utils.Constants.*;

/**
 * A utility class, containing methods to create dummy BI objects which would ultimately be streamed
 * by the sample connector.
 */
public class ModelUtils {

    private ModelUtils() {
        throw new IllegalStateException("Utilty class ModelUtils");
    }

    /**
     * Creates a sample BI folder.
     *
     * @param id         Folder external ID
     * @param folderName Folder name
     * @return Sample BI folder
     */
    public static Folder createFolder(String id, String folderName) {
        Folder folder = new Folder(id, folderName, "Project");
        folder.setIsExtractable(true);
        folder.setOwner(ADMINISTRATOR);
        return folder;
    }

    /**
     * Creates a sample BI workbook.
     *
     * @param id         Folder external ID
     * @param folderName Folder name
     * @return Sample BI workbook
     */
    public static Folder createWorkbook(String id, String folderName) {
        Folder folder = new Folder(id, folderName, "Workbook");
        folder.setIsExtractable(true);
        folder.setOwner(ADMINISTRATOR);
        return folder;
    }

    /**
     * Creates a sample BI report.
     *
     * @param id             Report external ID
     * @param reportName     Report name
     * @param parentFolderId Parent Folder ID
     * @return Sample BI report
     */
    public static Report createReport(String id, String reportName, String parentFolderId) {
        Report report = new Report(id, reportName, "Report");
        report.setParentFolderId(parentFolderId);
        return report;
    }

    /**
     * Creates a sample BI report column.
     *
     * @param id               Report column external ID
     * @param reportColumnName Report column name
     * @param parentReportId   Parent Report ID
     * @return Sample BI report column
     */
    public static ReportColumn createReportColumn(
            String id, String reportColumnName, String parentReportId) {
        ReportColumn reportColumn = new ReportColumn(id, reportColumnName, "ReportColumn");
        reportColumn.setReportId(parentReportId);
        reportColumn.setRole(MEASURE);
        reportColumn.setDataType(STRING);
        reportColumn.setExpression(SAMPLE_EXPRESSION);
        reportColumn.setValues(List.of(SAMPLE_VALUE_1, SAMPLE_VALUE_2));
        return reportColumn;
    }

    /**
     * Creates a sample BI report column.
     *
     * @param id                 Report column external ID
     * @param reportColumnName   Report column name
     * @param parentReportId     Parent Report ID
     * @param dataSourceColumnId Datasource Column ID
     * @return Sample BI report column
     */
    public static ReportColumn createReportColumn(
            String id, String reportColumnName, String parentReportId, String dataSourceColumnId) {
        ReportColumn reportColumn = createReportColumn(id, reportColumnName, parentReportId);
        reportColumn.setDatasourceColumnIds(new ArrayList<>(List.of(dataSourceColumnId)));
        return reportColumn;
    }

    /**
     * Creates a sample BI data source.
     *
     * @param id             Data source external ID
     * @param dataSourceName Data source name
     * @param parentFolderId Parent Folder ID
     * @param connectionId   Connection ID
     * @return Sample BI datasource
     */
    public static Datasource createDataSource(
            String id, String dataSourceName, String parentFolderId, String connectionId) {
        Datasource datasource = new Datasource(id, dataSourceName, "Datasource");
        datasource.setParentFolderId(parentFolderId);
        datasource.setConnectionIds(new ArrayList<>(List.of(connectionId)));
        return datasource;
    }

    /**
     * Creates a sample BI permission.
     *
     * @param userId User external ID
     * @param folder BI folder object
     * @return Sample BI permission
     */
    public static Permission createPermission(String userId, Folder folder) {
        return new Permission(userId, folder);
    }

    /**
     * Creates a sample BI user.
     *
     * @param id       User external ID
     * @param userName User name
     * @return Sample BI user
     */
    public static User createUser(String id, String userName) {
        return new User(id, userName, "User");
    }

    public static List<AlationUser> getAlationUsersFromFile(String filename) throws IOException {
        if (filename == null) {
            return Collections.emptyList();
        }
        File file = new File(filename);
        String content = new String(Files.readAllBytes(file.toPath()));
        Gson gson = new Gson();
        List<String> usernames = gson.fromJson(content, List.class);
        List<AlationUser> alationUsers = new ArrayList<>();
        for (String username : usernames) {
            alationUsers.add(AlationUser.newBuilder().setUsername(username).build());
        }
        return alationUsers;
    }

    public static List<Folder> getAlationFoldersFromFile(String filename) throws IOException {
        if (filename == null) {
            return Collections.emptyList();
        }
        File file = new File(filename);
        String content = new String(Files.readAllBytes(file.toPath()));
        Gson gson = new Gson();

        List<String> foldernames = gson.fromJson(content, List.class);
        List<Folder> alationFolders = new ArrayList<>();
        for (String folder : foldernames) {
            alationFolders.add(new Folder(folder, folder, "Project"));
        }
        return alationFolders;
    }

}
