package atscale.biconnector.utils;

import alation.sdk.bi.grpc.mde.AlationUser;
import alation.sdk.bi.mde.models.Datasource;
import alation.sdk.bi.mde.models.Folder;
import alation.sdk.bi.mde.models.ReportColumn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class ModelUtilsTest {

    @Test
    public void testGetAlationFoldersFromFile_WhenFileNameIsNull() throws Exception {
        String filename = null;
        if (filename == null) {
            Collections.emptyList();
        }
        List<Folder> list = ModelUtils.getAlationFoldersFromFile(null);
        assertEquals(list, Collections.emptyList());
    }

    @Test
    public void testGetAlationUsersFromFile_WhenFileNameIsNull() throws Exception {
        String filename = null;
        if (filename == null) {
            Collections.emptyList();
        }
        List<AlationUser> list = ModelUtils.getAlationUsersFromFile(null);
        assertEquals(list, Collections.emptyList());
    }

    @Test
    public void testCreateReportColumn() {
        ReportColumn result = ModelUtils.createReportColumn("id", "reportColumnName", "parentReportId", "dataSourceColumnId");
        assertNotNull(result);
    }

    @Test
    public void testCreateDataSource() {
        Datasource result = ModelUtils.createDataSource("id", "dataSourceName", "parentFolderId", "connectionId");
        assertNotNull(result);
    }
}
