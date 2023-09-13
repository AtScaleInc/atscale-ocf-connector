package atscale.biconnector.extractor;

import atscale.biconnector.models.Dataset;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import utils.AtScaleBIUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class DatasetExtractorTest {
    private DatasetExtractor datasetExtractor;

    @Before
    public void setUp() {
        datasetExtractor = new DatasetExtractor(null);
    }

    @Test
    public void testConvertResultsetToAtScaleObjects() throws SQLException, SQLException {
        ResultSet resultSet = Mockito.mock(ResultSet.class);

        Mockito.when(resultSet.next()).thenReturn(true, false);
        Mockito.when(resultSet.getRow()).thenReturn(1);
        Mockito.when(resultSet.getString("DATASET_NAME")).thenReturn("Dataset1");
        Mockito.when(resultSet.getString("CATALOG_NAME")).thenReturn("Catalog1");
        Mockito.when(resultSet.getString("CUBE_GUID")).thenReturn("CubeGuid1");
        Mockito.when(resultSet.getString("DATABASE")).thenReturn("Database1");
        Mockito.when(resultSet.getString("TABLE")).thenReturn("Table1");
        Mockito.when(resultSet.getString("SCHEMA")).thenReturn("Schema1");
        Mockito.when(resultSet.getString("EXPRESSION")).thenReturn("Expression1");
        Mockito.when(resultSet.getString("CONNECTION_ID")).thenReturn("ConnectionId1");

        datasetExtractor.convertResultsetToAtScaleObjects(resultSet);

        Dataset dataset = AtScaleBIUtils.getDataSets();
        assertEquals("Dataset1", dataset.getDatasetName());
        assertEquals("Catalog1", dataset.getCatalogName());
        assertEquals("CubeGuid1", dataset.getCubeGUID());
        assertEquals("Database1", dataset.getDatabase());
        assertEquals("Table1", dataset.getTable());
        assertEquals("Schema1", dataset.getSchema());
        assertEquals("Expression1", dataset.getExpression());
        assertEquals("ConnectionId1", dataset.getConnection());
    }

}
