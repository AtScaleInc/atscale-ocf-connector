package atscale.biconnector.extractor;

import atscale.biconnector.models.Dimension;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import utils.AtScaleBIUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import static org.junit.Assert.assertNotNull;

public class DimensionExtractorTest {
    private Map<String, Set<String>> dimToDatasetsMap;
//    DimensionExtractor dimensionExtractor = new DimensionExtractor(null, dimToDatasetsMap);


    private DimensionExtractor dimensionExtractor;

    @Before
    public void setUp() {
        dimensionExtractor = new DimensionExtractor(null, dimToDatasetsMap);
    }


    @Test
    public void testGetTableName() {
        Dimension dim = new Dimension();
        dim.setDimensionUniqueName("iatscaledb");
        dim.setSourceDBInstanceName("atscale");
        String result = dimensionExtractor.getTableName(dim);
        assertNotNull(result);
    }

    @Test
    public void testUpdateFoundObject() {
        DimensionExtractor.updateFoundObject(null, null);
    }

    @Test
    public void testConvertResultsetToAtScaleObjects() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString("CATALOG_NAME")).thenReturn("Catalog1");
        when(resultSet.getString("SCHEMA_NAME")).thenReturn("Schema1");
        when(resultSet.getString("CUBE_NAME")).thenReturn("CubeName1");
        when(resultSet.getString("CUBE_GUID")).thenReturn("CubeGuid1");
        when(resultSet.getString("DIMENSION_NAME")).thenReturn("DimensionName1");
        when(resultSet.getString("DIMENSION_UNIQUE_NAME")).thenReturn("DimensionUniqueName1");
        when(resultSet.getString("DIMENSION_GUID")).thenReturn("DimensionGuid1");
        when(resultSet.getString("DIMENSION_CAPTION")).thenReturn("DimensionCaption1");
        when(resultSet.getInt("DIMENSION_ORDINAL")).thenReturn(1);
        when(resultSet.getInt("DIMENSION_TYPE")).thenReturn(2);
        when(resultSet.getInt("DIMENSION_CARDINALITY")).thenReturn(3);
        when(resultSet.getString("DEFAULT_HIERARCHY")).thenReturn("DefaultHierarchy1");
        when(resultSet.getString("DESCRIPTION")).thenReturn("Description1");
        when(resultSet.getBoolean("IS_VIRTUAL")).thenReturn(true);
        when(resultSet.getBoolean("IS_READWRITE")).thenReturn(true);
        when(resultSet.getInt("DIMENSION_UNIQUE_SETTINGS")).thenReturn(4);
        when(resultSet.getString("DIMENSION_MASTER_NAME")).thenReturn("DimensionMasterName1");
        when(resultSet.getBoolean("DIMENSION_IS_VISIBLE")).thenReturn(true);

        dimensionExtractor.convertResultsetToAtScaleObjects(resultSet);

        Set<Dimension> dimensions = AtScaleBIUtils.getDimensions();

        assertEquals(1, dimensions.size());

        Dimension dimension = dimensions.iterator().next();
        assertEquals(1, dimension.getRowId());
        assertEquals(2, dimension.getTableNumber());
        assertEquals("Catalog1", dimension.getCatalogName());
        assertEquals("Schema1", dimension.getSchemaName());
        assertEquals("CubeName1", dimension.getCubeName());
        assertEquals("CubeGuid1", dimension.getCubeGUID());
        assertEquals("DimensionName1", dimension.getDimensionName());
        assertEquals("DimensionUniqueName1", dimension.getDimensionUniqueName());
        assertEquals("DimensionGuid1", dimension.getDimensionGUID());
        assertEquals("DimensionCaption1", dimension.getDimensionCaption());
        assertEquals(1, dimension.getDimensionOrdinal());
        assertEquals(3, dimension.getDimensionCardinality());
        assertEquals("DefaultHierarchy1", dimension.getDefaultHierarchy());
        assertEquals("Description1", dimension.getDescription());
        assertEquals(4, dimension.getDimensionUniqueSettings());
        assertEquals("DimensionMasterName1", dimension.getDimensionMasterName());
        assertEquals("SourceDBServerName1", dimension.getSourceDBServerName());
        assertEquals("SourceDBInstanceName1", dimension.getSourceDBInstanceName());
    }
}
