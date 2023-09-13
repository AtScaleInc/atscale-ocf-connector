package atscale.biconnector.utils;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SOAPResultSetTest {

    private SOAPResultSet soapResultSet;

    @Mock
    private SOAPResultSet resultSet;

    private SOAPResultSet SOAPResultSet = new SOAPResultSet();

    @Before
    public void setUp() {
        resultSet = new SOAPResultSet();

        // Populate the resultSet with test data
        List<String> columnNames = new ArrayList<>();
        columnNames.add("TimeColumn");

        List<String> row1 = new ArrayList<>();
        row1.add("10:30:45");

        resultSet.columnNames = columnNames;
        resultSet.dataRows.add(row1);

        MockitoAnnotations.initMocks(this);

        soapResultSet = new SOAPResultSet();
    }

    @Test
    public void testInsertRow() {
        Boolean result = SOAPResultSet.insertRow(resultSet.columnNames);
        assertEquals(true, result);
    }

    @Test
    public void testInsertColumn() {
        Boolean result = SOAPResultSet.insertColumn("fieldName", "fieldType");
        assertEquals(true, result);
    }

    @Test
    public void testGetRow() {
        int result = SOAPResultSet.getRow();
        assertEquals(0, result);
    }

    @Test
    public void testNext() {
        Boolean result = SOAPResultSet.next();
        assertEquals(false, result);
    }

    @Test
    public void testPrevious() {
        Boolean result = SOAPResultSet.previous();
        assertEquals(false, result);
    }

}
