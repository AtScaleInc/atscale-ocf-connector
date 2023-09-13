package atscale.biconnector.utils;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class ToolsTest {

    private static final String NEW_LINE_WITH_ASTERISK = "\n    * ";

    @Test
    public void testSetIsEmpty_WhenListIsNull() {
        Boolean result = Tools.setIsEmpty(null);
        assertEquals(true, result);
    }

    @Test
    public void testSetIsEmpty_WhenListIsEmpty() {
        Boolean result = Tools.setIsEmpty(Collections.emptySet());
        assertEquals(true, result);
    }

    @Test
    public void testSetIsEmpty_WhenListIsNotNull() {
        Set<String> dsList = new HashSet<>();
        dsList.add("ABC");
        Boolean result = Tools.setIsEmpty(dsList);
        assertEquals(false, result);
    }

    @Test
    public void testIsEmpty_WhenStringNull() {
        Boolean result = Tools.isEmpty(null);
        assertEquals(true, result);
    }

    @Test
    public void testIsEmpty_WhenStringEmpty() {
        Boolean result = Tools.isEmpty("");
        assertEquals(true, result);
    }

    @Test
    public void testIsEmpty_WhenString() {
        Boolean result = Tools.isEmpty("ABC");
        assertEquals(false, result);
    }

    @Test
    public void testAddToSetFirstEltOptional_WhenFirstEmpty() {
        Set<String> setIn = new HashSet<>();
        setIn.add("ABC");
        Set<String> set = Tools.addToSetFirstEltOptional(setIn, "delimit", "", "remaining");
        assertEquals(set, setIn);
    }

    @Test
    public void testAddToSetFirstEltOptional_WhenFirst() {
        Set<String> setIn = new HashSet<>();
        setIn.add("ABC");
        Set<String> set = Tools.addToSetFirstEltOptional(setIn, "delimit", "first", "remaining");
        assertEquals(set, setIn);
    }

    @Test
    public void testPrintListInLines_WhenStringNotEmpty() {
        List<String> list = new ArrayList<>();
        StringBuilder retVal = new StringBuilder();
        retVal.append(NEW_LINE_WITH_ASTERISK + "prefixitem");
        list.add("item");
        String result = Tools.printListInLines(list, "prefix");
        assertEquals(result, retVal.toString());
    }

    @Test
    public void testPrintSetWithSingleQuotes() {
        Set<String> list = new HashSet<>();
        list.add("item");
        StringBuilder retVal = new StringBuilder();
        retVal.append("prefix'item'");
        String result = Tools.printSetWithSingleQuotes(list, "prefix");
        assertEquals(result, retVal.toString());
    }

    @Test
    public void testPrintList() {
        List<String> list = new ArrayList<>();
        list.add("item");
        String retVal = "prefixitem";
        String result = Tools.printList(list, "prefix");
        assertEquals(retVal, result);
    }

}
