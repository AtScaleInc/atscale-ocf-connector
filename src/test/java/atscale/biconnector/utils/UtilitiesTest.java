package atscale.biconnector.utils;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class UtilitiesTest {

    @Test
    public void testInProjectListForTesting_WhenProjectsToFilterIsNull() {
        Constants.PROJECTS_TO_FILTER = null;
        boolean result = Utilities.inProjectListForTesting("Project A");
        assertTrue(result);
    }

    @Test
    public void testInProjectListForTesting_WhenProjectsToFilterContainsProjectName() {
        List<String> projectsToFilter = Arrays.asList("Project A", "Project B");
        Constants.PROJECTS_TO_FILTER = projectsToFilter;
        boolean result = Utilities.inProjectListForTesting("Project A");
        assertTrue(result);
    }

    @Test
    public void testInProjectListForTesting_WhenProjectsToFilterDoesNotContainProjectName() {
        List<String> projectsToFilter = Arrays.asList("Project A", "Project B");
        Constants.PROJECTS_TO_FILTER = projectsToFilter;
        boolean result = Utilities.inProjectListForTesting("Project C");
        assertFalse(result);
    }

}
