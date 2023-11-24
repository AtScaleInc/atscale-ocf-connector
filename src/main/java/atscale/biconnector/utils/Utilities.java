package atscale.biconnector.utils;

import java.util.*;

public class Utilities {

    private Utilities() {
        throw new IllegalStateException("Utility class");
    }

    public static boolean inProjectListForTesting(String projectName) {
        return (Constants.PROJECTS_TO_FILTER == null || (Constants.PROJECTS_TO_FILTER != null && !Constants.PROJECTS_TO_FILTER.isEmpty() && Constants.PROJECTS_TO_FILTER.contains(projectName)));
    }

    /**
     *
     * @param map
     * @param key
     * @param value
     */
    public static void addMultiUniqueValuesToMap(Map<String, Set<String>> map, String key, String value) {
        map.computeIfAbsent(key, k -> new HashSet<>()).add(value);
    }
}
