package atscale.biconnector.utils;

public class Utilities {

    private Utilities() {
        throw new IllegalStateException("Utility class");
    }

    public static boolean inProjectListForTesting(String projectName) {
        return (Constants.PROJECTS_TO_FILTER == null || (Constants.PROJECTS_TO_FILTER != null && !Constants.PROJECTS_TO_FILTER.isEmpty() && Constants.PROJECTS_TO_FILTER.contains(projectName)));
    }
}
