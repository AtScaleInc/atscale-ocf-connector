package atscale.biconnector.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for Measure
 */
public class MeasureUtils {

    private MeasureUtils() {
        throw new IllegalStateException("Measure Utility class");
    }

    /**
     * @param measureAggNum
     * @return
     */
    public static String getMeasureAggName(Integer measureAggNum) {
        Map<Integer, String> measureAggMap = new HashMap<>();
        measureAggMap.put(0, "STDDEV_POP"); // VAR_POP
        measureAggMap.put(1, "SUM");
        measureAggMap.put(2, "COUNT"); //NON-DISTINCT COUNT
        measureAggMap.put(3, "MIN");
        measureAggMap.put(4, "MAX");
        measureAggMap.put(5, "AVG");
        measureAggMap.put(6, "VAR"); // VAR_SAMP
        measureAggMap.put(7, "STDDEV_SAMP");
        measureAggMap.put(8, "DISTINCTCOUNT"); // DISTINCT COUNT ESTIMATE
        measureAggMap.put(9, "NOAGG");
        return measureAggMap.get(measureAggNum);
    }

    /**
     * @param formatStr
     * @return
     */
    public static String getFormatWithName(String formatStr) {
        String str = null;
        switch (formatStr) {
            case "#,##0.00":
                str = "Standard";
                break;
            case "0.0##E+0":
                str = "Scientific";
                break;
            case "#.####":
                str = "General Number";
                break;
            default:
                str = "Custom";
        }
        return String.format("%s(%s)", str, formatStr);
    }
}
