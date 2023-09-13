package atscale.biconnector.utils;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Locale;
import java.util.Set;

public class Tools {

    private static final Logger LOGGER = Logger.getLogger(Tools.class);
    private static final String NEW_LINE_WITH_ASTERISK = "\n    * ";

    private Tools() {
        throw new IllegalStateException("Utility class: Tools");
    }

    public static String printList(List<String> list, String prefix) {
        StringBuilder retVal = new StringBuilder();
        for (String item : list) {
            if (retVal.length() == 0) {
                retVal.append(prefix).append(item);
            } else {
                retVal.append(", ").append(item);
            }
        }
        return retVal.toString();
    }

    public static String printListInLines(List<String> list, String prefix) {
        StringBuilder retVal = new StringBuilder();
        for (String item : list) {
            if (retVal.length() == 0) {
                retVal.append(NEW_LINE_WITH_ASTERISK).append(prefix).append(item);
            } else {
                retVal.append(NEW_LINE_WITH_ASTERISK).append(item);
            }
        }
        return retVal.toString();
    }

    public static <T> T coalesce(T... items) {
        for (T i : items) {
            if (i != null && i.getClass().equals(String.class)) {
                if (((String) i).length() > 0) {
                    return i;
                }
            } else {
                if (i != null) return i;
            }
        }
        return null;
    }

    public static void printHeader(String header, Integer depth) {
        if (Constants.PRINT_HEADERS) {
            StringBuilder indent = new StringBuilder();
            for (int i = 1; i < depth; i++) {
                indent.append("   ");
            }
            LOGGER.info(indent + "** Top of " + header);
        }
    }

    public static boolean isEmpty(String in) {
        return (in == null || in.equals(""));
    }

    public static Set<String> addToSetFirstEltOptional(Set<String> setIn, String delimit, String first, String remaining) {
        if (Tools.isEmpty(first)) {
            setIn.add(remaining);
        } else {
            setIn.add(first + delimit + remaining);
        }
        return setIn;
    }

    public static boolean setIsEmpty(Set<String> dsList) {
        return (dsList == null || dsList.isEmpty());
    }

    public static String printSetWithSingleQuotes(Set<String> list, String prefix) {
        StringBuilder retVal = new StringBuilder();
        for (String item : list) {
            if (retVal.length() == 0) {
                retVal.append(prefix).append("'").append(item).append("'");
            } else {
                retVal.append(", '").append(item).append("'");
            }
        }
        return retVal.toString();
    }
}
