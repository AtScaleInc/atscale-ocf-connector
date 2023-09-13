package atscale.biconnector.models;

import atscale.biconnector.utils.Tools;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

@Data
@NoArgsConstructor
public class Level {
    private int tableNumber;
    private String importDate;
    private String catalogName;
    private int rowId;
    private String schemaName;
    private String cubeName;
    private String cubeGUID;
    private String datasetName;
    private String dimensionUniqueName;
    private String hierarchyUniqueName;
    private String levelName;
    private int levelNumber;
    private String levelUniqueName;
    private String levelGUID;
    private String levelCaption;
    private String description;
    private boolean isVisible;
    private String nameColumn;
    private String keyColumns;
    private String sortColumn;
    private String nameDataType;
    private String sortDataType;
    private Dataset dataset;
    private boolean isPrimary;
    private String parentLevelGUID;


    public List<String> getSourceColumnIDs(Dataset dataset) {
        Set<String> colSet = new HashSet<>();

        if (!Tools.isEmpty(dataset.getSchema()) && !Tools.isEmpty(dataset.getTable())) {
            colSet = Tools.addToSetFirstEltOptional(colSet, "/", dataset.getDatabase(), StringUtils.joinWith("/", dataset.getSchema(), dataset.getTable(), this.nameColumn));

            if (this.keyColumns != null && this.keyColumns.length() > 0) {
                for (String col : this.keyColumns.split(",")) {
                    colSet = Tools.addToSetFirstEltOptional(colSet, "/", dataset.getDatabase(), StringUtils.joinWith("/", dataset.getSchema(), dataset.getTable(), col));
                }
            }
            if (this.sortColumn != null && this.sortColumn.length() > 0) {
                colSet = Tools.addToSetFirstEltOptional(colSet, "/", dataset.getDatabase(), StringUtils.joinWith("/", dataset.getSchema(), dataset.getTable(), this.sortColumn));
            }
        }
        if (!colSet.isEmpty()) {
            return new ArrayList<>(colSet);
        }
        return Collections.emptyList();
    }

    public Level getAssociatedLevel(Set<Level> levels, List<String> missingLevels) {
        if (Tools.isEmpty(this.getParentLevelGUID())) {
            return null;
        }
        for (Level level : levels) {
            if (this.getParentLevelGUID().equals(level.getLevelGUID())) {
                return level;
            }
        }
        missingLevels.add(this.getLevelName());
        return null; // If level attribute is invisible, won't be included dmv results
    }
}
