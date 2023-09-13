package atscale.biconnector.utils;

import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

public class SOAPResultSet extends AbstractResultSet {

    public class Field {
        private String fieldName;
        private String fieldType;

        public Field(String name, String type) {
            fieldName = name;
            fieldType = type;
        }

        public void setFieldName(String name) {
            fieldName = name;
        }

        public void setFieldType(String type) {
            fieldType = type;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String getFieldType() {
            return fieldType;
        }
    }

    int currentRow = 0;
    List<Field> dataCols = new ArrayList<>();
    public List<List<String>> dataRows = new ArrayList<>();
    public List<String> columnNames = new ArrayList<>();

    public boolean insertColumn(String fieldName, String fieldType) {
        Field colField = new Field(fieldName, fieldType);
        dataCols.add(colField);
        columnNames.add(fieldName);
        return true;
    }

    public boolean insertRow(List<String> row) {
        dataRows.add(row);
        return true;
    }

    @Override
    public int getRow() {
        return currentRow;
    }

    @Override
    public boolean next() {
        if (currentRow < dataRows.size()) {
            currentRow++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean previous() {
        if (currentRow > 0) {
            currentRow--;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) {
        String value = getString(columnLabel);
        return Boolean.parseBoolean(value) || value.equals("1");
    }

    @Override
    public Date getDate(int columnIndex) {
        String value = getString(columnIndex);
        return value.isEmpty() ? null : Date.valueOf(value);
    }

    @Override
    public Date getDate(String columnLabel) {
        String value = getString(columnLabel);
        return value.isEmpty() ? null : Date.valueOf(value);
    }

    @Override
    public double getDouble(int columnIndex) {
        String value = getString(columnIndex);
        return value.isEmpty() ? 0.0 : Double.parseDouble(value);

    }

    @Override
    public double getDouble(String columnLabel) {
        String value = getString(columnLabel);
        return value.isEmpty() ? 0.0 : Double.parseDouble(value);
    }

    @Override
    public float getFloat(int columnIndex) {
        String value = getString(columnIndex);
        return value.isEmpty() ? 0.0f : Float.parseFloat(value);
    }

    @Override
    public float getFloat(String columnLabel) {
        String value = getString(columnLabel);
        return value.isEmpty() ? 0.0f : Float.parseFloat(value);
    }

    @Override
    public int getInt(int columnIndex) {
        String value = getString(columnIndex);
        return value.isEmpty() ? 0 : Integer.parseInt(value);
    }

    @Override
    public int getInt(String columnLabel) {
        String value = getString(columnLabel);
        return value.isEmpty() ? 0 : Integer.parseInt(value);
    }

    @Override
    public long getLong(int columnIndex) {
        String value = getString(columnIndex);
        return value.isEmpty() ? 0 : Long.parseLong(value);
    }

    @Override
    public long getLong(String columnLabel) {
        String value = getString(columnLabel);
        return value.isEmpty() ? 0 : Long.parseLong(value);
    }

    @Override
    public short getShort(int columnIndex) {
        String value = getString(columnIndex);
        return value.isEmpty() ? 0 : Short.parseShort(value);
    }

    @Override
    public short getShort(String columnLabel) {
        String value = getString(columnLabel);
        return value.isEmpty() ? 0 : Short.parseShort(value);
    }

    @Override
    public String getString(int columnIndex) {
        return dataRows.get(currentRow - 1).get(columnIndex);
    }

    @Override
    public String getString(String columnLabel) {
        int columnIndex = columnNames.indexOf(columnLabel);
        return dataRows.get(currentRow - 1).get(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) {
        String value = getString(columnIndex);
        return value.isEmpty() ? null : Time.valueOf(value);
    }

    @Override
    public Time getTime(String columnLabel) {
        String value = getString(columnLabel);
        return value.isEmpty() ? null : Time.valueOf(value);
    }
}
