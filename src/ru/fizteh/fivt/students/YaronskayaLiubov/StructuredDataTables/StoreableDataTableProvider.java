package ru.fizteh.fivt.students.YaronskayaLiubov.StructuredDataTables;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.storage.structured.TableProvider;

import javax.xml.stream.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;

/**
 * Created by luba_yaronskaya on 16.11.14.
 */

public class StoreableDataTableProvider implements TableProvider {
    public String dbDir;
    protected HashMap<String, StoreableDataTable> tables;

    protected StoreableDataTableProvider(String dir) throws IllegalArgumentException {
        if (dir == null) {
            throw new IllegalArgumentException("directory name is null");
        }
        this.dbDir = dir;
        if (!Files.exists(Paths.get(dbDir))) {
            try {
                Files.createDirectory(Paths.get(dbDir));
            } catch (IOException e) {
                throw new IllegalArgumentException(dbDir + " illegal name of directory");
            }
        }

        tables = new HashMap<String, StoreableDataTable>();
        String[] tableNames = new File(dbDir).list();
        for (String s : tableNames) {
            Path tableName = Paths.get(dbDir).resolve(s);
            if (Files.isDirectory(tableName)) {
                tables.put(s, new StoreableDataTable(tableName.toString()));
            }
        }
    }

    @Override
    public Table getTable(String name) {
        CheckParameters.checkTableName(name);
        return tables.get(name);
    }

    @Override
    public Table createTable(String name, List<Class<?>> columnTypes) throws IOException {
        CheckParameters.checkTableName(name);
        CheckParameters.checkColumnTypesList(columnTypes);
        StoreableDataTable newTable = new StoreableDataTable(dbDir + File.separator + name);
        newTable.setColumnTypes(columnTypes);
        if (tables.get(name) == null) {
            tables.put(name, newTable);
            return newTable;
        }
        return null;
    }

    @Override
    public void removeTable(String name) throws IOException {
        CheckParameters.checkTableName(name);
        if (tables.remove(name) == null) {
            throw new IllegalStateException("table '" + name + "' does not exist");
        }
        try {
            StoreableDataTable.fileDelete(new File(Paths.get(dbDir).resolve(name).toString()));
        } catch (NullPointerException e) {
            //do something?
        }
    }

    @Override
    public Storeable deserialize(Table table, String value) throws ParseException {
        TableItem row = new TableItem(table);
        try {
            XMLStreamReader xmlReader = XMLInputFactory.newInstance()
                    .createXMLStreamReader(new StringReader(value));
            if (!xmlReader.hasNext()) {
                throw new ParseException("value is empty", 0);
            }

            int nodeType = xmlReader.next();
            if (nodeType == XMLStreamConstants.START_ELEMENT && xmlReader.getName().getLocalPart().equals("row")) {
                int columnIndex = 0;
                while (xmlReader.hasNext()) {
                    int subElementNodeType = xmlReader.nextTag();
                    if (xmlReader.getName().equals("null")) {
                        row.setColumnAt(columnIndex, null);
                    } else {
                        if (!xmlReader.getName().equals("col")) {
                            throw new ParseException("Incorrect tag name", xmlReader.getLocation().getCharacterOffset());
                        }
                        row.setColumnAt(columnIndex, parseXxx(xmlReader.getElementText(), table.getColumnType(columnIndex)));
                    }
                    ++columnIndex;
                }

            } else {
                throw new ParseException("Incorrect xml format", 0);
            }
        } catch (XMLStreamException e) {
            throw new RuntimeException("error reading " + value);
        }
        return row;
    }

    @Override
    public String serialize(Table table, Storeable value) throws ColumnFormatException {
        CheckParameters.checkMatchItemToTable(table, value);
        StringWriter stringWriter = new StringWriter();
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        try {
            XMLStreamWriter writer = factory.createXMLStreamWriter(stringWriter);
            writer.writeStartElement("row");
            try {
                for (int i = 0; i < table.getColumnsCount(); ++i) {
                    if (value.getColumnAt(i) == null) {
                        writer.writeEmptyElement("null");
                    } else {
                        writer.writeStartElement("col");
                        writer.writeCharacters(value.getColumnAt(i).toString());
                        writer.writeEndElement();
                    }
                }
                writer.writeEndElement();
            } finally {
                writer.close();
            }
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
        return stringWriter.toString();
    }

    @Override
    public Storeable createFor(Table table) {
        return new TableItem(table);
    }

    @Override
    public Storeable createFor(Table table, List<?> values) throws ColumnFormatException, IndexOutOfBoundsException {
        TableItem row = new TableItem(table);
        if (table.getColumnsCount() != values.size()) {
            throw new IndexOutOfBoundsException("Incorrect values count");
        }
        for (int i = 0; i < table.getColumnsCount(); ++i) {
            row.setColumnAt(i, values.get(i));
        }
        return row;
    }

    public static Object parseXxx(String value, Class<?> type) {
        switch (type.getSimpleName()) {
            case "Integer":
                return Integer.parseInt(value);
            case "Long":
                return Long.parseLong(value);
            case "Byte":
                return Byte.parseByte(value);
            case "Float":
                return Float.parseFloat(value);
            case "Double":
                return Double.parseDouble(value);
            case "Boolean":
                return Boolean.parseBoolean(value);
            case "String":
                return value;
            default:
                throw new IOException("Undefined type of value");
        }
    }
}
