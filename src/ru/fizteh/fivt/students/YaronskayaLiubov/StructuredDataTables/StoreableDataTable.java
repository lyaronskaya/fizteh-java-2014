package ru.fizteh.fivt.students.YaronskayaLiubov.StructuredDataTables;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;

/**
 * Created by luba_yaronskaya on 16.11.14.
 */

public class StoreableDataTable implements Table {
    protected File curDB;
    public String dbPath;
    private List<Class<?>> columnTypes;
    private HashMap<String, Storeable> committedData;
    private HashMap<String, Storeable> deltaAdded;
    private HashMap<String, Storeable> deltaChanged;
    private HashSet<String> deltaRemoved;

    protected StoreableDataTable(String dbPath) {
        if (!Files.exists(Paths.get(dbPath))) {
            try {
                Files.createDirectory(Paths.get(dbPath));
            } catch (IOException e) {
                throw new IllegalArgumentException(dbPath + "is not a directory");
            }
        }
        if (!Files.isDirectory(Paths.get(dbPath))) {
            throw new IllegalArgumentException(dbPath + "is not a directory");
        }
        this.dbPath = dbPath;
        curDB = new File(dbPath);
        committedData = new HashMap<String, Storeable>();
        deltaAdded = new HashMap<String, Storeable>();
        deltaChanged = new HashMap<String, Storeable>();
        deltaRemoved = new HashSet<String>();
        loadDBData();
    }

    @Override
    public String getName() {
        return curDB.getName();
    }

    @Override
    public Storeable get(String key) {
        CheckParameters.checkKey(key);
        if (deltaRemoved.contains(key)) {
            return null;
        }
        Storeable value = deltaAdded.get(key);
        if (value != null) {
            return value;
        }
        value = deltaChanged.get(key);
        if (value != null) {
            return value;
        }
        return committedData.get(key);
    }

    @Override
    public Storeable put(String key, Storeable value) throws ColumnFormatException {
        CheckParameters.checkKey(key);
        CheckParameters.checkValue(value);

        Storeable oldValue = null;
        if (deltaRemoved.contains(key)) {
            deltaAdded.put(key, value);
        } else {
            if (deltaAdded.containsKey(key)) {
                oldValue = deltaAdded.remove(key);
                deltaChanged.put(key, value);
            } else {
                if (deltaChanged.containsKey(key)) {
                    oldValue = deltaChanged.get(key);
                    deltaChanged.put(key, value);
                } else {
                    oldValue = committedData.get(key);
                    deltaAdded.put(key, value);
                }
            }
        }
        return oldValue;
    }

    @Override
    public Storeable remove(String key) {
        CheckParameters.checkKey(key);

        Storeable value = null;
        if (deltaAdded.containsKey(key)) {
            value = deltaAdded.remove(key);
        } else {
            if (deltaChanged.containsKey(key)) {
                value = deltaChanged.remove(key);
            } else {
                value = committedData.remove(key);
            }
        }
        deltaRemoved.add(key);
        return value;
    }

    @Override
    public int size() {
        return committedData.size() + deltaAdded.size() + deltaChanged.size();
    }

    @Override
    public int commit() throws IOException {
        int deltaCount = deltaAdded.size() + deltaChanged.size() + deltaRemoved.size();
        committedData.putAll(deltaAdded);
        committedData.putAll(deltaChanged);
        committedData.keySet().removeAll(deltaRemoved);
        //save();
        deltaAdded.clear();
        deltaChanged.clear();
        deltaRemoved.clear();
        return deltaCount;
    }

    @Override
    public int rollback() {
        int deltaCount = deltaAdded.size() + deltaChanged.size() + deltaRemoved.size();
        deltaAdded.clear();
        deltaChanged.clear();
        deltaRemoved.clear();
        return deltaCount;
    }

    @Override
    public int getColumnsCount() {
        return columnTypes.size();
    }

    @Override
    public Class<?> getColumnType(int columnIndex) throws IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= getColumnsCount()) {
            throw new IndexOutOfBoundsException("illegal column index");
        }
        return columnTypes.get(columnIndex);
    }

    @Override
    protected void finalize() throws Throwable {
        save();
        super.finalize();
    }
    public List<String> list() {
        List<String> keys = new ArrayList<String>(committedData.keySet());
        keys.removeAll(deltaRemoved);
        keys.addAll(deltaAdded.keySet());
        keys.addAll(deltaChanged.keySet());
        return keys;
    }

    public int unsavedChangesCount() {
        return deltaAdded.size() + deltaChanged.size() + deltaRemoved.size();
    }

    public void loadDBData() {
        committedData.clear();
        deltaAdded.clear();
        deltaChanged.clear();
        deltaRemoved.clear();
        File[] tableDirs = curDB.listFiles();
        for (File dir : tableDirs) {
            if (dir.getName().equals(".DS_Store") || dir.getName().equals("signature.tsv")) {
                continue;
            }
            File[] tableFiles = dir.listFiles();
            if (tableFiles.length == 0) {
                continue;
            }
            for (File tableFile : tableFiles) {
                if (tableFile.getName().equals(".DS_Store")) {
                    continue;
                }
                FileChannel channel = null;
                try {
                    channel = new FileInputStream(tableFile.getCanonicalPath()).getChannel();

                    ByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());

                    while (byteBuffer.remaining() > 0) {
                        int keyLength = byteBuffer.getInt();
                        byte[] key = new byte[keyLength];
                        byteBuffer.get(key, 0, keyLength);
                        int valueLength = byteBuffer.getInt();
                        byte[] value = new byte[valueLength];

                        byteBuffer.get(value, 0, valueLength);
                        try {
                            Storeable row = new StoreableDataTableProviderFactory().create(curDB.getParent()).deserialize(this, new String(value, "UTF-8"));
                            committedData.put(new String(key, "UTF-8"), row);
                        }
                        catch (ParseException e) {
                            throw new RuntimeException(e.getMessage());
                        }
                    }
                } catch (IOException e) {
                    System.err.println("error reading file" + e.toString()
                    );
                }
            }
        }
    }

    public void save() {
        for (int i = 0; i < 16; ++i) {
            try {
                Path dirName = Paths.get(curDB.getCanonicalPath()).resolve(i + ".dir/");
                if (!Files.exists(dirName)) {
                    Files.createDirectory(dirName);
                }
                for (int j = 0; j < 16; ++j) {
                    Path fileName = dirName.resolve(j + ".dat");
                    if (!Files.exists(fileName)) {
                        Files.createFile(fileName);
                    }
                }
            } catch (IOException e) {
                System.err.println("error creating directory");
            }
        }

        FileOutputStream[][] fos = new FileOutputStream[16][16];
        boolean[] usedDirs = new boolean[16];
        boolean[][] usedFiles = new boolean[16][16];
        try {
            for (Map.Entry<String, Storeable> entry : committedData.entrySet()) {
                String key = entry.getKey();
                Storeable row = entry.getValue();
                int hashcode = Math.abs(key.hashCode());
                int ndirectory = hashcode % 16;
                int nfile = hashcode / 16 % 16;
                if (!usedFiles[ndirectory][nfile]) {
                    if (!usedDirs[ndirectory]) {
                        usedDirs[ndirectory] = true;
                    }
                    usedFiles[ndirectory][nfile] = true;
                    fos[ndirectory][nfile] = new FileOutputStream(dbPath + File.separator + ndirectory + ".dir"
                            + File.separator + nfile + ".dat");
                }
                byte[] keyInBytes = new byte[0];
                byte[] valueInBytes = new byte[0];
                keyInBytes = key.getBytes("UTF-8");
                String value = new StoreableDataTableProviderFactory().create(curDB.getParent()).serialize(this, row);
                valueInBytes = value.getBytes("UTF-8");
                ByteBuffer bb = ByteBuffer.allocate(8 + keyInBytes.length + valueInBytes.length);
                bb.putInt(keyInBytes.length);
                bb.put(keyInBytes);
                bb.putInt(valueInBytes.length);
                bb.put(valueInBytes);
                int limit = bb.limit();

                for (int i = 0; i < limit; ++i) {
                    fos[ndirectory][nfile].write(bb.get(i));
                }

            }
        } catch (Exception e) {
            for (int i = 0; i < 16; ++i) {
                for (int j = 0; j < 16; ++j) {
                    if (fos[i][j] != null) {
                        try {
                            fos[i][j].close();
                        } catch (IOException e1) {
                            continue;
                        }
                    }
                }
            }
        }
        for (int i = 0; i < 16; ++i) {
            for (int j = 0; j < 16; ++j) {
                if (fos[i][j] != null) {
                    try {
                        fos[i][j].close();
                    } catch (IOException e1) {
                        continue;
                    }
                }
            }
        }

        for (int i = 0; i < 16; ++i) {
            boolean emptyDir = true;
            for (int j = 0; j < 16; ++j) {
                String fileName = dbPath + File.separator + i + ".dir" + File.separator + j + ".dat";
                if (!usedFiles[i][j]) {
                    try {
                        Files.delete(Paths.get(dbPath + File.separator + i + ".dir" + File.separator + j + ".dat"));
                    } catch (IOException e) {
                        continue;
                    }
                } else {
                    emptyDir = false;
                }
            }
            if (emptyDir) {
                try {
                    Files.delete(Paths.get(dbPath + File.separator + i + ".dir"));
                } catch (IOException e) {
                    continue;
                }
            }
        }
    }

    public void setColumnTypes(List<Class<?>> columnTypes) throws IOException {
        this.columnTypes = columnTypes;
        File signatureFile = new File(dbPath, "signature.tsv");
        if (!signatureFile.exists()) {
            signatureFile.createNewFile();
        }
        FileOutputStream out = new FileOutputStream(signatureFile);
        try {
            StringBuffer res = new StringBuffer();
            for (Class<?> type : columnTypes) {
                String name = type.getSimpleName();
                switch (name) {
                    case "Integer":
                        res.append("int ");
                        break;
                    case "Long":
                        res.append("long ");
                        break;
                    case "Byte":
                        res.append("byte ");
                        break;
                    case "Float":
                        res.append("float ");
                        break;
                    case "Double":
                        res.append("double ");
                        break;
                    case "Boolean":
                        res.append("boolean ");
                        break;
                    case "String":
                        res.append("String ");
                        break;
                    default:
                        throw new RuntimeException("Incorrect type");
                }
            }
            res.setLength(res.length() - 1);
            out.write((res.toString()).getBytes("UTF-8"));
        } catch (IOException e) {
            throw new RuntimeException("error writing signature");
        }
    }
    public static void fileDelete(File myDir) {
        if (myDir.isDirectory()) {
            File[] content = myDir.listFiles();
            for (int i = 0; i < content.length; ++i) {
                fileDelete(content[i]);
            }
        }
        myDir.delete();
    }


}
