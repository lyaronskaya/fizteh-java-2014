package ru.fizteh.fivt.students.YaronskayaLiubov.StructuredDataTables;

import ru.fizteh.fivt.storage.structured.TableProvider;
import ru.fizteh.fivt.storage.structured.TableProviderFactory;

import java.io.IOException;

/**
 * Created by luba_yaronskaya on 16.11.14.
 */

/**
 * Представляет интерфейс для создание экземпляров {@link TableProvider}.
 * <p/>
 * Предполагается, что реализация интерфейса фабрики будет иметь публичный конструктор без параметров.
 */
public class StoreableDataTableProviderFactory implements TableProviderFactory {
    /**
     * Возвращает объект для работы с базой данных.
     *
     * @param path Директория с файлами базы данных.
     * @return Объект для работы с базой данных, который будет работать в указанной директории.
     * @throws IllegalArgumentException Если значение директории null или имеет недопустимое значение.
     * @throws java.io.IOException      В случае ошибок ввода/вывода.
     */
    @Override
    public TableProvider create(String path) throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("Directory is null");
        }
        if (path.isEmpty()) {
            throw new IllegalArgumentException("Empty directory name");
        }
        return new StoreableDataTableProvider(path);
    }
}
