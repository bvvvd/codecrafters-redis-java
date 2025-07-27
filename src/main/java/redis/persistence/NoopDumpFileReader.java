package redis.persistence;

import java.util.Collections;

public class NoopDumpFileReader implements PersistentFileReader {
    @Override
    public DumpFileContent read() {
        return new DumpFileContent(Collections.emptyMap());
    }
}
