package persistence;

import org.junit.jupiter.api.Test;
import redis.config.RedisConfig;
import redis.persistence.DumpFileContent;
import redis.persistence.DumpFileReader;
import redis.persistence.NoopDumpFileReader;
import redis.persistence.PersistentFileReader;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

class DumpFileReaderTest {

    @Test
    void testReadDumpFile() {
        URL resource = this.getClass().getResource("/dump.rdb");
        PersistentFileReader reader = resource == null
                ? new NoopDumpFileReader()
                : new DumpFileReader(new RedisConfig(new String[]{"--dir", resource.getPath(), "--dbfilename", ""}));
        DumpFileContent content = reader.read();
        if (reader instanceof DumpFileReader) {
            assertThat(content.keys()).containsExactlyInAnyOrder(
                    "key", "key1", "key2", "key3", "key4", "key5"
            );
        }
    }

}