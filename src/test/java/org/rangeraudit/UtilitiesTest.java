package org.rangeraudit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;
import net.sourceforge.argparse4j.inf.Namespace;
public class UtilitiesTest {
    @Test
    public void testGetUserInputsWithAllCustomized() {
        String[] args = {
                "--cloud_type", "aws",
                "--storage_location", "my-bucket/test",
                "--solr_path", "test:123",
                "--days_ago", "8",
                "--access_key_id", "accessKeyId",
                "--secret_access_key", "secretAccessKey",
                "--jaas_conf_path", "/path/to/conf",
                "--region", "us-east-1",
                "--threads", "2",
                "--documents_per_batch", "500"
        };
        Namespace inputs = Utilities.getUserInputs(args);
        assertEquals("aws", inputs.get("cloud_type"));
        assertEquals("my-bucket/test", inputs.get("storage_location"));
        assertEquals("test:123", inputs.get("solr_path"));
        assertEquals(8, (Object)inputs.get("days_ago"));
        assertEquals("accessKeyId", inputs.get("access_key_id"));
        assertEquals("secretAccessKey", inputs.get("secret_access_key"));
        assertEquals("/path/to/conf", inputs.get("jaas_conf_path"));
        assertEquals("us-east-1", inputs.get("region"));
        assertEquals(2, (Object) inputs.get("threads"));
        assertEquals((Object) 500, inputs.get("documents_per_batch"));
    }
    @Test
    public void testGetUserInputsWithSomeDefaults() {
        String[] args = {
                "--cloud_type", "aws",
                "--storage_location", "my-bucket/test",
                "--solr_path", "test:123",
                "--days_ago", "8",
                "--access_key_id", "accessKeyId"
        };
        Namespace inputs = Utilities.getUserInputs(args);
        assertEquals("/run/cloudera-scm-agent/process",
                inputs.get("jaas_conf_path"));
        assertEquals(null, inputs.get("secret_access_key"));
        assertEquals(null, inputs.get("region"));
        assertEquals(1, (Object) inputs.get("threads"));
        assertEquals((Object) 1000, inputs.get("documents_per_batch"));
    }
    @Test
    public void testGetDaysAgoDate() {
        LocalDate todayDate = LocalDate.of(2023, 1, 5);
        LocalDate twoDaysAgo = LocalDate.of(2023, 1, 3);
        LocalDate thirtyDaysAgo = LocalDate.of(2022, 12,
                6);
        assertEquals(twoDaysAgo, todayDate.minusDays(2));
        assertEquals(thirtyDaysAgo, todayDate.minusDays(30));

    }
    @Test
    public void testIsDateStr() {
        assertTrue(Utilities.isDateStr("20220506"));
        assertFalse(Utilities.isDateStr("testFolder"));
        assertFalse(Utilities.isDateStr("202205"));
    }
}

