package bid.adonis.lau.hbase.phoenix.jdbc.test;

import bid.adonis.lau.hbase.phoenix.jdbc.PhoenixJdbc;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * @Author adonis
 * @Date 2019-07-03 11:49
 * @Description
 */
@Slf4j
public class PhoenixJdbcTest {

    @Test
    public void delete() {
        PhoenixJdbc.dropTable("phoenix_test");
        PhoenixJdbc.dropSequence("PHOENIX_TEST_SEQ_ID");
    }
}
