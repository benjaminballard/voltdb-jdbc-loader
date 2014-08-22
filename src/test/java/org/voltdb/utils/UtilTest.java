package org.voltdb.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Properties;

import static junit.framework.TestCase.assertTrue;

/**
 * Util Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Mar 12, 2014</pre>
 */
public class UtilTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: filter(Collection<String> strings, String regExp)
     */
    @Test
    public void testFilterModules() throws Exception {
        Properties prop = new Properties();

        prop.put("MODULE1.Table1", "select * from Table1");
        prop.put("MODULE2.Table2", "select * from Table1");
        prop.put("MODULE3.Table3", "select * from Table1");
        prop.put("MODULE1.Table4", "select * from Table1");

        Collection<String> keys = Util.filter(prop.stringPropertyNames(), "^(MODULE1|MODULE2)");

        assertTrue(keys.size() == 3);
        assertTrue(keys.contains("MODULE1.Table1") && keys.contains("MODULE1.Table4") && keys.contains("MODULE2.Table2"));
    }

    /**
     * Method: filter(Collection<String> strings, String regExp)
     */
    @Test
    public void testFilterTables() throws Exception {
        Properties prop = new Properties();

        prop.put("MODULE1.Table1", "select * from Table1");
        prop.put("MODULE2.Table2", "select * from Table1");
        prop.put("MODULE3.Table3", "select * from Table1");
        prop.put("MODULE1.Table4", "select * from Table1");

        Collection<String> keys = Util.filter(prop.stringPropertyNames(), "^(MODULE1|MODULE2)");
        keys = Util.filter(keys, "(Table1|Table3)");

        assertTrue(keys.size() == 1);
        assertTrue(keys.contains("MODULE1.Table1") && !keys.contains("MODULE3.Table3") && !keys.contains("MODULE2.Table2"));
    }
} 
