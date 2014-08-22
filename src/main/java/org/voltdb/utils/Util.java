package org.voltdb.utils;

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;

import java.util.Collection;

/**
 * Created by kunkunur on 3/12/14.
 */
public class Util {
    public static Collection<String> filter(Collection<String> strings, String regExp){
        return Collections2.filter(strings, Predicates.containsPattern(regExp));
    }
}
