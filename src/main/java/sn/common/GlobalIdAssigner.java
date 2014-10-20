package sn.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Sumanth on 17/10/14.
 */
public class GlobalIdAssigner {
    //there won't be restarts with in milliseconds
    private AtomicLong counter = new AtomicLong(System.currentTimeMillis());
    private static GlobalIdAssigner ourInstance = new GlobalIdAssigner();

    public static GlobalIdAssigner getInstance() {
        return ourInstance;
    }

    private GlobalIdAssigner() {
    }

    public long getId(){
        return counter.incrementAndGet();
    }



}
