package sn.common.def;

import java.io.Serializable;

/**
 * Created by Sumanth on 20/10/14.
 */
public abstract class AbstractDef implements Serializable {
    protected final String uuid;
    protected String name;

    protected AbstractDef(String uuid) {
        this.uuid = uuid;
    }

    protected AbstractDef(String uuid, String name) {
        this.uuid = uuid;
        this.name = name;
    }

    public String getUuid() {
        return uuid;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


}
