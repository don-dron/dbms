package ru.bmstu.iu9.db.zvoa.dbms.storage;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;

public class TestValue implements Value {
    public String name;
    public Integer age;

    public TestValue() {
    }

    public TestValue(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public Integer getAge() {
        return age;
    }

    public String getName() {
        return name;
    }
}
