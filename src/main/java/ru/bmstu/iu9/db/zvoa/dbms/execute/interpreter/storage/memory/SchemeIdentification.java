/*
 * Copyright (c) 2021 Zvorygin Andrey BMSTU IU-9 https://github.com/don-dron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 * OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
 * OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;

import java.util.Arrays;
import java.util.List;

public class SchemeIdentification extends Key {
    private String name;

    public SchemeIdentification(String name) {
        this.name = name;
    }

    public SchemeIdentification(List<Object> list) {
        buildFromMemory(list);
    }

    @Override
    public int compareTo(Object o) {
        return name.compareTo(((SchemeIdentification) o).name);
    }

    @Override
    public List<Type> getTypes() {
        return Arrays.asList(Type.STRING);
    }

    @Override
    public List<Object> toObjects() {
        return Arrays.asList(name);
    }

    @Override
    public void buildFromMemory(List<Object> objects) {
        name = (String) objects.get(0);
    }
}
