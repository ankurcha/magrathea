/**
 * Copyright 2013 Cloudera Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.brightcove.rna.bigtable.core;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class IdentityFieldPartitioner<S extends Comparable> extends FieldPartitioner<S, S> {

    public IdentityFieldPartitioner(String name, Class<S> type) {
        super(name, type, type);
        checkArgument(type.equals(Integer.class) || type.equals(Long.class) || type.equals(String.class), "Type not supported %s", type.toString());
    }

    @Override
    public S apply(S value) {
        return value;
    }

    @Override
    @Deprecated
    @SuppressWarnings("unchecked")
    public S valueFromString(String stringValue) {
        if (getType() == Integer.class) {
            return (S) Integer.valueOf(stringValue);
        } else if (getType() == Long.class) {
            return (S) Long.valueOf(stringValue);
        } else if (getType() == String.class) {
            return (S) stringValue;
        }
        throw new IllegalArgumentException("Cannot convert string to type " + getType());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !getClass().equals(o.getClass())) {
            return false;
        }
        IdentityFieldPartitioner that = (IdentityFieldPartitioner) o;
        return Objects.equal(this.getName(), that.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(S o1, S o2) {
        return o1.compareTo(o2);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getName());
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("name", getName()).toString();
    }
}
