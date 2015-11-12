/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.blobstore;

import backtype.storm.generated.KeyNotFoundException;

// Thrift Exceptions may not store the message where Throwable#getMessage can
// retrieve it. This class provides a {@link #getMessage()} that works.
public class KeyNotFoundMessageException extends KeyNotFoundException {
    private final String message;

    // Copy constructor that preserves the exception message for {@link #getMessage()}
    public KeyNotFoundMessageException(KeyNotFoundException knf) {
        super(knf); // Deep Copy
        this.message = knf.get_msg();
    }

    /**
     * Returns the detail message string of this KeyNotFoundMessageException.
     *
     * @return  the detail message string of this {@code KeyNotFoundMessageException} instance
     *          (which may be {@code null}).
     */
    @Override
    public String getMessage() {
        return this.message;
    }

    /**
     * Creates a KeyNotFoundMessageException from the given exception if
     * necessary.
     * @param e a KeyNotFoundException
     * @return the original exception if it was an instance of this class otherwise returns a new instance of this class created from e
     */
    public static KeyNotFoundException maybeWrapKeyNotFoundException(
            KeyNotFoundException e) {
        if (!(e instanceof KeyNotFoundMessageException)) {
            return new KeyNotFoundMessageException(e);
        } else {
            return e;
        }
    }
}
