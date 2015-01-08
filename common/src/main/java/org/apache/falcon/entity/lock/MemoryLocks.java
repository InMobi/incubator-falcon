/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.falcon.entity.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In memory resource locking that provides lock capabilities.
 */
public class MemoryLocks {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryLocks.class);
    private Map<String, ReentrantLock> locks = new ConcurrentHashMap<String, ReentrantLock>();

    /**
     * Token object is issued when an entity update is invoked.
     */
    public class LockToken {

        private final ReentrantLock lock;
        private final String entityName;

        public String getEntityName() {
            return entityName;
        }

        public ReentrantLock getLock() {
            return lock;
        }

        LockToken(String entityName, ReentrantLock lock) {
            this.entityName = entityName;
            this.lock = lock;
        }

        /**
         * Release the lock.
         */
        public void release() {
            synchronized (locks) {
                locks.remove(this.getEntityName());

                this.getLock().unlock();
            }
        }
    }


    /**
     * Return the number of active locks.
     *
     * @return the number of active locks.
     */
    public int size() {
        return locks.size();
    }

    /**
     * Obtain a lock for a source.
     *
     * @param entityName resource name.
     * @return the lock token for the resource, or <code>null</code> if the lock could not be obtained.
     * @throws InterruptedException thrown if the thread was interrupted while waiting.
     */
    /**
     * Acquire a lock for a given entity name.
     */
    public LockToken getLock(String entityName) throws InterruptedException {
        ReentrantLock lockEntry;

        synchronized (locks) {
            if (locks.containsKey(entityName)) {
                return null;
            } else {
                lockEntry = new ReentrantLock(true);
                locks.put(entityName, lockEntry);
            }
            if (lockEntry.tryLock()) {
                LOG.info("Lock obtained for update of " + entityName + " by " + Thread.currentThread().getName());
                return new LockToken(entityName, lockEntry);
            } else {
                return null;
            }

        }
    }

}
