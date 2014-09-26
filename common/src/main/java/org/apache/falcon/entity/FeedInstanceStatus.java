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
package org.apache.falcon.entity;

/**
 * Feed Instance Status is used to provide feed instance listing and corresponding status.
 *
 * This is used for exchanging information for getListing api
 */
public class FeedInstanceStatus {

    private String instance;

    private final String uri;

    private long creationTime;

    private long size = -1;

    private AvailabilityStatus status = AvailabilityStatus.MISSING;

    /**
     * Availability status of a feed instance.
     *
     * Missing if the feed partition is entirely missing,
     * Available if present and the availability flag is also present
     * Availability flag is setup, but availability flag is missing
     * Empty if the empty
     */
    public enum AvailabilityStatus {MISSING, AVAILABLE, PARTIAL, EMPTY}

    public FeedInstanceStatus(String uri) {
        this.uri = uri;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getUri() {
        return uri;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public AvailabilityStatus getStatus() {
        return status;
    }

    public void setStatus(AvailabilityStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "FeedInstanceStatus{"
                + "instance='" + instance + '\''
                + ", uri='" + uri + '\''
                + ", creationTime=" + creationTime
                + ", size=" + size
                + ", status='" + status + '\''
                + '}';
    }
}
