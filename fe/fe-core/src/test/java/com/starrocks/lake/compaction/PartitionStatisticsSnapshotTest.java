// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.lake.compaction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionStatisticsSnapshotTest {
    @Test
    public void testBasic() {
        PartitionStatistics statistics = new PartitionStatistics(new PartitionIdentifier(100, 200, 300));
        Quantiles q1 = new Quantiles(1.0, 2.0, 3.0);
        statistics.setCompactionScore(q1);
        statistics.resetPriority();
        PartitionStatisticsSnapshot stat = new PartitionStatisticsSnapshot(statistics);
        Assertions.assertEquals(stat.getPartition(), statistics.getPartition());
        Assertions.assertEquals(stat.getPriority(), statistics.getPriority());
        Assertions.assertTrue(stat.getCompactionScore().compareTo(statistics.getCompactionScore()) == 0);

        // change does not affect `stat`
        Quantiles q2 = new Quantiles(4.0, 5.0, 6.0);
        statistics.setCompactionScore(q2);
        statistics.setPriority(PartitionStatistics.CompactionPriority.MANUAL_COMPACT);
        Assertions.assertNotEquals(stat.getPriority(), statistics.getPriority());
        Assertions.assertFalse(stat.getCompactionScore().compareTo(statistics.getCompactionScore()) == 0);

        stat.toString();
    }
}
