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

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class EmptyValueTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.enablePruneEmptyOutputScan = true;
        FeConstants.runningUnitTest = true;
    }

    @AfterAll
    public static void afterClass() {
        FeConstants.enablePruneEmptyOutputScan = false;
        PlanTestBase.afterClass();
    }

    @Test
    public void testPartitionCrossJoin() throws Exception {
        String sql = "select * from lineitem_partition p join t1 join t2 join t3 where L_SHIPDATE = '2000-01-01' ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET");

        sql = "select * from lineitem_partition p join t0 on p.L_ORDERKEY = t0.v2 where L_SHIPDATE = '2000-01-01' ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET");
    }

    @Test
    public void testPruneEmptyJoinWithAssociate() throws Exception {
        String sql = "WITH shipment_data AS (\n" +
                "    SELECT\n" +
                "        L_RECEIPTDATE,\n" +
                "        L_ORDERKEY,\n" +
                "        L_SUPPKEY,\n" +
                "        L_COMMENT,\n" +
                "        L_SHIPMODE,\n" +
                "        L_QUANTITY,\n" +
                "        L_SHIPINSTRUCT\n" +
                "    FROM\n" +
                "        lineitem_partition\n" +
                "    WHERE \n" +
                "        L_SHIPDATE BETWEEN '1995-03-09' AND '1995-03-09'\n" +
                "        AND L_PARTKEY IS NOT NULL\n" +
                "),\n" +
                "supplier_data AS (\n" +
                "    SELECT \n" +
                "        L_COMMENT,\n" +
                "        L_RETURNFLAG \n" +
                "    FROM \n" +
                "        lineitem_partition\n" +
                "    WHERE \n" +
                "        L_SHIPDATE = '2020-01-01'\n" +
                "        AND L_LINESTATUS = 'O'\n" +
                "),\n" +
                "part_data AS (\n" +
                "    SELECT \n" +
                "        L_PARTKEY,\n" +
                "        L_LINENUMBER,\n" +
                "        L_DISCOUNT \n" +
                "    FROM \n" +
                "        lineitem_partition\n" +
                "    WHERE \n" +
                "        L_SHIPMODE = 'AIR'\n" +
                "),\n" +
                "customer_data AS (\n" +
                "    SELECT\n" +
                "        L_SUPPKEY,\n" +
                "        L_TAX,\n" +
                "        L_EXTENDEDPRICE\n" +
                "    FROM\n" +
                "        lineitem_partition\n" +
                "    WHERE\n" +
                "        L_RETURNFLAG = 'N'\n" +
                ")\n" +
                "SELECT\n" +
                "    shipment_data.L_RECEIPTDATE,\n" +
                "    shipment_data.L_ORDERKEY,\n" +
                "    supplier_data.L_RETURNFLAG,\n" +
                "    part_data.L_DISCOUNT\n" +
                "FROM\n" +
                "    shipment_data\n" +
                "    LEFT JOIN customer_data ON customer_data.L_SUPPKEY = shipment_data.L_SUPPKEY\n" +
                "    LEFT JOIN supplier_data ON supplier_data.L_COMMENT = shipment_data.L_COMMENT\n" +
                "    LEFT JOIN part_data ON part_data.L_PARTKEY = CAST(shipment_data.L_SHIPMODE AS INT)\n" +
                "        AND part_data.L_LINENUMBER = CAST(shipment_data.L_QUANTITY AS INT)\n" +
                "        AND part_data.L_DISCOUNT = CAST(shipment_data.L_SHIPINSTRUCT AS DOUBLE)\n" +
                "WHERE\n" +
                "    supplier_data.L_RETURNFLAG != 'R'\n" +
                "    OR part_data.L_DISCOUNT != 0.05\n";

        String plan = getFragmentPlan(sql);
        assertCContains(plan, "other predicates: (43: L_RETURNFLAG != 'R') OR (58: L_DISCOUNT != 0.05)");
    }

    @Test
    public void testPartitionOtherJoin() throws Exception {
        String sql = "select L_PARTKEY, t0.v2 from lineitem_partition p " +
                "left outer join t0 on p.L_ORDERKEY = t0.v2 where L_SHIPDATE = '2000-01-01' ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n  0:EMPTYSET");

        sql = "select L_PARTKEY, t0.v2 from lineitem_partition p " +
                "right outer join t0 on p.L_ORDERKEY = t0.v2 where L_SHIPDATE = '2000-01-01' ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n  0:EMPTYSET");

        sql = "select L_PARTKEY, t0.v2 from t0 left outer join " +
                "(select * from lineitem_partition p where L_SHIPDATE = '2000-01-01') x on x.L_ORDERKEY = t0.v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 5> : NULL\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");

        sql = "select L_PARTKEY, t0.v2 from t0 right outer join " +
                "(select * from lineitem_partition p where L_SHIPDATE = '2000-01-01') x on x.L_ORDERKEY = t0.v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n  0:EMPTYSET");

        sql = "select L_PARTKEY from lineitem_partition p " +
                "left semi join t0 on p.L_ORDERKEY = t0.v2 where L_SHIPDATE = '2000-01-01' ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n  0:EMPTYSET");

        sql = "select t0.v2 from t0 left semi join " +
                "(select * from lineitem_partition p where L_SHIPDATE = '2000-01-01') x on x.L_ORDERKEY = t0.v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n  0:EMPTYSET");

        sql = "select L_PARTKEY from t0 right semi join " +
                "(select * from lineitem_partition p where L_SHIPDATE = '2000-01-01') x on x.L_ORDERKEY = t0.v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n  0:EMPTYSET");

        sql = "select L_PARTKEY from lineitem_partition p " +
                "left anti join t0 on p.L_ORDERKEY = t0.v2 where L_SHIPDATE = '2000-01-01' ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n  0:EMPTYSET");

        sql = "select L_PARTKEY from t0 right anti join " +
                "(select * from lineitem_partition p where L_SHIPDATE = '2000-01-01') x on x.L_ORDERKEY = t0.v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n  0:EMPTYSET");

        sql = "select t0.v2 from t0 left anti join " +
                "(select * from lineitem_partition p where L_SHIPDATE = '2000-01-01') x on x.L_ORDERKEY = t0.v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: t0");

        sql = "select L_PARTKEY, t0.v2 from lineitem_partition p " +
                "full outer join t0 on p.L_ORDERKEY = t0.v2 where L_SHIPDATE = '2000-01-01' ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RESULT SINK\n" +
                "\n  0:EMPTYSET");

        sql = "select L_PARTKEY, t0.v2 from t0 full outer join " +
                "(select * from lineitem_partition p where L_SHIPDATE = '2000-01-01') x on x.L_ORDERKEY = t0.v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 5> : NULL\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testAggregate() throws Exception {
        String sql = "select sum(1) from lineitem_partition p where L_SHIPDATE = '2000-01-01' group by L_PARTKEY";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:EMPTYSET");

        sql = "select sum(L_PARTKEY) from lineitem_partition p where L_SHIPDATE = '2000-01-01' ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: L_PARTKEY)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:EMPTYSET");
    }

    @Test
    public void testCTE() throws Exception {
        String sql = "with x as (select * from lineitem_partition p where L_SHIPDATE = '2000-01-01')" +
                "select * from " +
                "x x1 join x x2 on x1.L_ORDERKEY = x2.L_ORDERKEY" +
                "     join x x3 on x1.L_ORDERKEY = x3.L_ORDERKEY" +
                "     join x x4 on x1.L_ORDERKEY = x4.L_ORDERKEY" +
                "     join x x5 on x1.L_ORDERKEY = x5.L_ORDERKEY";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:EMPTYSET");

        connectContext.getSessionVariable().setCboCTERuseRatio(-1);
        plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setCboCTERuseRatio(1.5);
        assertContains(plan, "0:EMPTYSET");

        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setCboCTERuseRatio(1.5);
        assertContains(plan, "0:EMPTYSET");
    }

    @Test
    public void testOther() throws Exception {
        String sql = "select *, (select L_LINENUMBER from lineitem_partition p where L_SHIPDATE = '2000-01-01') x " +
                "from t0;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:ASSERT NUMBER OF ROWS\n" +
                "  |  assert number of rows: LE 1\n" +
                "  |  \n" +
                "  1:EMPTYSET");
    }

    @Test
    public void testUnionSlot() throws Exception {
        String sql = "select L_ORDERKEY " +
                "from t0 left outer join lineitem_partition on L_ORDERKEY = t0.v2 and L_SHIPDATE = '2000-01-01' " +
                "union all " +
                "select L_ORDERKEY " +
                "from t0 left outer join lineitem_partition on L_ORDERKEY = t0.v2 and L_SHIPDATE = '2000-01-01' ";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  0:UNION\n" +
                "  |  output exprs:\n" +
                "  |      [41, INT, true]\n" +
                "  |  child exprs:\n" +
                "  |      [4: L_ORDERKEY, INT, true]\n" +
                "  |      [24: L_ORDERKEY, INT, true]");
    }

    @Test
    public void testRemoveUnion() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000000);
        String sql = "select \n" +
                "v1,\n" +
                "name1,\n" +
                "name2,\n" +
                "max(v3)\n" +
                "from\n" +
                "(\n" +
                "select v1, coalesce(v2, 1) as name1, coalesce(v2, 1) as name2, max(v3) v3, max(v4)  " +
                "from (select v1, v2 from t0) t1 join (select v3, v1 as v4 from t0) t2 group by 1, 2, 3\n" +
                "union all\n" +
                "select v1, v2, v2 + 1, v3, v4  from (select v1, v2 from t0) t1 join " +
                "(select v1 v4, v3 from t0 where 1 > 2) t2\n" +
                ") t\n" +
                "group by 1, 2, 3;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "8:Project\n" +
                "  |  <slot 17> : 17: v1\n" +
                "  |  <slot 18> : 18: coalesce\n" +
                "  |  <slot 19> : clone(18: coalesce)\n" +
                "  |  <slot 22> : 22: max\n" +
                "  |  \n" +
                "  7:AGGREGATE (update finalize)\n" +
                "  |  output: max(20: max)\n" +
                "  |  group by: 17: v1, 18: coalesce\n" +
                "  |  \n" +
                "  6:Project\n" +
                "  |  <slot 17> : 1: v1\n" +
                "  |  <slot 18> : 7: coalesce\n" +
                "  |  <slot 20> : 8: max\n" +
                "  |  \n" +
                "  5:AGGREGATE (update finalize)\n" +
                "  |  output: max(6: v3)\n" +
                "  |  group by: 1: v1, 7: coalesce");
    }

    @Test
    public void testOuterPredicate() throws Exception {
        String sql = "select t0.v2 from t0 full outer join " +
                "(select * from lineitem_partition p where L_SHIPDATE = '2000-01-01') x on x.L_ORDERKEY = t0.v2" +
                " where (t0.v3 + 1) is NULL";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:SELECT\n" +
                "  |  predicates: 3: v3 + 1 IS NULL");

        sql = "select t0.v2 from t0 left outer join " +
                "(select * from lineitem_partition p where L_SHIPDATE = '2000-01-01') x on x.L_ORDERKEY = t0.v2" +
                " where (x.L_SUPPKEY + 1) is NULL";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:SELECT\n" +
                "  |  predicates: CAST(6: L_SUPPKEY AS BIGINT) + 1 IS NULL\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 6> : NULL\n" +
                "  |  <slot 21> : NULL");

        sql = "select t0.v2 from t0 right outer join " +
                "(select * from lineitem_partition p where L_SHIPDATE = '2000-01-01') x on x.L_ORDERKEY = t0.v2" +
                " where (x.L_SUPPKEY + 1) is NULL";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET");

        sql = "select t0.v2 from (select * from lineitem_partition p where L_SHIPDATE = '2000-01-01') x " +
                " right outer join t0 on x.L_ORDERKEY = t0.v2" +
                " where (x.L_SUPPKEY + 1) is NULL";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:SELECT\n" +
                "  |  predicates: CAST(3: L_SUPPKEY AS BIGINT) + 1 IS NULL\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 3> : NULL\n" +
                "  |  <slot 19> : 19: v2\n" +
                "  |  <slot 21> : NULL");
    }
}
