/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayDeque;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verifyNotNull;
import static java.util.Objects.requireNonNull;

public class ReversedTopNOperator
        implements Operator
{
    public static class ReversedTopNOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final int count;
        private boolean closed;

        public ReversedTopNOperatorFactory(int operatorId, PlanNodeId planNodeId, int count)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.count = count;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ReversedTopNOperator.class.getSimpleName());
            return new ReversedTopNOperator(operatorContext, count);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new ReversedTopNOperatorFactory(operatorId, planNodeId, count);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;

    // If the counts is very large, pages will consume more memory
    private final int count;
    private ArrayDeque<Page> pages = new ArrayDeque<>();
    private int totalPositions;
    private State state = State.NEEDS_INPUT;

    public ReversedTopNOperator(OperatorContext operatorContext, int count)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        checkArgument(count >= 0, "count must be at least zero");
        this.count = count;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
            int toRemove = totalPositions - count;
            if (toRemove > 0) {
                Page removedPage = pages.remove();
                Page remainingPage = removedPage.getRegion(toRemove, removedPage.getPositionCount() - toRemove);
                pages.offerFirst(remainingPage);
                totalPositions = count;
            }
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        pages.addLast(page);
        totalPositions += page.getPositionCount();
        while (!pages.isEmpty() && totalPositions - pages.peek().getPositionCount() >= count) {
            totalPositions -= pages.removeFirst().getPositionCount();
        }
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }
        verifyNotNull(pages, "pages is null");
        if (pages.isEmpty()) {
            state = State.FINISHED;
            return null;
        }
        return pages.removeFirst();
    }

    @Override
    public void close() throws Exception
    {
        pages = null;
    }
}
