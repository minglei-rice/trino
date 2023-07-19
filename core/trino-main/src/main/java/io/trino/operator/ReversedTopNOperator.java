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
import io.trino.spi.block.Block;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayDeque;
import java.util.Iterator;

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
        private final long count;
        private boolean closed;

        public ReversedTopNOperatorFactory(int operatorId, PlanNodeId planNodeId, long count)
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
    private long totalPositions;
    private Iterator<Page> pageIterator;

    private State state = State.NEEDS_INPUT;

    public ReversedTopNOperator(OperatorContext operatorContext, long count)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        checkArgument(count >= 0, "count must be at least zero");
        this.count = (int) count;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    private Page getPageLastRows(Page page, int positionOffset, int length)
    {
        return page.getRegion(positionOffset, length);
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
            while (totalPositions > count) {
                // The number of records of each page in the queue is less than or equal to counts
                Page removedPage = pages.removeFirst();
                totalPositions -= removedPage.getPositionCount();
                if (totalPositions < count) {
                    int reservedInRemovedPage = (int) (count - totalPositions);
                    Page remainingPages = getPageLastRows(removedPage, removedPage.getPositionCount() - reservedInRemovedPage, reservedInRemovedPage);
                    totalPositions += remainingPages.getPositionCount();
                    pages.addLast(remainingPages);
                }
            }
            pageIterator = pages.iterator();
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
        if (page.getPositionCount() > count) {
            Page remainingPage = getPageLastRows(page, page.getPositionCount() - count, count);
            pages.clear();
            pages.addLast(remainingPage);
            totalPositions = remainingPage.getPositionCount();
        }
        else {
            pages.addLast(page);
            totalPositions += page.getPositionCount();
        }
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }
        verifyNotNull(pages, "pages is null");
        if (!pageIterator.hasNext()) {
            state = State.FINISHED;
            return null;
        }
        Page nextPage = pageIterator.next();
        int channelCount = nextPage.getChannelCount();
        Block[] blocks = new Block[channelCount];
        for (int i = 0; i < channelCount; i++) {
            blocks[i] = nextPage.getBlock(i);
        }
        return new Page(nextPage.getPositionCount(), blocks);
    }

    @Override
    public void close() throws Exception
    {
        pages = null;
    }
}
