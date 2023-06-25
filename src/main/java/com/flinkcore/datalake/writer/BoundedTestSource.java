package com.flinkcore.datalake.writer;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class BoundedTestSource<T> implements SourceFunction<T>, CheckpointListener {

  private final List<List<T>> elementsPerCheckpoint;
  private final boolean checkpointEnabled;
  private volatile boolean running = true;

  private final AtomicInteger numCheckpointsComplete = new AtomicInteger(0);

  /** Emits all those elements in several checkpoints. */
  public BoundedTestSource(List<List<T>> elementsPerCheckpoint, boolean checkpointEnabled) {
    this.elementsPerCheckpoint = elementsPerCheckpoint;
    this.checkpointEnabled = checkpointEnabled;
  }

  public BoundedTestSource(List<List<T>> elementsPerCheckpoint) {
    this(elementsPerCheckpoint, true);
  }

  /** Emits all those elements in a single checkpoint. */
  public BoundedTestSource(T... elements) {
    this(Collections.singletonList(Arrays.asList(elements)));
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    if (!checkpointEnabled) {
      Preconditions.checkArgument(
          elementsPerCheckpoint.size() <= 1,
          "There should be at most one list in the elementsPerCheckpoint when checkpoint is disabled.");
      elementsPerCheckpoint.stream().flatMap(List::stream).forEach(ctx::collect);
      return;
    }

    for (List<T> elements : elementsPerCheckpoint) {

      final int checkpointToAwait;
      synchronized (ctx.getCheckpointLock()) {
        // Let's say checkpointToAwait = numCheckpointsComplete.get() + delta, in fact the value of
        // delta should not
        // affect the final table records because we only need to make sure that there will be
        // exactly
        // elementsPerCheckpoint.size() checkpoints to emit each records buffer from the original
        // elementsPerCheckpoint.
        // Even if the checkpoints that emitted results are not continuous, the correctness of the
        // data should not be
        // affected in the end. Setting the delta to be 2 is introducing the variable that produce
        // un-continuous
        // checkpoints that emit the records buffer from elementsPerCheckpoints.
        checkpointToAwait = numCheckpointsComplete.get() + 2;
        for (T element : elements) {
          ctx.collect(element);
        }
      }

      synchronized (ctx.getCheckpointLock()) {
        while (running && numCheckpointsComplete.get() < checkpointToAwait) {
          ctx.getCheckpointLock().wait(1);
        }
      }
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    System.out.println("checkpoint completed...");
    numCheckpointsComplete.incrementAndGet();
  }

  @Override
  public void cancel() {
    running = false;
  }
}