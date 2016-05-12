package edu.washington.escience.myria.operator.network;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.jboss.netty.channel.ChannelFuture;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SimpleAppender;
import edu.washington.escience.myria.operator.StreamingState;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.LocalFragmentResourceManager;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCEvent;
import edu.washington.escience.myria.parallel.ipc.IPCEventListener;
import edu.washington.escience.myria.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A Producer is the counterpart of a consumer. It dispatch data using IPC channels to Consumers. Like network socket,
 * Each (workerID, operatorID) pair is a logical destination.
 */
public abstract class Producer extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The worker this operator is located at. */
  private transient LocalFragmentResourceManager taskResourceManager;

  /** the Netty channels doing the true IPC IO. */
  private transient StreamOutputChannel<TupleBatch>[] ioChannels;

  /** if the corresponding ioChannel is available to write again. */
  private transient boolean[] ioChannelsAvail;

  /** tried to send tuples for each channel. */
  private List<StreamingState> triedToSendTuples;

  /** pending tuples to be sent for each channel. */
  private transient List<LinkedList<TupleBatch>> pendingTuplesToSend;

  /** output stream channel IDs. */
  private transient StreamIOChannelID[] outputIds;

  /** if current query execution is in non-blocking mode. */
  private transient boolean nonBlockingExecution;

  /** number of channels, should be operatorId.length * destinationWorkerIds.length. */
  private int numOfChannels;

  /** operator IDs. */
  private final ExchangePairID[] operatorIds;

  /** destination worker IDs. */
  private final int[] destinationWorkerIds;

  /**
   * @param child the child providing data.
   * @param operatorIds the operator IDs.
   * @param destinationWorkerIds the worker IDs.
   */
  public Producer(final Operator child, final ExchangePairID[] operatorIds, final int[] destinationWorkerIds) {
    super(child);
    this.operatorIds = operatorIds;
    this.destinationWorkerIds = destinationWorkerIds;
    setNumOfChannels(operatorIds.length * destinationWorkerIds.length);
  }

  @SuppressWarnings("unchecked")
  @Override
  public final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    taskResourceManager = (LocalFragmentResourceManager) execEnvVars.get(
        MyriaConstants.EXEC_ENV_VAR_FRAGMENT_RESOURCE_MANAGER);
    setSelfOutputId(taskResourceManager.getNodeId());
    ioChannels = new StreamOutputChannel[outputIds.length];
    ioChannelsAvail = new boolean[outputIds.length];
    if (triedToSendTuples == null) {
      setBackupBuffer(new SimpleAppender());
    }

    pendingTuplesToSend = new ArrayList<LinkedList<TupleBatch>>();
    for (int i = 0; i < outputIds.length; i++) {
      createANewChannel(i);
      pendingTuplesToSend.add(new LinkedList<TupleBatch>());
      triedToSendTuples.get(i).init(null);
    }
    nonBlockingExecution = (execEnvVars.get(
        MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE) == QueryExecutionMode.NON_BLOCKING);
  }

  /**
   * Does all the jobs needed to create a new channel with index i.
   * 
   * @param i the index of the channel
   */
  public void createANewChannel(final int i) {
    ioChannels[i] = taskResourceManager.startAStream(outputIds[i].getRemoteID(), outputIds[i].getStreamID());
    ioChannels[i].addListener(StreamOutputChannel.OUTPUT_DISABLED, new IPCEventListener() {
      @Override
      public void triggered(final IPCEvent event) {
        taskResourceManager.getFragment().notifyOutputDisabled(outputIds[i]);
      }
    });
    ioChannels[i].addListener(StreamOutputChannel.OUTPUT_RECOVERED, new IPCEventListener() {
      @Override
      public void triggered(final IPCEvent event) {
        taskResourceManager.getFragment().notifyOutputEnabled(outputIds[i]);
      }
    });
    ioChannelsAvail[i] = true;
  }

  /**
   * 
   * @param state the backup buffer.
   */
  public void setBackupBuffer(final StreamingState state) {
    triedToSendTuples = new ArrayList<StreamingState>();
    for (int i = 0; i < destinationWorkerIds.length * operatorIds.length; i++) {
      triedToSendTuples.add(i, state.newInstanceFromMyself());
      triedToSendTuples.get(i).setAttachedOperator(this);
    }
  }

  /** the number of tuples written to channels. */
  private long numTuplesWrittenToChannels = 0;

  /**
   * @param chIdx the channel to write
   * @param msg the message.
   * @return write future
   */
  protected final ChannelFuture writeMessage(final int chIdx, final TupleBatch msg) {
    StreamOutputChannel<TupleBatch> ch = ioChannels[chIdx];
    if (nonBlockingExecution) {
      numTuplesWrittenToChannels += msg.numTuples();
      return ch.write(msg);
    } else {
      int sleepTime = 1;
      int maxSleepTime = MyriaConstants.SHORT_WAITING_INTERVAL_MS;
      while (true) {
        if (ch.isWritable()) {
          numTuplesWrittenToChannels += msg.numTuples();
          return ch.write(msg);
        } else {
          int toSleep = sleepTime - 1;
          if (maxSleepTime < sleepTime) {
            toSleep = maxSleepTime;
          }
          try {
            Thread.sleep(toSleep);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
          }
          sleepTime *= 2;
        }
      }
    }
  }

  /**
   * Pop tuple batches from each of the buffers and try to write them to corresponding channels, if possible.
   * 
   * @param tb the tuple batch.
   * @param pf the partition function.
   * @throws DbException if getting profiling logger had error.
   */
  protected final void distribute(final TupleBatch tb, final PartitionFunction pf) throws DbException {
    /**
     * Note: another possible implementation is to compact small tuple batches (partitions) into a TupleBatchBuffer and
     * only pop one tuple batch when it is full or after a certain timeout. An early experiment showed that this design
     * slows things down due to both the CPU cycles spent on read/copy tuples and also the network delays so we do not
     * have it here. {#link TupleBatchBuffer#absorb} and {#link TupleBatchBuffer#popAnyUsingTimeout} are kept for this
     * reason.
     */
    TupleBatch[] partitions = null;
    if (tb != null && pf != null) {
      partitions = tb.partition(pf);
    }
    for (int i = 0; i < numOfChannels; ++i) {
      if (partitions != null) {
        pendingTuplesToSend.get(i).add(partitions[i]);
      }
      if (!ioChannelsAvail[i] && (getFTMode().equals(FTMode.ABANDON) || getFTMode().equals(FTMode.REJOIN))) {
        continue;
      }
      while (true) {
        TupleBatch toSend = pendingTuplesToSend.get(i).poll();
        if (toSend == null) {
          break;
        }
        if (getFTMode().equals(FTMode.REJOIN) && !(this instanceof LocalMultiwayProducer)) {
          // rejoin, append the TB into the backup buffer in case of recovering
          toSend = triedToSendTuples.get(i).update(toSend);
          if (toSend == null) {
            break;
          }
        }
        try {
          writeMessage(i, toSend);
          if (getProfilingMode().contains(ProfilingMode.QUERY)) {
            final int destWorkerId = getOutputChannelIds()[i].getRemoteID();
            getProfilingLogger().recordSent(this, toSend.numTuples(), destWorkerId);
          }
        } catch (IllegalStateException e) {
          if (getFTMode().equals(FTMode.ABANDON) || getFTMode().equals(FTMode.REJOIN)) {
            ioChannelsAvail[i] = false;
            break;
          } else {
            throw e;
          }
        }
      }
    }
  }

  /**
   * @return the number of tuples in all buffers.
   */
  public final long getNumTuplesInBuffers() {
    long sum = 0;
    for (StreamingState state : triedToSendTuples) {
      sum += state.numTuples();
    }
    return sum;
  }

  /**
   * @param chIdx the channel to write
   * @return channel release future.
   */
  protected final ChannelFuture channelEnds(final int chIdx) {
    if (ioChannelsAvail[chIdx]) {
      return ioChannels[chIdx].release();
    }
    return null;
  }

  @Override
  public final void cleanup() throws DbException {
    for (int i = 0; i < outputIds.length; i++) {
      if (ioChannels[i] != null) {
        /* RecoverProducer may detach & set its channel to be null, shouldn't call release here */
        ioChannels[i].release();
      }
    }
  }

  /** @return output channel IDs. */
  public StreamIOChannelID[] getOutputChannelIds() {
    return outputIds;
  }

  /**
   * 
   * @param myId my worker ID.
   */
  public void setSelfOutputId(final int myId) {
    if (outputIds == null) {
      outputIds = new StreamIOChannelID[operatorIds.length * destinationWorkerIds.length];
      int idx = 0;
      for (int wId : destinationWorkerIds) {
        for (ExchangePairID oId : operatorIds) {
          outputIds[idx] = new StreamIOChannelID(oId.getLong(), wId);
          idx++;
        }
      }
    }
    for (StreamIOChannelID id : outputIds) {
      if (id.getRemoteID() == IPCConnectionPool.SELF_IPC_ID) {
        id.setRemoteID(myId);
      }
    }
  }

  /**
   * @return The resource manager of the running task.
   */
  protected LocalFragmentResourceManager getTaskResourceManager() {
    return taskResourceManager;
  }

  /**
   * enable/disable output channels that belong to the worker.
   * 
   * @param workerId the worker that changed its status.
   * @param enable enable/disable all the channels that belong to the worker.
   */
  public final void updateChannelAvailability(final int workerId, final boolean enable) {
    List<Integer> indices = getChannelIndicesOfAWorker(workerId);
    for (int i : indices) {
      ioChannelsAvail[i] = enable;
    }
  }

  /**
   * return the backup buffers.
   * 
   * @return backup buffers.
   */
  public final List<StreamingState> getTriedToSendTuples() {
    return triedToSendTuples;
  }

  /**
   * return the indices of the channels that belong to the worker.
   * 
   * @param workerId the id of the worker.
   * @return the list of channel indices.
   */
  public final List<Integer> getChannelIndicesOfAWorker(final int workerId) {
    List<Integer> ret = new ArrayList<Integer>();
    for (int i = 0; i < numOfChannels; ++i) {
      if (ioChannels[i].getID().getRemoteID() == workerId) {
        ret.add(i);
      }
    }
    return ret;
  }

  /**
   * @return the channel availability array.
   */
  public boolean[] getChannelsAvail() {
    return ioChannelsAvail;
  }

  /**
   * @return the channel array.
   */
  public StreamOutputChannel<TupleBatch>[] getChannels() {
    return ioChannels;
  }

  /**
   * process EOS and EOI logic.
   */
  @Override
  protected final void checkEOSAndEOI() {
    Operator child = getChild();
    if (child.eoi()) {
      setEOI(true);
      child.setEOI(false);
    } else if (child.eos()) {
      if (getFTMode().equals(FTMode.REJOIN)) {
        for (List<TupleBatch> tbs : pendingTuplesToSend) {
          if (tbs.size() > 0) {
            // due to failure, buffers are not empty, this task needs to be executed again to push these TBs out when
            // channels are available
            return;
          }
        }
      }
      // all buffers are empty, ready to end this task
      setEOS();
    }
  }

  /**
   * set the number of partitions.
   * 
   * @param num the number
   */
  public void setNumOfChannels(final int num) {
    numOfChannels = num;
  }

  /**
   * 
   * @return the number of partitions.
   */
  public int getNumOfChannels() {
    return numOfChannels;
  }

  /**
   * @return the number of tuples written to channels.
   */
  public final long getNumTuplesWrittenToChannels() {
    return numTuplesWrittenToChannels;
  }
}
