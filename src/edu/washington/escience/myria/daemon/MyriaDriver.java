package edu.washington.escience.myria.daemon;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.inject.Inject;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.evaluator.JVMProcess;
import org.apache.reef.driver.evaluator.JVMProcessFactory;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.daemon.MyriaDriverLauncher.SerializedGlobalConf;
import edu.washington.escience.myria.parallel.Worker;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule;
import edu.washington.escience.myria.tools.MyriaWorkerConfigurationModule;

/**
 * Driver for Myria API server/master. Each worker (including master) is mapped to an Evaluator and
 * a persistent Task.
 */
@Unit
public final class MyriaDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(MyriaDriver.class);
  private final EvaluatorRequestor requestor;
  private final JVMProcessFactory jvmProcessFactory;
  private final Configuration globalConf;
  private final Injector globalConfInjector;
  private final Configuration globalConfWrapper;
  private final ImmutableSet<Configuration> workerConfs;
  private final Set<Integer> workerIdsAllocated;
  private final Map<Integer, ActiveContext> contextsById;
  private final CountDownLatch masterRunning;
  private final CountDownLatch allWorkersRunning;

  /**
   * Possible states of the Myria driver. Can be one of:
   * <dl>
   * <du><code>INIT</code></du>
   * <dd>Initial state. Ready to request an evaluator.</dd>
   * <du><code>PREPARING_MASTER</code></du>
   * <dd>Waiting for master Evaluator/Task to be allocated.</dd>
   * <du><code>PREPARING_WORKERS</code></du>
   * <dd>Waiting for each worker's Evaluator/Task to be allocated.</dd>
   * <du><code>READY</code></du>
   * <dd>Each per-worker Task is ready to receive queries.</dd>
   * </dl>
   */

  private enum State {
    INIT, PREPARING_MASTER, PREPARING_WORKERS, READY
  };

  private volatile State state = State.INIT;

  @Inject
  public MyriaDriver(final EvaluatorRequestor requestor, final JVMProcessFactory jvmProcessFactory,
      final @Parameter(MyriaDriverLauncher.SerializedGlobalConf.class) String serializedGlobalConf)
      throws Exception {
    this.requestor = requestor;
    this.jvmProcessFactory = jvmProcessFactory;
    globalConf = new AvroConfigurationSerializer().fromString(serializedGlobalConf);
    globalConfInjector = Tang.Factory.getTang().newInjector(globalConf);
    globalConfWrapper =
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindNamedParameter(SerializedGlobalConf.class, serializedGlobalConf).build();
    workerConfs = getWorkerConfs();
    workerIdsAllocated = new HashSet<>();
    contextsById = new HashMap<>();
    masterRunning = new CountDownLatch(1);
    allWorkersRunning = new CountDownLatch(workerConfs.size());
  }

  private ImmutableSet<Configuration> getWorkerConfs() throws InjectionException, BindException,
      IOException {
    final ImmutableSet.Builder<Configuration> workerConfsBuilder = new ImmutableSet.Builder<>();
    final Set<String> serializedWorkerConfs =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.WorkerConf.class);
    final ConfigurationSerializer serializer = new AvroConfigurationSerializer();
    for (final String serializedWorkerConf : serializedWorkerConfs) {
      final Configuration workerConf = serializer.fromString(serializedWorkerConf);
      workerConfsBuilder.add(workerConf);
    }
    return workerConfsBuilder.build();
  }

  private static Integer getIdFromWorkerConf(final Configuration workerConf)
      throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(workerConf);
    return injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerId.class);
  }

  private static String getHostFromWorkerConf(final Configuration workerConf)
      throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(workerConf);
    return injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerHost.class);
  }

  private void requestWorkerEvaluators() throws InjectionException {
    final int jvmMemoryQuotaMB =
        1024 * globalConfInjector
            .getNamedInstance(MyriaGlobalConfigurationModule.MemoryQuotaGB.class);
    final int localFragmentWorkerThreads =
        globalConfInjector
            .getNamedInstance(MyriaGlobalConfigurationModule.LocalFragmentWorkerThreads.class);
    LOGGER.info("Requesting {} worker evaluators.", localFragmentWorkerThreads);
    for (final Configuration workerConf : workerConfs) {
      String hostname = getHostFromWorkerConf(workerConf);
      final EvaluatorRequest workerRequest =
          EvaluatorRequest.newBuilder().setNumber(1).setMemory(jvmMemoryQuotaMB)
              .setNumberOfCores(localFragmentWorkerThreads).addNodeName(hostname).build();
      requestor.submit(workerRequest);
    }
  }

  private void requestMasterEvaluator() throws InjectionException {
    LOGGER.info("Requesting master evaluator.");
    final String masterHost =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.MasterHost.class);
    final int jvmMemoryQuotaMB =
        1024 * globalConfInjector
            .getNamedInstance(MyriaGlobalConfigurationModule.MemoryQuotaGB.class);
    final int localFragmentWorkerThreads =
        globalConfInjector
            .getNamedInstance(MyriaGlobalConfigurationModule.LocalFragmentWorkerThreads.class);
    final EvaluatorRequest masterRequest =
        EvaluatorRequest.newBuilder().setNumber(1).setMemory(jvmMemoryQuotaMB)
            .setNumberOfCores(localFragmentWorkerThreads).addNodeName(masterHost).build();
    requestor.submit(masterRequest);
  }

  private void setJVMOptions(final AllocatedEvaluator evaluator) throws InjectionException {
    final int jvmHeapSizeMinGB =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.JvmHeapSizeMinGB.class);
    final int jvmHeapSizeMaxGB =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.JvmHeapSizeMaxGB.class);
    final Set<String> jvmOptions =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.JvmOptions.class);
    final JVMProcess jvmProcess =
        jvmProcessFactory.newEvaluatorProcess()
            .addOption(String.format("-Xms%dg", jvmHeapSizeMinGB))
            .addOption(String.format("-Xmx%dg", jvmHeapSizeMaxGB))
            // for native libraries
            .addOption("-Djava.library.path=./reef/global");
    for (String option : jvmOptions) {
      jvmProcess.addOption(option);
    }
    evaluator.setProcess(jvmProcess);
  }

  /**
   * The driver is ready to run.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOGGER.info("Driver started at {}", startTime);
      assert (state == State.INIT);
      state = State.PREPARING_MASTER;
      try {
        requestMasterEvaluator();
      } catch (InjectionException e) {
        throw new RuntimeException(e);
      }
      // We need to wait for the master to be allocated before we request evaluators for the
      // workers, otherwise we can't tell which evaluator is which (because REEF doesn't let us
      // submit a client token with the evaluator requests).
      try {
        masterRunning.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      state = State.PREPARING_WORKERS;
      try {
        requestWorkerEvaluators();
      } catch (InjectionException e) {
        throw new RuntimeException(e);
      }
      try {
        allWorkersRunning.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      // We are now ready to receive requests.
      // TODO: send RPC to REST API that it can now service requests?
      state = State.READY;
    }
  }

  /**
   * Shutting down the job driver: close the evaluators.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      LOGGER.info("Driver stopped at {}", stopTime);
      for (final ActiveContext context : contextsById.values()) {
        context.close();
      }
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator evaluator) {
      String host = evaluator.getEvaluatorDescriptor().getNodeDescriptor().getName();
      LOGGER.info("Allocated evaluator {} on node {}", evaluator.getId(), host);
      if (state == State.PREPARING_MASTER) {
        Configuration contextConf =
            ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER,
                MyriaConstants.MASTER_ID + "").build();
        try {
          setJVMOptions(evaluator);
        } catch (InjectionException e) {
          throw new RuntimeException(e);
        }
        evaluator.submitContext(Configurations.merge(contextConf, globalConf, globalConfWrapper));
      } else {
        Preconditions.checkState(state == State.PREPARING_WORKERS);
        // allocate the next worker ID associated with this host
        try {
          for (final Configuration workerConf : workerConfs) {
            final String confHost = getHostFromWorkerConf(workerConf);
            final Integer confId = getIdFromWorkerConf(workerConf);
            if (confHost.equals(host) && !workerIdsAllocated.contains(confId)) {
              Configuration contextConf =
                  ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER, confId + "")
                      .build();
              setJVMOptions(evaluator);
              evaluator.submitContext(Configurations.merge(contextConf, globalConf,
                  globalConfWrapper, workerConf));
              workerIdsAllocated.add(confId);
              break;
            }
          }
        } catch (InjectionException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator eval) {
      LOGGER.info("CompletedEvaluator: {}", eval.getId());
    }
  }

  final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOGGER.error("FailedEvaluator: {}", failedEvaluator);
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      String host = context.getEvaluatorDescriptor().getNodeDescriptor().getName();
      LOGGER.info("Context {} available on node {}", context.getId(), host);
      final Configuration taskConf;
      if (context.getId().equals(MyriaConstants.MASTER_ID + "")) {
        taskConf =
            TaskConfiguration.CONF.set(TaskConfiguration.TASK, MasterDaemon.class)
                .set(TaskConfiguration.IDENTIFIER, context.getId()).build();

      } else {
        taskConf =
            TaskConfiguration.CONF.set(TaskConfiguration.TASK, Worker.class)
                .set(TaskConfiguration.IDENTIFIER, context.getId()).build();
      }
      context.submitTask(taskConf);
      contextsById.put(Integer.valueOf(context.getId()), context);
    }
  }

  final class ContextFailureHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      LOGGER.error("FailedContext: {}", failedContext);
    }
  }

  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      LOGGER.info("Running task: {}", task.getId());
      if (task.getActiveContext().getId().equals(MyriaConstants.MASTER_ID + "")) {
        masterRunning.countDown();
      } else {
        allWorkersRunning.countDown();
      }
    }
  }

  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask task) {
      LOGGER.error("Unexpected CompletedTask: {}", task.getId());
    }
  }

  final class TaskFailureHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      LOGGER.error("FailedTask: {}", failedTask);
      failedTask.getActiveContext().get().close();
    }
  }
}
