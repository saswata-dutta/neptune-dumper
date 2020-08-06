package org.saswata;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.SigV4WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class Main {
  static final long EVAL_TIMEOUT = TimeUnit.MINUTES.toMillis(15);

  public static void main(String[] args) {
    final String neptune = args[0];
    final char itemType = args[1].toUpperCase().charAt(0); // item type E : edges, V : vertices
    final long FETCH_SIZE = Long.parseLong(args[2]); // num of items to fetch at once
    final int POOL_SIZE = Integer.parseInt(args[3]); // num of threads

    final Cluster cluster = clusterProvider(neptune, POOL_SIZE);
    final GraphTraversalSource g = graphProvider(cluster);

    dump(g, itemType == 'V', FETCH_SIZE, POOL_SIZE);

    cluster.close();
  }

  static Cluster clusterProvider(String neptune, int POOL_SIZE) {

    Cluster.Builder clusterBuilder = Cluster.build()
        .addContactPoint(neptune)
        .port(8182)
        .enableSsl(true)
        .channelizer(SigV4WebSocketChannelizer.class)
        .serializer(Serializers.GRAPHBINARY_V1D0)
        .maxInProcessPerConnection(1)
        .minInProcessPerConnection(1)
        .maxSimultaneousUsagePerConnection(1)
        .minSimultaneousUsagePerConnection(1)
        .minConnectionPoolSize(POOL_SIZE)
        .maxConnectionPoolSize(POOL_SIZE);

    return clusterBuilder.create();
  }

  static GraphTraversalSource graphProvider(Cluster cluster) {
    RemoteConnection connection = DriverRemoteConnection.using(cluster);
    return AnonymousTraversalSource.traversal().withRemote(connection);
  }

  static void dump(GraphTraversalSource g, boolean isVertex, long FETCH_SIZE, int POOL_SIZE) {

    try (PrintWriter out = new PrintWriter(new FileWriter(isVertex ? "vertex_ids" : "edge_ids"))) {

      final long count = isVertex ? countVertices(g) : countEdges(g);
      System.out.println("count," + count);

      final ExecutorService executorService = new ThreadPoolExecutor(POOL_SIZE, POOL_SIZE,
          0L, TimeUnit.MILLISECONDS,
          new ExecutorBlockingQueue<>(POOL_SIZE));

      for (long i = 0; i < count; ) {
        final long low = i;
        i += FETCH_SIZE;
        final long high = i;

        final Runnable action = isVertex ?
            () -> dumpVertices(g, low, high, out) :
            () -> dumpEdges(g, low, high, out);

        executorService.submit(action);
      }

      executorService.shutdown();
      if (!executorService.awaitTermination(15, TimeUnit.MINUTES)) {
        System.err.println("Executor forced exit");
        System.exit(0);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static void dumpVertices(GraphTraversalSource g, long low, long high, PrintWriter out) {
    List<Object> ids = g.with(Tokens.ARGS_EVAL_TIMEOUT, EVAL_TIMEOUT).
        V().id().range(low, high).toList();

    write(ids, out, high);
  }

  static void dumpEdges(GraphTraversalSource g, long low, long high, PrintWriter out) {
    List<Object> ids = g.with(Tokens.ARGS_EVAL_TIMEOUT, EVAL_TIMEOUT).
        E().id().range(low, high).toList();

    write(ids, out, high);
  }

  static long countVertices(GraphTraversalSource g) {
    return g.with(Tokens.ARGS_EVAL_TIMEOUT, EVAL_TIMEOUT).
        V().count().next();
  }

  static long countEdges(GraphTraversalSource g) {
    return g.with(Tokens.ARGS_EVAL_TIMEOUT, EVAL_TIMEOUT).
        E().count().next();
  }

  static synchronized void write(List<Object> ids, PrintWriter out, long count) {
    ids.forEach(out::println);
    System.out.println("progress," + count);
  }
}
