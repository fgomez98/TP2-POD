package tp2.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tp2.api.Mov;
import tp2.api.TokenizerMapper;
import tp2.api.WordCountReducerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    static private String LIST1 = "g8";
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //ex1();
        movPerAirPorts(Arrays.asList("localhost"));
    }

    private static void movPerAirPorts(List<String> ips) throws ExecutionException, InterruptedException {
        logger.info("movPerAirPorts...");

        Config cfg = new Config();
        NetworkConfig network = cfg.getNetworkConfig();
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        ips.forEach(ip -> join.getTcpIpConfig().addMember(ip));
        join.getTcpIpConfig().setEnabled(true);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        JobTracker t = hz.getJobTracker("movPerAirports");

        // cleanAll;

        final IMap<Integer, Mov> list = hz.getMap(LIST1);
        list.clear();

        for (int i = 0; i <300000; i++) {
            list.put(i, new Mov(new Date("30/06/2019"),i,"Regular","Internacional","Despegue","SACO","KMIA","American Airlines","BOEING B-767"));
        }

        Thread.sleep(10000);

        final KeyValueSource<Integer, Mov> source = KeyValueSource.fromMap(list);

        Job<Integer, Mov> job = t.newJob( source );
        ICompletableFuture<Map<String, Long>> future = job
                .mapper( new TokenizerMapper() )
                .reducer( new WordCountReducerFactory() )
                .submit();

        // Attach a callback listener
        future.andThen(new ExecutionCallback<Map<String, Long>>() {
            @Override
            public void onResponse(Map<String, Long> stringLongMap) {
                for (Map.Entry<String, Long> entry: stringLongMap.entrySet()) {
                    System.out.println(entry.getKey() + " => " + entry.getValue());
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                System.err.println(throwable.getMessage());
            }
        });
        // Wait and retrieve the result
        Map<String, Long> result = future.get();

        System.out.println(result);
    }

    private static void ex1() throws ExecutionException, InterruptedException {
        /*logger.info("TP2 Client Starting ...");

        final ClientConfig ccfg = new ClientConfig();
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);
        JobTracker t = hz.getJobTracker("word-count");

        final IMap<String, String> map = hz.getMap("default");

        map.put("Drácula", "Esta es mi casa");
        map.put("Alicia en el país", "Esta no es mi casa");
        map.put("Miguel", "Esta tal vez es mi casa");


        final KeyValueSource<String, String> source = KeyValueSource.fromMap(map);

        Job<String, String> job = t.newJob( source );
        ICompletableFuture<Map<String, Long>> future = job
                .mapper( new tp2.api.TokenizerMapper() )
                .reducer( new tp2.api.WordCountReducerFactory() )
                .submit();

        // Attach a callback listener
        future.andThen(new ExecutionCallback<Map<String, Long>>() {
            @Override
            public void onResponse(Map<String, Long> stringLongMap) {
                for (Map.Entry<String, Long> entry: stringLongMap.entrySet()) {
                    System.out.println(entry.getKey() + " => " + entry.getValue());
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                System.err.println(throwable.getMessage());
            }
        });
        // Wait and retrieve the result
        Map<String, Long> result = future.get();

        System.out.println(result);*/
    }

}
