package pailor.com.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pailor.com.Mov;
import pailor.com.TokenizerMapper;
import pailor.com.WordCountReducerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    static private String LIST1 = "g8";
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //ex1();
        movPerAirPorts();
    }

    private static void movPerAirPorts() throws ExecutionException, InterruptedException {
        logger.info("movPerAirPorts...");

        final ClientConfig ccfg = new ClientConfig();
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);
        JobTracker t = hz.getJobTracker("movPerAirPorts");

        // cleanAll;

        final IList<Mov> list = hz.getList(LIST1);
        list.clear();

        list.add(new Mov(new Date("30/06/2019"),10,"Regular","Internacional","Despegue","SACO","KMIA","American Airlines","BOEING B-767"));
        list.add(new Mov(new Date("30/06/2019"),10,"Regular","Internacional","Despegue","SACO","KMIA","American Airlines","BOEING B-767"));
        list.add(new Mov(new Date("30/06/2019"),10,"Regular","Internacional","Despegue","SACO","KMIA","American Airlines","BOEING B-767"));
        list.add(new Mov(new Date("30/06/2019"),10,"Regular","Internacional","Despegue","SACO","KMIA","American Airlines","BOEING B-767"));
        list.add(new Mov(new Date("30/06/2019"),10,"Regular","Internacional","Despegue","SACO","KMIA","American Airlines","BOEING B-767"));

        final KeyValueSource<String, Mov> source = KeyValueSource.fromList(list);

        Job<String, Mov> job = t.newJob( source );
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

        System.out.println(result);*/
    }

}
