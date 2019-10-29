package tpe2.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        logger.info("tpe2-g8 Server Starting ...");
        HazelcastInstance hzInstance = Hazelcast.newHazelcastInstance();

    }
}
