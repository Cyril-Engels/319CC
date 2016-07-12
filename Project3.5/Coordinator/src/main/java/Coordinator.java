import java.io.IOException;
import java.util.*;
import java.sql.Timestamp;
import java.util.concurrent.LinkedBlockingQueue;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

    //Default mode: Strongly consistent. Possible values are "strong" and "causal"
    private static String consistencyType = "strong";

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances
     */
    private static final String dataCenter1 = "ec2-52-5-99-158.compute-1.amazonaws.com";
    private static final String dataCenter2 = "ec2-52-5-59-46.compute-1.amazonaws.com";
    private static final String dataCenter3 = "ec2-52-4-38-36.compute-1.amazonaws.com";
    //Use a HashMap to store key lokers in strong consistency.
    final Map<String, Locker> keyLockerMap = new HashMap<>();
    //Use three HashMaps to validate locking three data centers respectively in causal consistency.
    final Map<String, Locker> dc1Map = new HashMap<>();
    final Map<String, Locker> dc2Map = new HashMap<>();
    final Map<String, Locker> dc3Map = new HashMap<>();

    @Override
    public void start() {
        //DO NOT MODIFY THIS
        KeyValueLib.dataCenters.put(dataCenter1, 1);
        KeyValueLib.dataCenters.put(dataCenter2, 2);
        KeyValueLib.dataCenters.put(dataCenter3, 3);
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);

        routeMatcher.get("/put", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String value = map.get("value");
                //You may use the following timestamp for ordering requests
                final String timestamp = new Timestamp(System.currentTimeMillis()
                        + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                req.response().end(); //Do not remove this
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        //TODO: Write code for PUT operation here.
                        //Each PUT operation is handled in a different thread.
                        //Highly recommended that you make use of helper functions.
                        if (consistencyType.equals("strong")) {
                            strongPut(key, value, timestamp, keyLockerMap);
                        } else {
                            //In causal consitency, start the first thread first
                            causalPut(key, value, timestamp, dataCenter1, dc1Map);
                        }
                    }
                });
                t.start();
                //Use another two threads to put key value into dc2 and dc3 in causal consistency.
                if (consistencyType.equals("causal")) {
                    Thread t2 = new Thread(new Runnable() {
                        public void run() {
                            causalPut(key, value, timestamp, dataCenter2, dc2Map);
                        }
                    });
                    t2.start();
                    Thread t3 = new Thread(new Runnable() {
                        public void run() {
                            causalPut(key, value, timestamp, dataCenter3, dc3Map);
                        }
                    });
                    t3.start();
                }
            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String loc = map.get("loc");
                //You may use the following timestamp for ordering requests
                final String timestamp = new Timestamp(System.currentTimeMillis()
                        + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        //TODO: Write code for GET operation here.
                        //Each GET operation is handled in a different thread.
                        //Highly recommended that you make use of helper functions.
                        String value = null;
                        if (consistencyType.equals("strong")) {
                            value = strongGet(key, loc, timestamp, keyLockerMap);
                        } else {
                            value = causalGet(key, loc);
                        }
                        req.response().end(value); //Default response = 0
                    }
                });
                t.start();
            }
        });

        routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                consistencyType = map.get("consistency");
                //This endpoint will be used by the auto-grader to set the
                //consistency type that your key-value store has to support.
                //You can initialize/re-initialize the required data structures here
                req.response().end();
            }
        });

        routeMatcher.noMatch(new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                req.response().putHeader("Content-Type", "text/html");
                String response = "Not found.";
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                req.response().end(response);
                req.response().close();
            }
        });
        server.requestHandler(routeMatcher);
        server.listen(8080);
    }

    /**
     * This method validates PUT in strong consistency.
     *
     * @param key          key
     * @param value        value
     * @param timestamp    timestamp
     * @param keyLockerMap Hash Map that stores the keys and lockers
     */
    public void strongPut(String key, String value, String timestamp, HashMap<String, Locker> keyLockerMap) {
        //If the HashMap doesn't contain tha key, continue putting
        if (!keyLockerMap.containsKey(key)) {
            Locker locker = new Locker(timestamp);
            keyLockerMap.put(key, locker);
            //Use syn to lock the key
            synchronized (locker) {
                try {
                    KeyValueLib.PUT(dataCenter1, key, value);
                    KeyValueLib.PUT(dataCenter2, key, value);
                    KeyValueLib.PUT(dataCenter3, key, value);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //Poll the key after putting
                locker.poll();
                if (locker.isEmpty()) {
                    keyLockerMap.remove(key);
                } else {
                    // Notify the blocking threads
                    locker.notifyAll();
                }
            }

        } else {
            //Add the timestamp into the queue
            final Locker lock = keyLockerMap.get(key);
            lock.addToQueue(timestamp);
            synchronized (lock) {
                //Unless timestamp equals to that of the head of the queue, block it
                while (!lock.peek().equals(timestamp)) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    KeyValueLib.PUT(dataCenter1, key, value);
                    KeyValueLib.PUT(dataCenter2, key, value);
                    KeyValueLib.PUT(dataCenter3, key, value);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                lock.poll();
                if (lock.isEmpty()) {
                    keyLockerMap.remove(key);
                } else {
                    lock.notifyAll();
                }
            }
        }
    }

    /**
     * This method validates GET in strong consistency.
     *
     * @param key          key
     * @param loc          location of data center
     * @param timestamp    timestamp
     * @param keyLockerMap Hash Map that stores the keys and lockers
     * @return result
     */
    public String strongGet(String key, String loc, String timestamp, HashMap<String, Locker> keyLockerMap) {
        String value = null;
        //If the Hash Map doesn't contain this key, continue getting operation
        if (!keyLockerMap.containsKey(key)) {
            try {
                if (loc.equals("1")) {
                    value = KeyValueLib.GET(dataCenter1, key);
                } else if (loc.equals("2")) {
                    value = KeyValueLib.GET(dataCenter2, key);
                } else {
                    value = KeyValueLib.GET(dataCenter3, key);
                }
                return value;
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            /* Unless timestamp equals to that of the head of the queue, block it */
            final Locker locker = keyLockerMap.get(key);
            locker.addToQueue(timestamp);
            synchronized (locker) {
                while (!locker.peek().equals(timestamp)) {
                    try {
                        locker.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    if (loc.equals("1")) {
                        value = KeyValueLib.GET(dataCenter1, key);
                    } else if (loc.equals("2")) {
                        value = KeyValueLib.GET(dataCenter2, key);
                    } else {
                        value = KeyValueLib.GET(dataCenter3, key);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                locker.poll();
                if (locker.isEmpty()) {
                    keyLockerMap.remove(key);
                } else {
                    locker.notifyAll();
                }
                return value;
            }
        }
        return value;
    }

    /**
     * This method validates PUT in causal consitency.
     *
     * @param key           key
     * @param value         value
     * @param timestamp     timestamp
     * @param dataCenter    data center to be put into
     * @param dataCenterMap Hash Map that stores the keys and lockers of the data center
     */
    public void causalPut(String key, String value, String timestamp, String dataCenter, HashMap<String, Locker> dataCenterMap) {
        if (!dataCenterMap.containsKey(key)) {
            Locker locker = new Locker(timestamp);
            dataCenterMap.put(key, locker);
            synchronized (locker) {
                try {
                    // Lock one data center at a time
                    KeyValueLib.PUT(dataCenter, key, value);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                locker.poll();
                if (locker.isEmpty()) {
                    dataCenterMap.remove(key);
                } else {
                    locker.notifyAll();
                }
            }
        } else {
            final Locker lock = dataCenterMap.get(key);
            lock.addToQueue(timestamp);
            synchronized (lock) {
                while (!lock.peek().equals(timestamp)) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    KeyValueLib.PUT(dataCenter, key, value);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                lock.poll();
                if (lock.isEmpty()) {
                    dataCenterMap.remove(key);
                } else {
                    lock.notifyAll();
                }
            }
        }
    }

    /**
     * This method validates GET in causal consistency.
     * @param key key
     * @param loc location of data center
     * @return result
     */
    public String causalGet(String key, String loc) {
        /* This method doesn't block*/
        String value = null;
        try {
            if (loc.equals("1")) {
                value = KeyValueLib.GET(dataCenter1, key);
            } else if (loc.equals("2")) {
                value = KeyValueLib.GET(dataCenter2, key);
            } else {
                value = KeyValueLib.GET(dataCenter3, key);
            }
            return value;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }
}

/**
 * This class acts as a locker and uses a queue to store timestamps in a Queue.
 *
 * @author Yilong Chang
 * @since 04/04/2015
 */
class Locker {
    //Use a queue to store timestamps
    BlockingQueue<String> timestampQueue = new LinkedBlockingQueue<>();
    //Use the constructor to add timestamp to queue
    public Locker(String timestamp) {
        timestampQueue.add(timestamp);
    }
    //Check if the queue is empty
    public boolean isEmpty() {
        return timestampQueue.isEmpty();
    }
    //Add the timestamp to queue
    public void addToQueue(String timestamp) {
        timestampQueue.add(timestamp);
    }
    // Get the head of the queue without removing it
    public String peek() {
        return timestampQueue.peek();
    }
    // Get the head of the queue and remove it
    public String poll() {
        return timestampQueue.poll();
    }
}