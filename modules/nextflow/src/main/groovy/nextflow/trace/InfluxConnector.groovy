package nextflow.trace

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

import com.influxdb.client.InfluxDBClient
import com.influxdb.client.InfluxDBClientFactory
import com.influxdb.client.WriteApi
import com.influxdb.client.domain.Bucket
import com.influxdb.client.write.Point
import com.influxdb.client.domain.Buckets
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Collects task resource-metrics periodically
 * and stores metrics into InfluxDB
 * (time-series database). After completion of
 * the workflow, the metrics will be evaluated
 * via a custom clustering algorithm.
 *
 * @author Julian Fechner <j.fechner@campus.tu-berlin.de>
 */

@Slf4j
@CompileStatic
class InfluxConnector {
    // Class variables for periodically execute thread
    private int collectDelaySec, collectIntervalSec
    private ScheduledExecutorService executor
    private ScheduledFuture<?> handler
    // Class variables for executing 'ps' command via shell
    private ProcessBuilder builder
    private String pid, tid, name, myWorkDir, cleanedTaskName
    private String max_mem, max_disk
    private int max_cpus
    // Class variables related to database
    public boolean connected
    private String dbHost, dbToken, dbOrg, dbBucket
    private InfluxDBClient dbClient
    private WriteApi writeApi
    // Switch for testing on Local-VM VS UNI-Server (for DB-Access-Tokens)
    private boolean uniServer = false    // true = UNI-Server, false = Local-VM

    // Class constructor
    public InfluxConnector(String pid, String tid, String name, int collectDelaySec,int collectIntervalSec,String bucketName,  String myWorkDir, String cleanedTaskName){
        // Init provided args via constructor
        this.pid = pid
        this.tid = tid
        this.name = name
        this.collectDelaySec = collectDelaySec
        this.collectIntervalSec = collectIntervalSec
        this.myWorkDir = myWorkDir
        this.cleanedTaskName = cleanedTaskName
        // Init additional class variables
        this.executor = Executors.newScheduledThreadPool(2)     // Scheduled every 1 sec ... but script needs 1-2 sec ... so 2 threads maybee needed
        this.builder = new ProcessBuilder()
        // Init variables for influx-db
        this.dbHost = "http://localhost:8086"
        this.dbToken = "4QDi6FI3cKHptyQ7bVeMJRgqNQi17auqH1vNO5m7Lu1tuOy0uqPOExC9d2GJqHTI2dXcLW3PoObinY9ZpjzdLw=="       // Enter <token> here
        this.dbOrg = "bachelor"           // Enter <org> here
        this.dbBucket = bucketName
        this.connected = false
    }
    // Public method to start collecting metrics for single process
    public void startCollectingMetrics(){
        if (!this.connected){
            boolean connectionEstablished = this.connectToDb()
            if (!connectionEstablished){
                println("[*] Could not connect to database => Can't collect metrics")
                log.debug("[*] Could not connect to database => Can't collect metrics")
                return
            }
        }
        // Start collecting metrics frequently
        this.handler = executor.scheduleAtFixedRate(() -> {
            Map<String,?> metrics = this.fetchProcessMetrics()
            log.debug("[*] Collected current metrics for ${this.pid} (${this.cleanedTaskName}): $metrics")
            if (metrics){
                this.saveMetricsToDatabase(metrics)
                log.debug("[*] Saved metrics for ${this.pid} (${this.cleanedTaskName}) to db")
            }
        },this.collectDelaySec,this.collectIntervalSec, TimeUnit.SECONDS)
    }

    // Public method to stop collecting metrics for single process
    public void stopCollectingMetrics(){
        // Stop handler (true=immediately)
        this.handler.cancel(true)
        this.executor.shutdown()
        // Disconnect from DB
        this.disconnectFromDb()
    }
    public void setMaxValues(int cpus,String mem,String disk){
        this.max_cpus = cpus
        this.max_mem = mem
        this.max_disk = disk
    }
    // Method to fetch process metrics via ps-command
    public Map<String,?> fetchProcessMetrics(){
        // Prepare and execute shell command
        log.debug("[*] EXEC: docker-metrics.sh \$(cat ${this.myWorkDir}/cidfile_${this.cleanedTaskName}.txt)")
        builder.command(["/bin/bash","-c","/usr/bin/docker-metrics.sh \$(cat ${this.myWorkDir}/cidfile_${this.cleanedTaskName}.txt)".toString()])
        Process process = builder.start()
        // Read result from shell command (as String)
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))

        // Create merics-record
        HashMap<String,?> metrics = new HashMap<>()
        // Add some default entries to the record
        metrics.put("pid",this.pid)
        metrics.put("tid",this.tid)
        metrics.put("name",this.name)
        metrics.put("max_cpus",this.max_cpus)
        metrics.put("max_mem",this.max_mem)
        metrics.put("max_disk",this.max_disk)

        // Parse command output (line-by-line)
        String line = reader.readLine()
        while (line){
            // Parse values from line
            String[] parts = line.split(" ",2)
            if (parts.length == 2) {
                String metric_name = parts[0]
                String metric_value = parts[1]
                // Add key->val to metric
                metrics.put(metric_name, metric_value)
            }
            line = reader.readLine()
        }
        return metrics.get("docker_cpu_usage_pct",null) ? metrics : null
    }

    // Method to send collected metrics to db
    public void saveMetricsToDatabase(Map<String,?> metrics){
        try {
            Point point = Point.measurement(this.name)  // Should use TID
            for (String key : metrics.keySet()) {
                if (key == "name" || key == "cmd" || key == "pid" || key == "tid") point.addTag(key, metrics.get(key) as String)
                else if (!metrics.get(key).toString().isNumber()) point.addField(key, metrics.get(key) as String)
                else point.addField(key, metrics.get(key) as Double)
            }
            writeApi.writePoint(point)
        }
        catch(Exception e){
            log.debug("[*] Saving task metrics for PID=${this.pid} to db failed: "+e);
        }
    }

    public boolean createBucket(String bucketName){
        this.dbClient = InfluxDBClientFactory.create(this.dbHost, this.dbToken as char[], this.dbOrg)
        Bucket bucket = dbClient.getBucketsApi().findBucketByName(bucketName)
        log.debug("[*] Test existance: "+bucket)
        if (!bucket) {
            String existingOrgID = "d9e1112bd16762e1"
            this.dbClient.getBucketsApi().createBucket(bucketName, existingOrgID)
            log.debug("[*] New bucket created: " + bucketName)
            this.dbBucket = bucketName
        }
        else {
            log.debug("[*] Bucket '"+bucketName+"' exists already")
        }
        this.dbClient.close()
    }

    public boolean connectToDb(){
        try {
            // Connect via provided credentials
            this.dbClient = InfluxDBClientFactory.create(this.dbHost, this.dbToken as char[], this.dbOrg, this.dbBucket)
            // Create asynchronous writer
            this.writeApi = this.dbClient.makeWriteApi()
            // Confirm db status=ok via ping
            this.connected = dbClient.ping()
            return this.connected
        }
        catch (Exception e){
            println("[*] AN ERROR OCCURRED: "+e)
            log.debug("[*] AN ERROR OCCURRED: "+e)
            return false
        }
    }
    public void disconnectFromDb(){
        this.writeApi.close()
        this.dbClient.close()
        this.connected = false
    }
    public boolean initDb(){
        def buckets = new Buckets()
        buckets.getBuckets()
    }
}


