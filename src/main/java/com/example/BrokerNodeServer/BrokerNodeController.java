package com.example.BrokerNodeServer;

import java.net.InetAddress;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.amazonaws.util.EC2MetadataUtils;

import java.net.UnknownHostException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@RestController
public class BrokerNodeController {

    @Value("${server.port}")
    private int port; // Inject server port

    @Value("${config.server.url}")
    private String configServerUrl;
    // In application.properties:
    // server.port=8080
    // config.server.url=http://3.223.147.155:8080/

    private final RestTemplate restTemplate = new RestTemplate();

    private Broker broker;

    private String currentLeaderBrokerPrivateIP;

    @PostMapping("/register")
    public void registerWithConfigServer(@RequestBody Broker broker) {
        try {

            // broker.setIpAddress(InetAddress.getLocalHost().getHostAddress());//sets the
            // private IP address
            String privateIpAddress = EC2MetadataUtils.getInstanceInfo().getPrivateIp();
            broker.setIpAddress(privateIpAddress);
            broker.setPort(port); // Set the injected port
            broker.setUniqueId(20);
            // RestTemplate restTemplate = new RestTemplate();

            // Retrieve EC2 instance ID dynamically using AWS EC2 Metadata Service
            String ec2InstanceId = EC2MetadataUtils.getInstanceId();
            broker.setEC2instanceID(ec2InstanceId);

            this.broker = broker;

            restTemplate.postForObject(configServerUrl + "/register-broker", broker, Void.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/leadBroker-status")
    public ResponseEntity<String> checkHealth() {
        return ResponseEntity.ok("Alive");
    }

    @GetMapping("/helloBroker")
    public String hello() {
        return "Hello, World! BrokerServer is up & running!";
    }

    @DeleteMapping("/deregister/{uniqueId}")
    public void deregisterFromConfigServer(@PathVariable int uniqueId) {
        restTemplate.delete(configServerUrl + "/deregister-broker/" + uniqueId);
    }

    @PostConstruct
    public void triggerRegistration() {
        // Create a sample Broker object with necessary information
        Broker broker = new Broker();
        // You may set other properties of the broker object as needed

        // Trigger registration with the configuration server
        registerWithConfigServer(broker);
        currentLeaderBrokerPrivateIP = getLeaderPrivateIPFromConfigServer();

        updateLeaderStatus();
    }

    // Method to handle POST request from config server to update leader's private
    // IP

    @PostMapping("/updateCurrentNode-leaderIPValue")
    public void updateLeaderIP(@RequestBody String leaderPrivateIP) {
        // Update the current leader's private IP with the value received in the request
        this.currentLeaderBrokerPrivateIP = leaderPrivateIP;
        updateLeaderStatus();
    }

    // Update leader status based on current private IP
    private void updateLeaderStatus() {

        if (currentLeaderBrokerPrivateIP != null && broker.getIpAddress().equals(currentLeaderBrokerPrivateIP)) {
            broker.setLeader(true);
        } else {
            broker.setLeader(false);
        }
    }

    // Method to retrieve the private IP of the leader broker from config server
    private String getLeaderPrivateIPFromConfigServer() {
        // Make a GET request to the config server endpoint
        String leaderPrivateIP = restTemplate.getForObject(configServerUrl + "/getCurrent-leadBroker-PrivateIP",
                String.class);

        // Return the fetched private IP of the lead broker
        return leaderPrivateIP;
    }

    // Method to periodically ping the leader broker to check its status
    @Scheduled(fixedRate = 10000) // 10 seconds
    private void pingLeaderBroker() {

        if (!broker.isLeader()) {
            // This is not the leader broker, so ping the leader broker to check its status

            if (currentLeaderBrokerPrivateIP != null) {
                boolean isLeaderResponsive = pingLeader(currentLeaderBrokerPrivateIP);
                if (!isLeaderResponsive) {
                    // If leader does not respond, inform the config server and then request the
                    // private IP of newly elected leader broker
                    informLeaderNotResponding();
                    getLeaderPrivateIPFromConfigServer();
                }
            }
        }
    }
    /*
     * Method to ping the leader broker
     * Perform ping operation to check the status of the leader broker using its
     * private IP Sending a GET request to a to a specific endpoint for health check
     * on the leader broker and check for a successful response
     */

    private boolean pingLeader(String leaderPrivateIp) {

        boolean isLeaderResponsive = false;
        try {

            String healthCheckUrl = "http://" + leaderPrivateIp + "/leadBroker-status";
            String statusOfLeadBroker = restTemplate.getForObject(healthCheckUrl, String.class);
            System.out.println("Response from leader broker: " + statusOfLeadBroker);
            isLeaderResponsive = true;
        } catch (Exception e) {
            // Exception occurred, leader is not responsive
            System.err.println("Error occurred while pinging leader broker: " + e.getMessage());
            isLeaderResponsive = false;
        }
        return isLeaderResponsive;
    }

    // Method to inform the config server that the leader has not responded
    private void informLeaderNotResponding() {
        try {
            restTemplate.postForObject(configServerUrl + "/leader-not-responding", null, Void.class);

        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    @PreDestroy
    public void triggerDeregistration() {
        // Deregister from the config server before the instance terminates
        deregisterFromConfigServer(broker.getUniqueId());
    }

}
