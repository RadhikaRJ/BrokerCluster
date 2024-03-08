package com.example.BrokerNodeServer;

import org.json.JSONObject;
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

import org.springframework.http.MediaType;
import org.springframework.http.HttpEntity;

import com.amazonaws.util.EC2MetadataUtils;

import org.springframework.http.HttpHeaders;

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

    private String currentLeaderBrokerPrivateIP; // now will refer to public IPv4 address
    // Flag to track whether leader IP check is needed
    private boolean leaderIPCheckNeeded = true;

    @PostConstruct
    public void triggerRegistration() {
        // Create a sample Broker object with necessary information
        this.broker = new Broker();
        // You may set other properties of the broker object as needed

        // Trigger registration with the configuration server
        System.out.println("Registering with coordinator server...");
        registerWithConfigServer(this.broker);
        currentLeaderBrokerPrivateIP = getLeaderPrivateIPFromConfigServer();

        updateLeaderStatus();
    }

    // Update leader status based on current private IP
    private void updateLeaderStatus() {
        try {
            if (currentLeaderBrokerPrivateIP != null && broker != null && broker.getIpAddress() != null
                    && broker.getIpAddress().equals(currentLeaderBrokerPrivateIP)) {
                broker.setLeader(true);
                System.out.println("I am lead broker node");
            } else {
                if (broker != null) {
                    broker.setLeader(false);
                }

                System.out.println("I am just a peer node in the broker cluster.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostMapping("/register")
    public void registerWithConfigServer(@RequestBody Broker broker) {
        try {
            System.out.println("Registering broker node with Coordinator Server...");
            // broker.setIpAddress(InetAddress.getLocalHost().getHostAddress());//sets the
            // private IP address
            String privateIpAddress = EC2MetadataUtils.getInstanceInfo().getPrivateIp();
            // // private IP of currrent EC2

            broker.setIpAddress(privateIpAddress);
            broker.setPort(port); // Set the injected port 8080
            broker.setUniqueId(30);
            // RestTemplate restTemplate = new RestTemplate();

            // Retrieve EC2 instance ID dynamically using AWS EC2 Metadata Service
            String ec2InstanceId = EC2MetadataUtils.getInstanceId();
            broker.setEC2instanceID(ec2InstanceId);

            this.broker = broker;

            restTemplate.postForObject(configServerUrl + "/register-broker", broker, Void.class);
            System.out.println(
                    "broker node with uniqueID: " + broker.getUniqueId() + "has sent registeration request to server");
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
        if (broker != null) {
            return "Hello, World! BrokerServer is up & running! uniqueID: " + broker.getUniqueId();
        } else {
            return "Hello, World! BrokerServer is up & running!";
        }
        // return "Hello, World! BrokerServer is up & running! uniqueID: " +
        // broker.getUniqueId();
    }

    @DeleteMapping("/deregister/{uniqueId}")
    public void deregisterFromConfigServer(@PathVariable int uniqueId) {
        System.out.println("Sending request to Coordinator Server to deregister broker with uniqueID: " + uniqueId
                + " from Coordinator Server...");
        restTemplate.delete(configServerUrl + "/deregister-broker/" + uniqueId);
    }

    // Method to handle POST request from config server to update leader's private
    // IP

    @PostMapping("/updateLeaderIPAndCheckStatus")
    public void handleUpdateLeaderIPAndCheckStatus(@RequestBody String requestBody) {
        JSONObject jsonObject = new JSONObject(requestBody);
        String newLeadBrokerPrivateIPAddress = jsonObject.getString("newLeadBrokerPrivateIPAddress");
        // System.out.println("Received new lead broker private IP address: " +
        // newLeadBrokerPrivateIPAddress);
        this.currentLeaderBrokerPrivateIP = newLeadBrokerPrivateIPAddress;
        this.leaderIPCheckNeeded = true;
        updateLeaderStatus();
    }

    /*
     * @PostMapping("/updateCurrentNode-leaderIPValue")
     * public void updateLeaderIP(@RequestBody String leaderPrivateIP) {
     * // Update the current leader's private IP with the value received in the
     * request
     * this.currentLeaderBrokerPrivateIP = leaderPrivateIP;
     * updateLeaderStatus();
     * }
     */

    // Method to retrieve the private IP of the leader broker from config server
    private String getLeaderPrivateIPFromConfigServer() {
        // Make a GET request to the config server endpoint
        try {
            System.out.println("Requesting lead broker's private IP address at Coordinator server");
            String currleadBrokerPrivateIPAtConfigServer = restTemplate.getForObject(
                    configServerUrl + "/getCurrent-leadBroker-PrivateIP",
                    String.class);

            // Return the fetched private IP of the lead broker
            return currleadBrokerPrivateIPAtConfigServer;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Scheduled(fixedRate = 10000) // 10 seconds
    private void pingLeaderBroker() {
        // if current node is leader, then it will not perform the ping operation.
        if (!broker.isLeader()) {
            // this node is not lead broker, so it will perform ping as scheduled to the
            // lead broker
            // initially, leaderIPCheckNeeded is set to true as we need to check the status
            // periodically
            // also, the currentLeaderBrokerPrivateIP is not set to null ( it will be set to
            // null in the duration when leader election is occurring at the config server
            // and until new leader's IP is not updated)
            if (leaderIPCheckNeeded && currentLeaderBrokerPrivateIP != null) {
                // we ping and check the lead broker
                System.out.println("Pinging the lead broker IP...");
                boolean isLeaderResponsive = pingLeader(currentLeaderBrokerPrivateIP);
                // if isLeaderResponsive is true, then next health check at same leadBroker's IP
                // Address will happen as per scheduled execution
                // if we don't get a response from leaderIP address, execute the next block of
                // code
                if (!isLeaderResponsive) {
                    // we inform the configServer about the leaderIP being non-responsive and send
                    // it the currentLeaderBrokerPrivateIP
                    System.out.println("Leader broker has not responded to ping");
                    informLeaderNotResponding();
                    String currLeadBrokerPrivateIPAtServer = getLeaderPrivateIPFromConfigServer();
                    System.out
                            .println("Requesting leader's privateIP at coordinating server. Server has returned value: "
                                    + currLeadBrokerPrivateIPAtServer);
                    // the configServer will have some currentBrokerLeaderIP value:
                    // 1. null if its carrying out leader election
                    // 2. another IP address if new leader has been elected
                    // 3. same IP address as that of currentLeaderBrokerPrivateIP.
                    // 3.a lead broker is unhealthy, but it is back up by the time node reports and
                    // config verifies.
                    // So configServer will returns same IP that peer reporting node has.
                    // 3.b Handles the scemnario where due to some communication failure, peer node
                    // reports leader broker to be down. But in reality, it never failed. So same IP
                    // is returned by configServer

                    // if IP returned is null, update the currentLeaderBrokerPrivateIP to null and
                    // do nothing.
                    // this ensures that no scheduled ping action happens in next iteration
                    // configServer will send an update to all peer nodes about new IP address of
                    // the leader broker node.
                    // untill then, the pinging and reporting lead broker status to configServer
                    // will be paused.

                    if (currLeadBrokerPrivateIPAtServer == null) {
                        currentLeaderBrokerPrivateIP = null;
                        // don't check untill we have a new elected value for the
                        // currentLeaderBrokerPrivateIP
                        System.out.println("leaderIPCheckNeeded is set to false.");
                        leaderIPCheckNeeded = false;
                        // this value will be set to true by configServer when it updates the
                        // currentLeaderBrokerPrivateIP.
                    }

                    if (currLeadBrokerPrivateIPAtServer != null) {
                        // configServer may also in the mean time set the current peer to lead.
                        // But if it hasn't yet completed that action, the broker node can check the
                        // returned IP and accordingly update isLead()
                        // so then the scheduled ping does not happen hereafter for the newly elected
                        // broker node
                        if (currLeadBrokerPrivateIPAtServer.equals(broker.getIpAddress())) {
                            broker.setLeader(true);
                            System.out.println("I am the leader broker node");

                        }
                        // Lead broker IP sent by server is not current node's private IP
                        // it does not match the currentLeaderBrokerPrivateIP that this current node has
                        // This means a new leader node has been elected.
                        // Its possible that before configServer has communicated to currentNode of
                        // newly elected leadBroker's IPAddress,
                        // the current node has retrieved it. So the current node updates
                        // currentLeaderBrokerPrivateIP itself.
                        // next scheduled ping will occur at the updated lead broker's private IP
                        else if (!currLeadBrokerPrivateIPAtServer.equals(currentLeaderBrokerPrivateIP)) {
                            // leaderIPCheckNeeded is still true and so next pings will execute
                            System.out.println(
                                    "Lead Broker's Private IP at server was different. Updating lead broker's private IP at local node...");
                            currentLeaderBrokerPrivateIP = currLeadBrokerPrivateIPAtServer;
                        }
                    }
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

            String healthCheckUrl = "http://" + leaderPrivateIp + ":8080/leadBroker-status";
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

    // inform configServer that lead node is not responding
    // and send the currentLeaderBrokerPrivateIP stored at peer broker node
    private void informLeaderNotResponding() {
        try {
            System.out.println("Informing Coordinator Server that lead broker did not respond...");
            String requestBody = "{\"currleadBrokerIPAtNode\": \"" + currentLeaderBrokerPrivateIP + "\"}";
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(requestBody, headers);
            restTemplate.postForObject(configServerUrl + "/leader-not-responding", entity, Void.class);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void triggerDeregistration() {
        System.out.println("Deregistering from Coordinator Server...");
        // try {
        // if (broker != null) {
        // deregisterFromConfigServer(broker.getUniqueId());
        // }
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        // Deregister from the config server before the instance terminates

    }

}
