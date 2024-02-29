package com.example.BrokerNodeServer;

//Can update this class to be called BrokerData//think on it
public class Broker {
    private String ipAddress;
    private int port;
    private int uniqueId;
    private String ec2InstanceID;
    private boolean isLeader;

    public Broker() {
    }

    public Broker(String ipAddress, int port, int uniqueId, String ec2InstanceID) {
        this.ipAddress = ipAddress;
        this.port = port;
        this.uniqueId = uniqueId;
        this.ec2InstanceID = ec2InstanceID;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setLeader(boolean leader) {
        isLeader = leader;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(int uniqueId) {
        this.uniqueId = uniqueId;
    }

    public String getEC2instanceID() {
        return ec2InstanceID;
    }

    public void setEC2instanceID(String ec2InstanceID) {
        this.ec2InstanceID = ec2InstanceID;
    }
}