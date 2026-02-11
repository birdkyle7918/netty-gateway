package com.example.nettygateway.dto;

import lombok.Data;

@Data
public class VehicleLocation {
    private String deviceId; // 终端手机号
    private double latitude; // 纬度
    private double longitude;// 经度
    private long timestamp;  // 上报时间

    public VehicleLocation(String deviceId, double latitude, double longitude) {
        this.deviceId = deviceId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = System.currentTimeMillis();
    }


    @Override
    public String toString() {
        return String.format("Vehicle{id='%s', lat=%.6f, lon=%.6f}", deviceId, latitude, longitude);
    }
}