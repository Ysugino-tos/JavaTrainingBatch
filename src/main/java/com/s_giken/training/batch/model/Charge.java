package com.s_giken.training.batch.model;

import java.time.LocalDate;

public class Charge {
    private int chargeId;
    private String name;
    private int amount;
    private LocalDate startDate;
    private LocalDate endDate;

    public int getChargeId() {
        return chargeId;
    }
    public void setChargeId(int chargeId) {
        this.chargeId = chargeId;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public int getAmount() {
        return amount;
    }
    public void setAmount(int amount) {
        this.amount = amount;
    }
    public LocalDate getStartDate() {
        return startDate;
    }
    public void setStartDate(LocalDate startDate) {
        this.startDate = startDate;
    }
    public LocalDate getEndDate() {
        return endDate;
    }
    public void setEndDate(LocalDate endDate) {
        this.endDate = endDate;
    }
}