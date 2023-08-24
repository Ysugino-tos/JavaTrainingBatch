package com.s_giken.training.batch.model;

import java.time.LocalDate;

public class BatchDate {
    StringBuilder commandLineArg;
    LocalDate startJoinDate;
    LocalDate endJoinDate;

    public StringBuilder getCommandLineArg() {
        return commandLineArg;
    }
    public void setCommandLineArg(StringBuilder commandLineArg) {
        this.commandLineArg = commandLineArg;
    }
    public LocalDate getStartJoinDate() {
        return startJoinDate;
    }
    public void setStartJoinDate(LocalDate startJoinDate) {
        this.startJoinDate = startJoinDate;
    }
    public LocalDate getEndJoinDate() {
        return endJoinDate;
    }
    public void setEndJoinDate(LocalDate endJoinDate) {
        this.endJoinDate = endJoinDate;
    }
}
