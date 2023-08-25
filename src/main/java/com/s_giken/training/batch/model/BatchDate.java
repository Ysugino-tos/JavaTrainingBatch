package com.s_giken.training.batch.model;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class BatchDate {
    private String commandLineArg;
    private LocalDate startTargetDate;
    private LocalDate endTargetDate;
    private String printLogTargetDate;

    public BatchDate(){
        this.commandLineArg = " ";
        startTargetDate = LocalDate.of(2000,01,01);
        endTargetDate = LocalDate.of(2000,01,01);
        printLogTargetDate = " ";
    }

    public String getCommandLineArg() {
        return commandLineArg;
    }
    public String getPrintLogTargetDate() {
        return printLogTargetDate;
    }
    public LocalDate getStartTargetDate() {
        return startTargetDate;
    }
    public LocalDate getEndTargetDate() {
        return endTargetDate;
    }
    public void isBatchDate(String arg){
        DateTimeFormatter targetDateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE;
        DateTimeFormatter printLogTargetDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy"+"年"+"MM"+"月");
        
        commandLineArg = arg + "01";
		startTargetDate = LocalDate.parse(commandLineArg,targetDateTimeFormatter);
		endTargetDate = LocalDate.parse(commandLineArg,targetDateTimeFormatter).plusMonths(1);
        printLogTargetDate = LocalDate.parse(commandLineArg,targetDateTimeFormatter).format(printLogTargetDateTimeFormatter);
    }
}
