package com.poiitis.ducc;

import java.io.Serializable;

/**
 *
 * @author Poiitis Marinos
 */
public class Adult implements Serializable{

    private static final long serialVersionUID = -4226228608985544865L;
    
    private int age;
    private String workclass;
    private int fnlwgt;
    private String education;
    private int educationNum;
    private String maritalStatus;
    private String occupation;
    private String relationship;
    private String race;
    private String sex;
    private int capitalGain;
    private int capitalLoss;
    private int hoursPerWeek;
    private String nativeCountry;
    private Boolean overThanFifty;
    
    public Adult(String age, String workclass, String fnlwgt, String education,
            String educationNum, String maritalStatus, String occupation,
            String relationship, String race, String sex, String capitalGain,
            String capitalLoss, String hoursPerWeek, String nativeCountry, 
            String overThanFifty){
        
        this.age = Integer.parseInt(age);
        this.workclass = workclass;
        this.fnlwgt = Integer.parseInt(fnlwgt);
        this.education = education;
        this.educationNum = Integer.parseInt(educationNum);
        this.maritalStatus = maritalStatus;
        this.occupation = occupation;
        this.relationship = relationship;
        this.race = race;
        this.sex = sex;
        this.capitalGain = Integer.parseInt(capitalGain);
        this.capitalLoss = Integer.parseInt(capitalLoss);
        this.hoursPerWeek = Integer.parseInt(hoursPerWeek);
        this.nativeCountry = nativeCountry;
        this.overThanFifty = Boolean.getBoolean(overThanFifty);
        
    }
    
    public int getAge(){return age;}
    public String getWorkclass(){return workclass;}
    public int getFnlwgt(){return fnlwgt;}
    public String getEducation(){return education;}
    public int getEducationNum(){return educationNum;}
    public String getMaritalStatus(){return maritalStatus;}
    public String getOccupation(){return occupation;}
    public String getRelationship(){return relationship;}
    public String getRace(){return race;}
    public String getSex(){return sex;}
    public int getCapitalGain(){return capitalGain;}
    public int getCaputalLoss(){return capitalLoss;}
    public int getHoursPerWeek(){return hoursPerWeek;}
    public String getNativeCountry(){return nativeCountry;}
    public Boolean getOverThanFifty(){return overThanFifty;}
    
    
}
