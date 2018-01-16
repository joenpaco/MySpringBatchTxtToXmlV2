package com.joenpaco.springbatch;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.joda.time.LocalDate;

@SuppressWarnings("restriction")
public class LocalDateAdapter extends XmlAdapter<String, LocalDate> {

    public LocalDate unmarshal(String v) throws Exception {
        return new LocalDate(v);
    }
 
    public String marshal(LocalDate v) throws Exception {
        return v.toString();
    }

}
