package com.cs.rfq.utils;

import com.cs.rfq.decorator.Rfq;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class HistoryServer {
    private List<Rfq> history;

    public HistoryServer() {
        history = new ArrayList<>();
    }


    public void addRfq(String jsonData) {
        Rfq rfq = Rfq.fromJson(jsonData);
        if (!history.contains(rfq)) {
            history.add(rfq);
        }
    }

    public String getRfq(int index) {
        if (--index < history.size()) {
            return history.get(index).getRfqString();
        }
        return null;
    }

    public static void main(String[] args) {
        HistoryServer historyServer = new HistoryServer();
//        historyServer.addRfq("Hi");
//        historyServer.addRfq("{\"a\": \"1\"}");
        historyServer.addRfq("{ \"id\" :  1, \"traderId\" :  6089985145335003723, \"entityId\" :  5561279226039690843, \"instrumentId\" :  \"AT0000A105W3\", \"qty\" :  100, \"price\" :  1.4, \"side\" :  \"B\"}");
        System.out.println(historyServer);
    }

    @Override
    public String toString() {
        ListIterator<Rfq> iterator = history.listIterator();
        String ret = "------------------------------\n";
        ret += "History for current session: \n";
        int cnt = 0;
        while (iterator.hasNext()) {
            ret += String.format("%d : %s\n", ++cnt, iterator.next().getRfqString());
        }
        ret += "------------------------------";
        return ret;
    }
}
