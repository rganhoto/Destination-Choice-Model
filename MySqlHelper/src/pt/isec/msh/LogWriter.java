package pt.isec.msh;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.logging.Logger;

/**
 * Created by Rui on 12/03/2016.
 */
public class LogWriter {


    public static void WriteSucessLog(String Message) {
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("MESSASES.log", true)));
            out.println(new Date());
            out.println(Message);
            out.println();
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void WriteErrorLogs(Exception ex) {
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("ERRORS.log", true)));
            out.println(new Date());
            out.println(ex.getMessage());
            out.println(ex.getStackTrace());
            out.println();
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void WriteTimeLog(String Text)
    {
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("TIME.log", true)));
            out.println(new Date());
            out.println(Text);
            out.println();
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
