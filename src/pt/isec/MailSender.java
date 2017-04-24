package pt.isec;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * Created by Rui on 15/11/2016.
 */
public class MailSender {


    public static void SendMail(String to, String Subject, String TextMessage)
    {
        final String username = "misrganhoto@gmail.com";
        final String password = "1q2w3erui";

        Properties props = new Properties();
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        Session session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password);
                    }
                });

        try {

            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress("your_user_name@gmail.com"));
            message.setRecipients(javax.mail.Message.RecipientType.TO, InternetAddress.parse(to));
            message.setSubject(Subject);
            message.setText(TextMessage);

            Transport.send(message);

        } catch (MessagingException e) {
        }

    }

}
