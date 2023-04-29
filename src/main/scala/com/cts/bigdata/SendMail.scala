package com.cts.bigdata

import java.util.Properties
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail.{Message, PasswordAuthentication, Session, Transport}

object SendMail {
  def main(args: Array[String]) {
    /* This is used to read Properties files and get the values
    val is: InputStream = ClassLoader.getSystemResourceAsStream("pgmproperties.properties")
    val prop: Properties = new Properties()
    if (is != null) {
      prop.load(is)
    }
    else {
      throw new FileNotFoundException("Properties file cannot be loaded")
    }
    val sndrmailid = prop.getProperty("SndrMailId")      //Sender Mail id
    val psswd      = prop.getProperty("SndrMailPsswd")   //Sender gmail Mailbox id password
    val tomailid   = prop.getProperty("RcvrMailId")      // Receiver email id.
    */

    val sndrmailid = "XXXXXX" //Sender Mail id
    val psswd = "XXXXXX" //Sender gmail Mailbox id password
    val tomailid = "XXXXXX" // Receiver email id.

    sndmailwithAttachmnt(sndrmailid, psswd, tomailid)
    sndmail(sndrmailid, psswd, tomailid)
  }

  def sndmailwithAttachmnt(senderid: String, psswd: String, tomailid: String) = {
    val props = new Properties()
    props.put("mail.smtp.host", "smtp.gmail.com")
    props.put("mail.smtp.socketFactory.port", "465")
    props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory")
    props.put("mail.smtp.auth", "true")
    // props.put("mail.smtp.port", "465")

    val session = Session.getInstance(props, new javax.mail.Authenticator() {
      override protected def getPasswordAuthentication = new PasswordAuthentication(senderid, psswd)
    })

    try {
      val msg = new MimeMessage(session)
      msg.setFrom(new InternetAddress(senderid)) //Sender MailId
      val address = new InternetAddress(tomailid) //Address of recipient
      msg.setRecipient(Message.RecipientType.TO, address)
      msg.setSubject("Sample mail with attachment")

      //-----Create MailBody Using Below--------
      val messageBodyPart = new MimeBodyPart()
      messageBodyPart.setText("This is not a spam. This is attachment.")

      //-----Create Attachment Using Below--------
      val attchmentPart = new MimeBodyPart()
      attchmentPart.attachFile("F:\\bigdata\\Dataset\\wcdata.txt")

      //Combine Body and Attachment
      val multipart = new MimeMultipart()
      multipart.addBodyPart(messageBodyPart)
      multipart.addBodyPart(attchmentPart)

      //Setting the content of mail with main session
      msg.setContent(multipart)

      //-----Send mail------------------
      Transport.send(msg)

      println("Mail Successfully Sent with attachment..")

    } catch {
      case me: Exception =>
        me.printStackTrace()
    }

  }

  def sndmail(senderid: String, psswd: String, tomailid: String) = {
    val props = new Properties()
    props.put("mail.smtp.host", "smtp.gmail.com")
    props.put("mail.smtp.socketFactory.port", "465")
    props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory")
    props.put("mail.smtp.auth", "true")
    // props.put("mail.smtp.port", "465")

    val session = Session.getInstance(props, new javax.mail.Authenticator() {
      override protected def getPasswordAuthentication = new PasswordAuthentication(senderid, psswd)
    })

    try {
      val msg = new MimeMessage(session)
      msg.setFrom(new InternetAddress(senderid)) //Sender MailId
      val address = new InternetAddress(tomailid) //Address of recipient
      msg.setRecipient(Message.RecipientType.TO, address)
      msg.setSubject("Hello..Testing Mail")
      msg.setText("Hello this is not spam," +
        "\n\n This is a Scala Mail test...!")
      //-----Send mail------------------
      Transport.send(msg)

      println("Mail Successfully Sent..")

    } catch {
      case me: Exception =>
        me.printStackTrace()
    }

  }
}
