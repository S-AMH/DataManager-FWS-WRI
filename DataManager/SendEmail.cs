using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace DataManager
{
    class SendEmail
    {
        public static void sendEmail(List<string> _recipeints, string _title = "", string _mailBody = "", bool _isHtml = false, string _attachment = "")
        {
            MailMessage msg = new MailMessage();
            var smtpClient = new SmtpClient("smtp.gmail.com", 587);
            smtpClient.UseDefaultCredentials = true;
            smtpClient.Credentials = new System.Net.NetworkCredential("hasanli.amir@gmail.com", "bwtubtybixmbothr");
            smtpClient.EnableSsl = true;

            foreach (var r in _recipeints)
                msg.To.Add(r);

            msg.From = new MailAddress("hasanli.amir@gmail.com");

            if (_title == "")
                msg.Subject = "NoSubject";
            else
                msg.Subject = _title;

            msg.Body = _mailBody;
            msg.IsBodyHtml = _isHtml;
            if (_attachment != "")
            {
                if (!File.Exists(_attachment))
                    throw new FileNotFoundException("Specified file attachment was not found.", _attachment);
                else
                    msg.Attachments.Add(new System.Net.Mail.Attachment(_attachment));
            }

            try
            {
                smtpClient.Send(msg);
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }
    }
}
