import com.branegy.inventory.api.InventoryService
import com.branegy.dbmaster.model.*

import io.dbmaster.sync.*

import com.branegy.dbmaster.sync.api.SyncService
import com.branegy.dbmaster.sync.api.SyncPair.ChangeType
import com.branegy.email.EmailSender

import java.text.DateFormat

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import com.branegy.service.base.api.ProjectService
import com.branegy.tools.impl.presenter.DirectHmltDataPresenter;

def invService = dbm.getService(InventoryService.class)

logger.info("Loading databases via all project connections")

def syncSession = invService.getDatabaseListDiff(logger);
def version = new Date();
def emailBody = null;

if (syncSession.getSyncResult().getChangeType() == ChangeType.EQUALS){
    logger.info("No changes found")
    print "No changes found";
} else {
    logger.info("Changes found. Generating preview")
    emailBody = dbm.getService(SyncService.class).generateSyncSessionPreviewHtml("/db-sync-summary.groovy", syncSession, true);
    print emailBody;
    
    logger.info("Importing changes into inventory")
    syncSession.applyChanges();
    
    logger.info("Sync was completed successfully")
}

if (p_emails!=null && emailBody!=null) {
    EmailSender email = dbm.getService(EmailSender.class);
    def emailDf = DateFormat.getDateTimeInstance(DateFormat.SHORT,DateFormat.SHORT, Locale.US);
    String[] to = p_emails.split("[,;]");
    if (!emailBody.isEmpty()) {
        String projectName =  dbm.getService(ProjectService.class).getCurrentProject().getName();
        
        def subject = "Project ${projectName}: inventory changes";
        def body = """\
                      Please find changes attached
                      These changes were automatically imported into inventory at ${emailDf.format(version)}.
                      """.stripIndent();
                      
        emailBody = "<!DOCTYPE html><html><head><style type=\"text/css\">"+
        // TODO do refactoring
        IOUtils.toString(DirectHmltDataPresenter.class.getResource("extra.css"), Charsets.UTF_8)+
        "</style></head><body><div>" + emailBody + "</div></body></html>";
        
        email.createMessage(to[0], subject, body, true)
        email.addAttachment("changes.html", emailBody)
    }
    
    for (int i=1; i<to.length; ++i){
        email.addRecepient(RecipientType.TO, to[i]);
    }
    email.sendMessage();
}