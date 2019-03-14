import java.util.Comparator
import java.util.Arrays
import java.util.Collections

import com.branegy.dbmaster.sync.api.*
import com.branegy.dbmaster.sync.api.SyncPair.ChangeType
import com.branegy.dbmaster.sync.api.SyncAttributePair.AttributeChangeType;

public class PreviewGenerator implements SummaryGenerator {
    // todo should be copied
    def changePairClass = [ "new", "changed", "changed", "deleted", ""];
    def changeAttrClass = [ "new", "changed", "deleted", "" ];
    
    private StringBuilder sb;
    private List<SyncPair> path = new ArrayList<SyncPair>(10);
    private showChangesOnly;
    private Set<String> longText;
   
    public PreviewGenerator() {
    }

    public PreviewGenerator(boolean showChangesOnly) {
        this.showChangesOnly = showChangesOnly
    }
    
    public String generateSummary(SyncSession session) {
        return generatePreview(session)    
    }

    public String generateSummary(SyncPair pair) {
        sb = new StringBuilder(100*1024)
        printSyncPair(pair)
        return sb.toString()    
    }

    public void setParameter(String parameterName, Object value) {
        if (parameterName.equals(SHOW_CHANGES_ONLY)) {
            showChangesOnly = (Boolean)value;
        }
    }

    public synchronized String generatePreview(SyncSession session) {
        sb = new StringBuilder(100*1024);
        String longTextString = session.getParameter("longText");
        longText = longTextString == null ? Collections.emptySet() : Arrays.asList(longTextString.split(";")) as Set;
            
        sb.append("""<script type="text/javascript">""");
        sb.append("""
                     var f = function(event){
                         event = event || window.event;
                         var target = event.target || event.srcElement;
                         if (target.tagName == "A"){
                             window.location.hash = target.href.substring(target.href.lastIndexOf('#'));
                         }
                         event.preventDefault ? event.preventDefault() : (event.returnValue=false);
                         return false;
                     }
                     if(window.addEventListener){
                         window.addEventListener("click", f, false);
                     } else {
                         window.attachEvent("onclick", f);
                     }
                     </script>
        """);
        
        SyncPair pair = session.getSyncResult()
        printSyncPair(pair)
        return sb.toString()
    }
    
    private void printSyncPair(SyncPair pair) {
        String type = pair.objectType
        def change = pair.getChangeType()
        if (type.equals("Inventory")) {
            if (change == ChangeType.EQUALS) {
                sb.append("No changes found")
                // TODO Handle errors
            } else {
                sb.append("<table class=\"simple-table\" cellspacing=\"0\" cellpadding=\"10\"><tr style=\"background-color:#EEE\"><td>Server</td><td>Change details</td></tr>")
                for (SyncPair child : pair.getChildren()) {
                    sb.append("<tr valign=\"bottom\"><td style=\"margin:3px; vertical-align:top\">")
                      .append(child.sourceName)
                      .append("</td><td style=\"margin:3px;vertical-align:top\">")
                    printSyncPair(child)
                    sb.append("</td></tr>")

                }
                sb.append("</table>")
            }
        } else if (type.equals("Server")) {
            def errorStatus = pair.getErrorStatus()
            if (errorStatus.errorStatus!=SyncPair.ErrorType.NONE || change != ChangeType.EQUALS) {
                if (errorStatus.errorStatus!=SyncPair.ErrorType.NONE) {
                    sb.append("Error:"  + errorStatus.syncPair.error)
                } else {
                    sb.append("<ul style=\"margin:0px;\" >");
                    if (pair.isAttributeChanges()) {
                        sb.append("<li>Change(s) in parameters/configuration/sysinfo</li>");
                        printAttributeDiff(pair,1)
                    }
                    for (SyncPair child : pair.getChildren()) {
                        if (child.changeType == ChangeType.CHANGED) {
                            String childType = child.objectType
                            if (childType.equals("Database")) {
                                sb.append("<li>Database " + child.sourceName + " changed</li>")
                            }
                            if (childType.equals("Job")) {
                                sb.append("<li>Job " + child.sourceName + " changed</li>")
                            }                            
                            printSyncPair(child)
                        }
                    }
                    def deletedJobs = pair.children.findAll { it.changeType == ChangeType.DELETED && it.objectType == "Job" }
                    def newJobs = pair.children.findAll { it.changeType == ChangeType.NEW && it.objectType == "Job" }
                    def newDbs  = pair.children.findAll { it.changeType == ChangeType.NEW && it.objectType == "Database" }
                    def deletedDbs = pair.children.findAll { it.changeType == ChangeType.DELETED && it.objectType == "Database" }
                    if (deletedDbs.size()==1) {
                        sb.append("<li>Database "+deletedDbs[0].sourceName + " was deleted</li>")                    
                    }
                    if (deletedDbs.size()>1) {
                        sb.append("<li>"+deletedDbs.size()+" databases were removed: "+deletedDbs.collect{it.sourceName}.join(", ")+"</li>")
                    }
                    if (newDbs.size()==1) {
                        sb.append("<li>Database "+newDbs[0].targetName + " was added</li>")
                    }
                    if (newDbs.size()>1) {
                        sb.append("<li>"+newDbs.size()+" databases added: "+newDbs.collect{it.targetName}.join(", ")+"</li>")
                    }

                    if (deletedJobs.size()==1) {
                        sb.append("<li>Job "+deletedJobs[0].sourceName + " was deleted</li>")                    
                    }
                    if (deletedJobs.size()>1) {
                        sb.append("<li>"+deletedJobs.size()+" jobs were removed: "+deletedJobs.collect{it.sourceName}.join(", ")+"</li>")
                    }
                    if (newJobs.size()==1) {
                        sb.append("<li>Job "+newJobs[0].targetName + " was added</li>")
                    }
                    if (newJobs.size()>1) {
                        sb.append("<li>"+newJobs.size()+" jobs added: "+newJobs.collect{it.targetName}.join(", ")+"</li>")
                    }                
                    sb.append("</ul>");
                }
            }
        } else if (type.equals("Database")) {
            printAttributeDiff(pair,1)
            if (pair.isChildrenChanges()) {
                sb.append("<ul>");
                for (SyncPair child : pair.getChildren()) {
                    if (child.changeType == ChangeType.CHANGED) {
                        String childType = child.objectType
                        if (childType.equals("File")) {
                            sb.append("<li>File " + child.sourceName + " changed</li>")
                        }
                        printSyncPair(child)
                    }
                }
                def deletedFiles = pair.children.findAll { it.changeType == ChangeType.DELETED && it.objectType == "File" }
                def newFiles = pair.children.findAll { it.changeType == ChangeType.NEW && it.objectType == "File" }
              
                if (deletedFiles.size()==1) {
                    sb.append("<li>File "+deletedFiles[0].sourceName + " was deleted</li>")
                }
                if (deletedFiles.size()>1) {
                    sb.append("<li>"+deletedFiles.size()+" files were removed: "+deletedFiles.collect{it.sourceName}.join(", ")+"</li>")
                }
                if (newFiles.size()==1) {
                    sb.append("<li>File "+newFiles[0].targetName + " was added</li>")
                }
                if (newFiles.size()>1) {
                    sb.append("<li>"+newFiles.size()+" files added: "+newFiles.collect{it.targetName}.join(", ")+"</li>")
                }
                sb.append("</ul>");
            }
        } else if (type.equals("File")) {
            printAttributeDiff(pair,2)
        } else if (type.equals("Job")) {
            printAttributeDiff(pair,1)
        }
    }
    
    private void printAttributeDiff(SyncPair pair, int depth) {
        if (!pair.isAttributeChanges()) {
            return;
        }
        
        sb.append("<ul style=\"list-style-type: square;\">")

        for (SyncAttributePair attribute: pair.getAttributes()) {
            def change = attribute.getChangeType()
            def name   = attribute.getAttributeName()
            def t = attribute.getTargetValue()
            def s = attribute.getSourceValue()
            
            if (change != AttributeChangeType.EQUALS) {
                sb.append("<li>")
                switch (attribute.getChangeType()) {
                    case AttributeChangeType.NEW:
                        sb.append(name+" set to "+t);
                        break
                    case AttributeChangeType.CHANGED:
                        sb.append(name+" changed from "+s+" to "+t);
                        break
                    case AttributeChangeType.DELETED: 
                        sb.append(name+" - value "+ s +" is removed");                    
                        break
                }
                sb.append("</li>")
            }
        }
        sb.append("</ul>")
    }
}

summaryGenerator = new PreviewGenerator()
