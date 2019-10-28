import java.util.Comparator
import java.util.Arrays
import java.util.Collections

import com.branegy.dbmaster.sync.api.*
import com.branegy.dbmaster.sync.api.SyncPair.ChangeType
import com.branegy.dbmaster.sync.api.SyncAttributePair.AttributeChangeType;
import com.branegy.util.DateTimeUtils;
import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;


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
		def servers = pair.getChildren().sort { it.sourceName }
                for (SyncPair serverPair : servers) {
                    def errorStatus = serverPair.getErrorStatus()
                    if (errorStatus.errorStatus!=SyncPair.ErrorType.NONE || serverPair.getChangeType()!=ChangeType.EQUALS) {
                        def lastSyncDate = serverPair.target.getCustomData("Last Sync Date");
                        sb.append("<tr valign=\"bottom\"><td style=\"margin:3px; vertical-align:top; white-space: nowrap\">")
                          .append(serverPair.sourceName).append("<br/>")
                          .append("Last Sync: "+(lastSyncDate == null ? "Initial sync" : DateTimeUtils.dateTimeShort(lastSyncDate)))
                          .append("</td><td style=\"margin:3px;vertical-align:top\">")
                        printSyncPair(serverPair)
                        sb.append("</td></tr>")
                    }
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
                            if (childType.equals("ServerPrincipal")) {
                                sb.append("<li>Server principal " + child.sourceName + " changed</li>")
                            }                            
                            printSyncPair(child)
                        }
                    }
                    def deletedDbs = pair.children.findAll { it.changeType == ChangeType.DELETED && it.objectType == "Database" }
                    if (deletedDbs.size()==1) {
                        sb.append("<li>Database "+deletedDbs[0].sourceName + " was deleted</li>")                    
                    }
                    if (deletedDbs.size()>1) {
                        sb.append("<li>"+deletedDbs.size()+" databases were deleted: "+deletedDbs.collect{it.sourceName}.join(", ")+"</li>")
                    }
                    def newDbs  = pair.children.findAll { it.changeType == ChangeType.NEW && it.objectType == "Database" }
                    if (newDbs.size()==1) {
                        sb.append("<li>Database "+newDbs[0].targetName + " was added</li>")
                    }
                    if (newDbs.size()>1) {
                        sb.append("<li>"+newDbs.size()+" databases added: "+newDbs.collect{it.targetName}.join(", ")+"</li>")
                    }

                    def newJobs = pair.children.findAll { it.changeType == ChangeType.NEW && it.objectType == "Job" }
                    def deletedJobs = pair.children.findAll { it.changeType == ChangeType.DELETED && it.objectType == "Job" }
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


                    def deletedServerPrincipals = pair.children.findAll { it.changeType == ChangeType.DELETED && it.objectType == "ServerPrincipal" }
                    if (deletedServerPrincipals.size()==1) {
                        sb.append("<li>Principal "+deletedServerPrincipals[0].sourceName + " was deleted</li>")                    
                    }
                    if (deletedServerPrincipals.size()>1) {
                        sb.append("<li>" + deletedServerPrincipals.size() + " princials were removed: " + deletedServerPrincipals.collect{it.sourceName}.join(", ")+"</li>")
                    }

                    def newServerPrincipals     = pair.children.findAll { it.changeType == ChangeType.NEW    && it.objectType == "ServerPrincipal" }
                    if (newServerPrincipals.size()==1) {
                        sb.append("<li>Principal "+newServerPrincipals[0].targetName + " was added</li>")
                    }
                    if (newServerPrincipals.size()>1) {
                        sb.append("<li>" + newServerPrincipals.size()+" principals added: " + newServerPrincipals.collect{it.targetName}.join(", ")+"</li>")
                    }
                    sb.append("</ul>");
                }
            }
        } else if (type.equals("Database")) {
            printAttributeDiff(pair, 1)
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
        } else if (type.equals("ServerPrincipal")) {
            printAttributeDiff(pair ,2)
        } else if (type.equals("File")) {
            printAttributeDiff(pair, 2)
        } else if (type.equals("Step")) {
            printAttributeDiff(pair, 2)
        } else if (type.equals("Schedule")) {
            printAttributeDiff(pair, 2)
        } else if (type.equals("Job")) {
            printAttributeDiff(pair, 1)
            if (pair.isChildrenChanges()) {
                sb.append("<ul>");
                for (SyncPair child : pair.getChildren()) {
                    if (child.changeType == ChangeType.CHANGED) {
                        String childType = child.objectType
                        if (childType.equals("Step")) {
                            sb.append("<li>Step " + child.sourceName + " changed</li>")
                        } else if (childType.equals("Schedule")) {
                            sb.append("<li>Schedule " + child.sourceName + " changed</li>")
                        }
                        printSyncPair(child)
                    }
                }
                def deletedSteps = pair.children.findAll { it.changeType == ChangeType.DELETED && it.objectType == "Step" }
                def newSteps = pair.children.findAll { it.changeType == ChangeType.NEW && it.objectType == "Step" }
              
                if (deletedSteps.size()==1) {
                    sb.append("<li>Step "+deletedSteps[0].sourceName + " was deleted</li>")
                }
                if (deletedSteps.size()>1) {
                    sb.append("<li>"+deletedSteps.size()+" steps were deleted: "+deletedSteps.collect{it.sourceName}.join(", ")+"</li>")
                }
                if (newSteps.size()==1) {
                    sb.append("<li>Step "+newSteps[0].targetName + " was added</li>")
                }
                if (newSteps.size()>1) {
                    sb.append("<li>"+newSteps.size()+" steps added: "+newSteps.collect{it.targetName}.join(", ")+"</li>")
                }
                
                def deletedSchedules = pair.children.findAll { it.changeType == ChangeType.DELETED && it.objectType == "Schedule" }
                def newSchedules = pair.children.findAll { it.changeType == ChangeType.NEW && it.objectType == "Schedule" }
              
                if (deletedSchedules.size()==1) {
                    sb.append("<li>Schedule "+deletedSchedules[0].sourceName + " was deleted</li>")
                }
                if (deletedSchedules.size()>1) {
                    sb.append("<li>"+deletedSchedules.size()+" schedules were deleted: "+deletedSchedules.collect{it.sourceName}.join(", ")+"</li>")
                }
                if (newSchedules.size()==1) {
                    sb.append("<li>Schedule "+newSchedules[0].targetName + " was added</li>")
                }
                if (newSchedules.size()>1) {
                    sb.append("<li>"+newSchedules.size()+" schedules added: "+newSchedules.collect{it.targetName}.join(", ")+"</li>")
                }                
                sb.append("</ul>");
            }
        }
    }
    
    private void printAttributeDiff(SyncPair pair, int depth) {
        if (!pair.isAttributeChanges()) {
            return;
        }
        
        sb.append("<ul style=\"list-style-type: square;margin-left:${depth}em\">")

        for (SyncAttributePair attribute: pair.getAttributes()) {
            def change = attribute.getChangeType()
            def name   = attribute.getAttributeName()
            def t = attribute.getTargetValue()
            def s = attribute.getSourceValue()
            
            if (change != AttributeChangeType.EQUALS) {
                sb.append("<li>")
                if (name == "command" && pair.objectType == "Step") {
                   
                     switch (attribute.getChangeType()) {
                        case AttributeChangeType.NEW:
                            sb.append(name+" added");
                            break
                        case AttributeChangeType.DELETED:
                            sb.append(name+" - value is removed");
                            break
                        case AttributeChangeType.CHANGED:
                            sb.append(name+" changed");
                            break
                    }
                    
                    if (change == SyncAttributePair.AttributeChangeType.CHANGED) {
                        String id = pair.getId()+"cmd";
                        
                        sb.append("&nbsp;<span><a href=\"#\" data-type=\"popup-cmp\" data-title=\"");
                        sb.append(pair.getObjectType());
                        sb.append(' ');
                        sb.append(name);
                        sb.append("\" id=\"");
                        sb.append(id);
                        sb.append("\">(view changes)</a></span>");
                        
                        sb.append("<div style=\"display:none\" id=\"");
                        sb.append(id);
                        sb.append("s\">");
                        sb.append(s==null ? "-not defined-" : escapeHtml(s));
                        sb.append("</div>");
                        
                        sb.append("<div style=\"display:none\" id=\"");
                        sb.append(id);
                        sb.append("t\">");
                        sb.append(t==null ? "-not defined-" : escapeHtml(t));
                        sb.append("</div>");
                    }
                } else {
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
                }
                sb.append("</li>")
            }
        }
        sb.append("</ul>")
    }
}

summaryGenerator = new PreviewGenerator()
