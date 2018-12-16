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
        // System.out.print("Type = " + type)
        if (type.equals("Inventory")) {
            if (change == ChangeType.EQUALS) {
                sb.append("No changes found")
                // TODO Handle errors
            } else {
                sb.append("<table class=\"simple-table\" cellspacing=\"0\" cellpadding=\"10\"><tr style=\"background-color:#EEE\"><td>Server</td><td>Change details</td></tr>")
                for (SyncPair child : pair.getChildren()) {
                    printSyncPair(child)
                }
                sb.append("</table>")
            }
        } else if (type.equals("Server")) {
            def errorStatus = pair.getErrorStatus()
            if (errorStatus.errorStatus!=SyncPair.ErrorType.NONE || change != ChangeType.EQUALS) {
                sb.append("<tr valign=\"bottom\"><td style=\"margin:3px; vertical-align:top\">").append(pair.sourceName).append("</td><td style=\"margin:3px;vertical-align:top\">")
                if (errorStatus.errorStatus!=SyncPair.ErrorType.NONE) {
                    sb.append("Error:"  + errorStatus.syncPair.error)
            //    } else if (change == ChangeType.EQUALS) {
            //        sb.append("No changes found")                
                } else {
                    sb.append("<ul style=\"margin:0px;\" >");
                    def hasChanges = (pair.getAttributes().find { it.changeType != AttributeChangeType.EQUALS } != null) 
                    if (hasChanges) {
                        sb.append("<li>Change(s) in parameters/configuration/sysinfo</li>");
                        printAttributeDiff(pair)
                    }
                    for (SyncPair child : pair.getChildren()) {
                        if (child.changeType == ChangeType.CHANGED) {
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
                sb.append("</td></tr>")
            }
        } else if (type.equals("Database")) {
            sb.append("<li>Database " + pair.sourceName + " changed</li>")
            printAttributeDiff(pair)
        } else if (type.equals("Job")) {
            sb.append("<li>Job " + pair.sourceName + " changed</li>")
            printAttributeDiff(pair)
        }
    }
    
    private void printAttributeDiff(SyncPair pair) {
        sb.append("<ul style=\"list-style-type: square;margin-left: 1em;\">")

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
    
    private void printLink(Object hash, Object name){
        sb.append("<a href=\"#");
        sb.append(hash);
        sb.append("""" target="_self">""");
        sb.append(name);
        sb.append("</a>");
    }
    
    private void printScreen(SyncPair pair){
        sb.append("<a name=\"");
        sb.append(pair.getId());
        sb.append("\">");
        sb.append("</a>");
        sb.append("""<div class="item """);
        sb.append("border-"+changePairClass.get(pair.getChangeType().ordinal()));
        sb.append("\">");
        sb.append("""<div class="header">""");
        printBreadcrumb(pair);
        sb.append("</div>");
        if (pair.isChildrenChanges()){
            sb.append("""<table cellspacing=\"0\" class="simple-table child">""");
            sb.append("<tr>");
            sb.append("<th>Type</th>");
            sb.append("<th>Status</th>");
            sb.append("<th>Source</th>");
            sb.append("<th>Index</th>");
            sb.append("<th>Target</th>");
            sb.append("</tr>");
                    for (SyncPair child: pair.getChildren()){
                        if (!showChangesOnly || child.isOrdered() || child.getChangeType()!=ChangeType.EQUALS){
                            sb.append("<tr class=\"");
                            sb.append(changePairClass.get(child.getChangeType().ordinal()));
                            sb.append("\">");
                            sb.append("<td>");
                            sb.append(child.getObjectType());
                            sb.append("</td>");
                            sb.append("<td>");
                            sb.append(""+child.getChangeType());
                            sb.append("</td>");
                            sb.append("<td>");
                            if (child.getSourceName()!=null){
                                if (child.isChildrenChanges() || child.isAttributeChanges()){
                                    printLink(child.getId(),child.getSourceName());
                                } else {
                                    sb.append(child.getSourceName());
                                }
                            }
                            sb.append("</td>");
                            if (child.isOrderChanged()){
                                sb.append("<td class=\"changed\">");
                            } else {
                                sb.append("<td>");
                            }
                            if (child.isOrdered()){
                                if (child.getSourceIndex()!=null){
                                    sb.append("<div class=\"i-s\">");
                                    sb.append(child.getSourceIndex()+1);
                                    sb.append("</div>");
                                    
                                    if (child.getTargetIndex()!=null){
                                        sb.append("<div class=\"i-a\">&rarr;</div>");
                                        sb.append("<div class=\"i-t\">");
                                        sb.append(child.getTargetIndex()+1);
                                        sb.append("</div>");
                                    }
                                } else {
                                    sb.append("<div class=\"i-t\" style=\"float:right;\">");
                                    sb.append(child.getTargetIndex()+1);
                                    sb.append("</div>");
                                }
                            }
                            sb.append("</td>");
                            sb.append("<td>");
                                    if (child.getTargetName()!=null){
                                        if (child.isChildrenChanges() || child.isAttributeChanges()){
                                            printLink(child.getId(),child.getTargetName());
                                        } else {
                                            sb.append(child.getTargetName());
                                        }
                                    }
                                sb.append("</td>");
                            sb.append("<tr>");
                        }
                    }
                sb.append("</table>");
            }
            if (pair.isAttributeChanges()){
                sb.append("""<div class="attr-header">Attrubute changes</div>""");
                sb.append("""<table cellspacing=\"0\" class="simple-table attr">""");
                sb.append("<tr>");
                sb.append("<th>Status</th>");
                sb.append("<th>Name</th>");
                sb.append("<th>Source</th>");
                sb.append("<th>Target</th>");
                sb.append("</tr>");
                pair.getAttributes().sort { a,b ->
                    def c = a.getChangeType().compareTo(b.getChangeType())
                    if (c==0) {
                        c = a.attributeName.compareTo(b.attributeName)
                    }
                    return c
                }
                Integer changeType = pair.getChangeType()==ChangeType.NEW?AttributeChangeType.NEW.ordinal():(pair.getChangeType()==ChangeType.DELETED?AttributeChangeType.DELETED.ordinal():null);
                for (SyncAttributePair child: pair.getAttributes()){
                    if (!showChangesOnly || child.getChangeType()!=AttributeChangeType.EQUALS){
                        boolean pre = longText.contains(pair.getObjectType()+"."+child.getAttributeName());
                        
                        sb.append("<tr class=\"");
                        sb.append(changeAttrClass.get(changeType != null ? changeType: child.getChangeType().ordinal()));
                         sb.append("\">");
                            sb.append("<td>");
                                 sb.append(""+child.getChangeType());
                            sb.append("</td>");
                            sb.append("<td>");
                                 sb.append(child.getAttributeName());
                            sb.append("</td>");
                            sb.append("<td>");
                                if (child.getSourceValue()!=null){
                                    if (pre){
                                        sb.append("<pre>");
                                        sb.append(child.getSourceValue());
                                        sb.append("</pre>");
                                    } else {
                                        sb.append(child.getSourceValue());
                                    }
                                } else {
                                    sb.append("-not defined-");
                                }
                            sb.append("</td>");
                            sb.append("<td>");
                                if (child.getTargetValue()!=null){
                                    if (pre){
                                        sb.append("<pre>");
                                        sb.append(child.getTargetValue());
                                        sb.append("</pre>");
                                    } else {
                                        sb.append(child.getTargetValue());
                                    }
                                } else {
                                    sb.append("-not defined-");
                                }
                            sb.append("</td>");
                        sb.append("<tr>");
                    }
                }
                sb.append("</table>");
            }
        sb.append("</div>");
    }
    
    public String getType(Object o) {
        return o==null ? "" : o.getClass().getName()
    }
    
    static class PreviewComparatorByTarget implements Comparator<SyncPair> {
        @Override
        public int compare(SyncPair a, SyncPair b) {
            int result = a.getObjectType().compareTo(b.getObjectType())
            if (result == 0){
                int type1 = getSyncPairAsType(a);
                int type2 = getSyncPairAsType(b);
                result = type1-type2;
                if (result == 0){
                    if (type1 == 0){ // index collection
                        result = compareTo(a.getTargetIndex(),b.getTargetIndex());
                        if (result == 0 && a.getTargetIndex() == null){
                            result = compareTo(a.getSourceIndex(),b.getSourceIndex());
                        }
                    } else { // order by change type
                        type1 = getSyncPairByOperationType(a);
                        type2 = getSyncPairByOperationType(b);
                        result = type1-type2;
                        if (result == 0){
                            result = a.getPairName().compareToIgnoreCase(b.getPairName());
                        }
                    }
                }
            }
            return result;
        }
        
        private <T extends Comparable<T>> int compareTo(T o1, T o2){
            if (o1 == null && o2 == null){
                return  0;
            } else if (o1 == null){
                return +1; // null - last
            } else if (o2 == null){
                return -1; // null - last
            } else {
                return o1.compareTo(o2);
            }
        }
        
        private int getSyncPairAsType(SyncPair pair){
            if (pair.isOrdered()){
               return 0;
            } else {
               return 1;
            }
        }

        private int getSyncPairByOperationType(SyncPair pair) {
            switch (pair.getChangeType()){
                case ChangeType.DELETED: return 0;
                case ChangeType.CHANGED: return 1;
                case ChangeType.COPIED:  return 2;
                case ChangeType.EQUALS:  return 3;
                case ChangeType.NEW:     return 4;
                default:
                    throw new RuntimeException();
            }
        }
    }
    
    def syncPairSorter = new PreviewComparatorByTarget();
}

summaryGenerator = new PreviewGenerator()
// .generatePreview(syncSession)
// syncSession.setParameter("html", htmlPreview)