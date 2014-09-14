import java.util.Comparator;
import java.util.Arrays;
import java.util.Collections;

import com.branegy.dbmaster.sync.api.*;
import com.branegy.dbmaster.sync.api.SyncPair.ChangeType;
import com.branegy.dbmaster.sync.api.SyncAttributePair.AttributeChangeType;

class PreviewGenerator{
    def changePairClass = [
        "new",
        "changed",
        "changed",
        "deleted",
        ""
    ];
    def changeAttrClass = [
        "new",
        "changed",
        "deleted",
        ""
    ];
    
    private StringBuilder sb;
    private List<SyncPair> path = new ArrayList<SyncPair>(10);
    private showChangesOnly;
    private Set<String> longText;
    private Deque<SyncPair> queue = new ArrayDeque<SyncPair>(1000);
   
    public PreviewGenerator(boolean showChangesOnly) {
        this.showChangesOnly = showChangesOnly;
    }
     
    public synchronized String generatePreview(SyncSession session) {
        sb = new StringBuilder(100*1024);
        String longTextString = session.getParameter("longText");
        longText = longTextString == null ? Collections.emptySet()
            : Arrays.asList(longTextString.split(";")) as Set;
            
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
                         window.addEventListener("click",f,false);
                     } else {
                         window.attachEvent("onclick",f);
                     }
        """);
        sb.append("</script>");
        
        SyncPair pair = session.getSyncResult();
        queue.addLast(pair);
        while (!queue.isEmpty()){
            SyncPair p = queue.removeFirst();
            printScreen(p);
            if (p.hasChildren()){
                p.getChildren().sort(syncPairSorter);
                for (SyncPair p2:p.getChildren()){
                   if (p2.hasChildrenChanges() || p2.hasAttributeChanges()){
                       queue.addLast(p2);
                   } 
                }
            }
        }
        return sb.toString();
    }
    
    private void printLink(Object hash, Object name){
        sb.append("<a href=\"#");
        sb.append(hash);
        sb.append("""" target="_self">""");
        sb.append(name);
        sb.append("</a>");
    }
    
    private void printBreadcrumb(SyncPair pair){
        while (pair!=null){
            path.add(pair);
            pair = pair.getParentPair();
        }
        for (int i=path.size()-1; i>0;--i){
            pair = path.get(i);
            sb.append(pair.getObjectType()+": ");
            printLink(pair.getId(),
                (pair.getTargetName()!=null?pair.getTargetName():pair.getSourceName())
            );
            sb.append(" &gt; ");
        }
        pair = path.get(0);
        sb.append(pair.getObjectType());
        sb.append(": ");
        sb.append("<b>");
        sb.append(pair.getTargetName()!=null?pair.getTargetName():pair.getSourceName());
        sb.append("</b>");
        if (pair.getChangeType()==ChangeType.NEW){
            sb.append(" (new)");
        } else if (pair.getChangeType()==ChangeType.DELETED){
            sb.append(" (deleted)");
        } else if (pair.getChangeType()==ChangeType.COPIED){
            sb.append(" (copied)");
        }
        path.clear();
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
            if (pair.hasChildrenChanges()){
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
                                        if (child.hasChildrenChanges() || child.hasAttributeChanges()){
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
                                        if (child.hasChildrenChanges() || child.hasAttributeChanges()){
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
            if (pair.hasAttributeChanges()){
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
    
    static class PreviewComparatorByTarget implements Comparator<SyncPair>{
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

        private int getSyncPairByOperationType(SyncPair pair){
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

htmlPreview = new PreviewGenerator(showChangesOnly).generatePreview(syncSession);
if (!showChangesOnly){ // save for diff only
    syncSession.setParameter("html", htmlPreview); // TODO subject to change
}