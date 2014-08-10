import java.util.Comparator;
import java.util.Arrays;
import java.util.Collections;

import com.branegy.dbmaster.sync.api.*;
import com.branegy.dbmaster.sync.api.SyncPair.ChangeType;

class PreviewGenerator{
    def colors = [
        "EQUALS"   : "",
        "NEW"      : "rgb(109, 241, 6)" ,
        "CHANGED"  : "rgb(255, 255, 128)" ,
        "COPIED"   : "rgb(255, 255, 128)",
        "DELETED"  : "rgb(249, 154, 156)"
    ]
    
    private StringBuilder sb;
    private showChangesOnly;
    private Set<String> longText;
   
    public PreviewGenerator(boolean showChangesOnly) {
        this.showChangesOnly = showChangesOnly;
    }
     
    public synchronized String generatePreview(SyncSession session) {
        sb = new StringBuilder(100*1024);
        String longTextString = session.getParameter("longText");
        longText = longTextString == null ? Collections.emptySet()
            : Arrays.asList(longTextString.split(";")) as Set;
        dumpItem(0, session.getSyncResult(), 0);
        return sb.toString();
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
                case ChangeType.COPIED:  return 1;
                case ChangeType.EQUALS:  return 2;
                case ChangeType.NEW:     return 3;
                default:
                    throw new RuntimeException();
            }
        }
    }
    
    def syncPairSorter = new PreviewComparatorByTarget();
        
    private Integer inc(Integer i){
        return Integer.valueOf(i.intValue()+1);
    }
    
    private void dumpItem(int level, SyncPair pair, int indexShift) {
        def childItems = pair.getChildren().sort(syncPairSorter)
        if (level==0) {
            sb.append("""<table cellspacing="0" class="simple-table" style="width:100%">""")
            String type = null;
            for (SyncPair child: childItems) {
                if (!child.getObjectType().equals(type)){
                    type = child.getObjectType();
                    indexShift = 0;
                }
                if (!child.isSelected() && child.isOrdered()){
                    indexShift++;
                }
                if (!child.isSelected() || (showChangesOnly 
                        && child.getChangeType()==SyncPair.ChangeType.EQUALS && !child.isOrdered())) {
                    continue;
                }
                dumpItem(level+1, child, indexShift);
            }
            sb.append("</table>")
        } else {
            def rowSpanNumber = 0 
            if ((!showChangesOnly && childItems.size()>0)
                ||(showChangesOnly && childItems.find {it.getChangeType()!=SyncPair.ChangeType.EQUALS}!=null)) {
                rowSpanNumber++;
                sb.append("<!-- +1 child items changes only ${showChangesOnly}-->");
            }
            sb.append("""<tr>
                         <td style="vertical-align:top;width:20%;background-color:${colors[pair.getChangeType().toString()]}" rowspan=" """)

            int rowSpanPosition = sb.size()
            sb.append(""""> ${pair.getChangeType()}<br/>${pair.getObjectType()}<br/><b>${pair.getPairName()}</b>""")
            if (pair.isOrdered()){
                if (pair.isOrderChanged()){
                    sb.append("""<br/>Move&nbsp;from&nbsp;index&nbsp;${inc(pair.getSourceIndex())}&nbsp;to&nbsp;${inc(pair.getTargetIndex())-indexShift}""");
                } else if (pair.getSourceIndex()==null){
                    sb.append("""<br/>Insert&nbsp;at&nbsp;index&nbsp;${inc(pair.getTargetIndex())-indexShift}""");
                } else if (pair.getTargetIndex()==null){
                    sb.append("""<br/>Delete&nbsp;at&nbsp;index&nbsp;${inc(pair.getSourceIndex())}""");
                } else {
                    sb.append("""<br/>Unchanged&nbsp;index&nbsp;${inc(pair.getTargetIndex())-indexShift}""");
                }
            }
            
            sb.append("""</td>""")
            boolean noOutput = true
            if (childItems.size()>0) {
                String type = null;
                for (SyncPair child: childItems) {
                    if (!child.getObjectType().equals(type)){
                        type = child.getObjectType();
                        indexShift = 0;
                    }
                    if (!child.isSelected() && child.isOrdered()){
                        indexShift++;
                    }
                    if (!child.isSelected() || (showChangesOnly 
                            && child.getChangeType()==SyncPair.ChangeType.EQUALS && !child.isOrdered())) {
                        continue
                    } else {
                        rowSpanNumber = 1
                        sb.append("<!-- =1 -->");
                    }
                    if (noOutput) {
                        sb.append("""<td colspan="3"><table style="width:100%;border:0" cellspacing="0" class="simple-table">""")
                        noOutput = false
                    }
                    dumpItem(level+1, child, indexShift);
                }
                if (!noOutput) {
                    sb.append("</table></td>")
                }
            }
            def attributes = pair.getAttributes().sort { a,b ->  
                def c = a.getChangeType().compareTo(b.getChangeType())
                if (c==0) {
                    c = a.attributeName.compareTo(b.attributeName)
                }
                return c
            }
            for (SyncAttributePair attr: attributes ) {
                if (!attr.isSelected() || (showChangesOnly 
                            && attr.getChangeType()==SyncAttributePair.AttributeChangeType.EQUALS)) {
                    continue
                } else {
                   rowSpanNumber++ 
                   sb.append("<!-- +1 attr ${attr.attributeName} -->");
                }
                if (noOutput) { 
                    noOutput = false
                } else {
                    sb.append("</tr><tr>")
                }
                def style = """background-color:${colors[attr.getChangeType().toString()]}"""

                if (longText.contains(pair.getObjectType()+"."+attr.getAttributeName())){
                        sb.append("""<td style="${style}">${attr.attributeName}<span><!--hide:${pair.getObjectType()} ${attr.attributeName}-->&nbsp;<a href="">(view changes)</a> </span></td>
                                 <td style="${style}"><div style="display:none">${attr.sourceValue==null ? "-not defined-" : attr.sourceValue}</div></td>
                                 <td style="${style}"><div style="display:none">${attr.targetValue==null ? "-not defined-" : attr.targetValue}</div></td>""")
                } else {
                        style = """style="${style}" """
                    sb.append("""<td ${style}>${attr.attributeName}</td>
                                 <td ${style}>${attr.sourceValue==null ? "-not defined-" : attr.sourceValue}</td>
                                 <td ${style}>${attr.targetValue==null ? "-not defined-" : attr.targetValue}</td>""")
                }
            }            
            if (rowSpanNumber == 0) { rowSpanNumber = 1 }
            sb.insert(rowSpanPosition, rowSpanNumber)
            if (noOutput) {
                sb.append("""<td colspan="3" style="border:0"></td>""")
            }
            sb.append("</tr>")
        }
    }
}

htmlPreview = new PreviewGenerator(showChangesOnly).generatePreview(syncSession);
if (!showChangesOnly){ // save for diff only
    syncSession.setParameter("html", htmlPreview); // TODO subject to change
}