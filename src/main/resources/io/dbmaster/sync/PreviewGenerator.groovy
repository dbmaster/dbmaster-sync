/*
 *  File Version:  $Id: PreviewGenerator.groovy 145 2013-05-22 18:10:44Z schristin $
 */

package io.dbmaster.sync;

import com.branegy.dbmaster.sync.api.*;

public class PreviewGenerator {
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

    public PreviewGenerator() {
        this(false)
    }

    public PreviewGenerator(boolean showChangesOnly) {
        this.showChangesOnly = showChangesOnly;
    }

    public synchronized String generatePreview(SyncSession session) {
        sb = new StringBuilder(100*1024);
        String longTextString = session.getParameter("longText");
        longText = new HashSet<String>(Arrays.asList(longTextString!=null? longTextString.split(";"):new String[0]));
        dumpItem(0, session.getSyncResult());
        return sb.toString();
    }

    public String getType(Object o) {
        return o==null ? "" : o.getClass().getName()
    }

    def syncPairSorter = { a,b ->
        def c = a.getObjectType().compareTo(b.getObjectType())
        if (c==0) {
            c = a.getChangeType().compareTo(b.getChangeType())
        }
        if (c==0) {
            c = a.getPairName().compareTo(b.getPairName())
        }
        return c
    }

    private void dumpItem(int level, SyncPair pair) {
        if (level==0) {
            if (showChangesOnly && pair.getChangeType()==SyncPair.ChangeType.EQUALS) {
                sb.append("No changes found between schemas")
                return
            }
            def childItems = pair.getChildren().sort(syncPairSorter)
            sb.append("""<table cellspacing="0" class="simple-table" style="width:100%">""")
            for (SyncPair child: childItems) {
               if (showChangesOnly && child.getChangeType()==SyncPair.ChangeType.EQUALS) {
                   continue
               }
               dumpItem(level+1, child)
            }
            sb.append("</table>")
        } else {
            def rowSpanNumber = 0
            def childItems = pair.getChildren().sort(syncPairSorter)
            if ((!showChangesOnly && childItems.size()>0)
                ||(showChangesOnly && childItems.find {it.getChangeType()!=SyncPair.ChangeType.EQUALS}!=null)) {
                rowSpanNumber++;
                sb.append("<!-- +1 child items changes only ${showChangesOnly}-->");
            }
            sb.append("""<tr>
                         <td style="vertical-align:top;width:20%;background-color:${colors[pair.getChangeType().toString()]}" rowspan=" """)

            int rowSpanPosition = sb.size()
            sb.append(""""> ${pair.getChangeType()}<br/>${pair.getObjectType()}<br/><b>${pair.getPairName()}</b></td>""")
            boolean noOutput = true
            if (childItems.size()>0) {
                for (SyncPair child: childItems) {
                    if (showChangesOnly && child.getChangeType()==SyncPair.ChangeType.EQUALS) {
                        continue
                    } else {
                        rowSpanNumber = 1
                        sb.append("<!-- =1 -->");
                    }
                    if (noOutput) {
                        sb.append("""<td colspan="3"><table style="width:100%;border:0" cellspacing="0" class="simple-table">""")
                        noOutput = false
                    }
                    dumpItem(level+1, child);
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
                if (showChangesOnly && attr.getChangeType()==SyncAttributePair.AttributeChangeType.EQUALS) {
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
                                 <td style="${style}"><div style="display:none"><pre>${attr.sourceValue==null ? "-not defined-" : attr.sourceValue}</pre></div></td>
                                 <td style="${style}"><div style="display:none"><pre>${attr.targetValue==null ? "-not defined-" : attr.targetValue}</pre></div></td>""")
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
