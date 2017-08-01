import java.util.Comparator
import java.util.Arrays
import java.util.Collections

import static org.apache.commons.lang.StringEscapeUtils.escapeHtml

import com.branegy.dbmaster.sync.api.*
import com.branegy.dbmaster.sync.api.SyncPair.ChangeType

class PreviewGenerator {
    def colors = [
        "EQUALS"   : "",
        "NEW"      : "rgb(109, 241, 6)" ,
        "CHANGED"  : "rgb(255, 255, 128)",
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
        sb = new StringBuilder(5*1024*1024);
        String longTextString = session.getParameter("longText");
        longText = longTextString == null ? Collections.emptySet()
            : Arrays.asList(longTextString.split(";")) as Set;
        dumpItem(0, session.getSyncResult());
        return sb.toString();
    }
    
    public String getType(Object o) {
        return o==null ? "" : o.getClass().getName()
    }
    
    private Integer inc(Integer i){
        return Integer.valueOf(i.intValue()+1);
    }
    
    private cleanDefault (String value) {
        if (value==null || value.length()<2 || !(value.startsWith("(")  && value.endsWith(")"))) {
            return escapeHtml(value);
        } else {
            return cleanDefault(value[1..value.length()-2])
        }
    }
    
    private void dumpItem(int level, SyncPair pair) {
        def typeOrder = { obj -> ["Table", "View", "Procedure", "Function" ].findIndexOf { obj.equals(it) } }
        if (level==0) { // model level
            sb.append("""<script type=\"text/javascript\">
                     function toggle(id) {
                         var e = document.getElementById(id);
                         if (e.style.display == 'block') e.style.display = 'none';  else e.style.display = 'block';
                     }
                     </script>""");
            
            
            def childItems = pair.children.sort 
                   { a,b -> (typeOrder(a.objectType) <=> typeOrder(b.objectType)) * 10 + (a.pairName <=> b.pairName) }
            sb.append("<h1>Change Summary</h1>");
            sb.append("""<table cellspacing="0" class="simple-table" style="width:100%">
                          <tr>
                            <td>Change</td>
                            <td>Object Type</td>
                            <td>Name</td>
                          </tr>""");

            childItems.each { child ->
                if (showChangesOnly && child.changeType==SyncPair.ChangeType.EQUALS && !child.isOrderChanged()) {
                   return
                }
                sb.append("""<tr>
                              <td style="background-color:${colors[child.getChangeType().toString()]}">${child.changeType}</td>
                              <td>${child.objectType}</td>
                              <td><a target="_self" href="javascript:void(location.hash='#pair${child.id}')">${child.pairName}</a></td>
                            </tr>""")
            }
            sb.append("</table>")

            sb.append("<h1>Change Details</h1>");
            childItems.each { dbObject ->
                if (showChangesOnly && dbObject.changeType==SyncPair.ChangeType.EQUALS && !dbObject.isOrderChanged()) {
                    return
                }
                // sb.append("""<div style="background-color:${colors[dbObject.changeType.toString()]}">""")
                sb.append("""<div style="background-color:#f1f1f1;padding:15px;margin-bottom:20px;border:5px solid ${colors[dbObject.changeType.toString()]}">""")                
                dumpItem(level+1, dbObject)
                sb.append("</div>")
            }
        } else { // object level
            
            sb.append("""<h2 id="pair${pair.id}">${pair.objectType} ${pair.pairName} (${pair.changeType.toString().toLowerCase()})</h2>""");

            def attributes = pair.getAttributes()
               .findAll { (!it.attributeName.equals("Source")) && (!showChangesOnly || it.changeType!=SyncAttributePair.AttributeChangeType.EQUALS) }
               .sort    { a,b -> a.attributeName <=> b.attributeName }
               
            if (attributes.size()>0) {
                sb.append("<h3>Attributes</h3>");
                if (pair.changeType==SyncPair.ChangeType.CHANGED) {

                    sb.append("""<table cellspacing="0" class="simple-table" style="width:100%">
                          <tr>
                            <td>Change</td>
                            <td>Attribute</td>
                            <td>Source Value</td>
                            <td>Target Value</td>
                          </tr>""");
                    attributes.each { attr -> 
                        if (longText.contains(pair.getObjectType()+"."+attr.getAttributeName())) {
                            sb.append("""<tr>
                                     <td style="background-color:${colors[attr.changeType.toString()]}">${attr.changeType}</td>
                                     <td>${attr.attributeName}<span><!--hide:${pair.getObjectType()} ${attr.attributeName}-->&nbsp;<a href="">(view changes)</a> </span></td>
                                     <td><div style="display:none">${attr.sourceValue==null ? "-not defined-" : attr.sourceValue}</div></td>
                                     <td><div style="display:none">${attr.targetValue==null ? "-not defined-" : attr.targetValue}</div></td></tr>""")                        
                        } else {
                            sb.append("""<tr>
                                  <td style="background-color:${colors[attr.changeType.toString()]}">${attr.changeType}</td>
                                  <td>${attr.attributeName}</td>
                                  <td>${attr.sourceValue==null ? "-not defined-" : attr.sourceValue}</td>
                                  <td>${attr.targetValue==null ? "-not defined-" : attr.targetValue}</td>
                                </tr>""");
                        }
                    }
                } else {
                    // when object is deleted/created/not changed display only single level of values
                    sb.append("""<table cellspacing="0" class="simple-table" style="width:100%">
                                 <tr><td>Attribute</td><td>Value</td></tr>""");
                    attributes.each { attr -> 
                        def attrValue
                        if (attr.changeType == SyncAttributePair.AttributeChangeType.NEW) {
                            attrValue = attr.targetValue
                        } else {
                            attrValue = attr.sourceValue
                        }
                        if (longText.contains(pair.getObjectType()+"."+attr.getAttributeName())) {
                            // TODO add hide/show here
                            sb.append("""<tr>
                                     <td>${attr.attributeName}</td>
                                     <td><div>${attrValue==null ? "-not defined-" : attrValue}</div></td>
                                     </tr>""")
                        } else {
                            sb.append("""<tr><td>${attr.attributeName}</td><td>${attrValue==null ? "-not defined-" : attrValue}</td></tr>""");
                        }
                    }                
                }
                sb.append("</table>");
            }

            // Handling columns
            def columnPairs = pair.children.findAll { it.objectType.equals("Column") }
            if (columnPairs.size()>0) {
                sb.append("<h3>Columns</h3>")
                sb.append("""<table cellspacing="0" class="simple-table" style="width:100%">""")
                if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                    sb.append("<tr><td>Change</td><td>Column Name</td><td>Column Spec</td><td>Source Definition</td></tr>")
                } else {
                    sb.append("<tr><td>Column Name</td><td>Column Spec</td></tr>")
                }
                
                def columnDefinition = { column ->
                    if (column.getCustomData("is_computed") == 1) {
                        "AS "+column.getCustomData("computedExpression")
                        // TODO add is_persisted
                    } else {                                                            
                        def identity = column.getCustomData("is_identity")

                        column.prettyType.toUpperCase() + 
                        ((identity != null && identity == 1) ? " IDENTITY" : "") +
                        (column.nullable ? " NULL" : " NOT NULL") +
                        // TODO Add quotes
                        (column.defaultValue  ? " DEFAULT "+cleanDefault(column.defaultValue) : "")
                    }
                }
                
                columnPairs.each { p -> 
                    def column = (p.changeType == SyncPair.ChangeType.DELETED) ? p.source : p.target
                    sb.append("<tr>")
                    if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                        sb.append("<td style=\"background-color:")
                        if (p.changeType==SyncPair.ChangeType.EQUALS && p.orderChanged) {
                            sb.append(colors[SyncPair.ChangeType.CHANGED.toString()]).append("\">MOVED")
                        } else {
                            sb.append(colors[p.changeType.toString()]).append("\">").append(p.changeType.toString())
                        }
                        if (p.orderChanged) {
                            sb.append(" (")
                              .append(p.sourceIndex == null ? 999 : p.sourceIndex+1)
                              .append("&nbsp;to&nbsp;")
                              .append(p.targetIndex == null ? 999 : p.targetIndex+1)
                              .append(")")
                        }
                    }
                    sb.append("<td>").append(p.pairName).append("</td>")
                    sb.append("""<td>${columnDefinition(column)}</td>""")
                    if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                        if (p.changeType == SyncPair.ChangeType.CHANGED) {
                            sb.append("<td>").append(columnDefinition(p.source)).append("</td>")    
                        } else { 
                            sb.append("<td></td>")
                        }
                    }
                    sb.append("</tr>")
                }
                sb.append("</table>")
            }
            
            // Handling parameters
            def parameterPairs = pair.children.findAll { it.objectType.equals("Parameter") }
            if (parameterPairs.size()>0) {
                sb.append("<h3>Parameters</h3>")
                sb.append("""<table cellspacing="0" class="simple-table" style="width:100%">""")
                if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                    sb.append("<tr><td>Change</td><td>Parameter name</td><td>Parameter spec</td><td>Source definition</td></tr>")
                } else {
                    sb.append("<tr><td>Parameter name</td><td>Parameter spec</td></tr>")
                }
                
                // TODO - review this closure
                def parameterDefinition = { parameter ->
                        parameter.prettyType.toUpperCase() + 
                        (parameter.nullable ? " NULL" : " NOT NULL") +
                        // TODO Add quotes
                        (parameter.defaultValue  ? " DEFAULT "+cleanDefault(parameter.defaultValue) : "")
                }
                
                parameterPairs.each { p ->
                    def parameter = (p.changeType == SyncPair.ChangeType.DELETED) ? p.source : p.target
                    sb.append("<tr>")

                    if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                        sb.append("<td style=\"background-color:")
                        if (p.changeType==SyncPair.ChangeType.EQUALS && p.orderChanged) {
                            sb.append(colors[SyncPair.ChangeType.CHANGED.toString()]).append("\">MOVED")
                        } else {
                            sb.append(colors[p.changeType.toString()]).append("\">").append(p.changeType.toString())
                        }
                        if (p.orderChanged) {
                            sb.append(" (")
                              .append(p.sourceIndex == null ? 999 : p.sourceIndex+1)
                              .append("&nbsp;to&nbsp;")
                              .append(p.targetIndex == null ? 999 : p.targetIndex+1)
                              .append(")")
                        }
                    }

                    sb.append("<td>").append(p.pairName).append("</td>")
                    sb.append("""<td>${parameterDefinition(parameter)}</td>""")
                    if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                        if (p.changeType == SyncPair.ChangeType.CHANGED) {
                            sb.append("<td>").append(parameterDefinition(p.source)).append("</td>")    
                        } else { 
                            sb.append("<td></td>")
                        }
                    }
                    sb.append("</tr>")
                }
                sb.append("</table>")
            }            

            // Handle indexes
            def indexPairs = pair.children.findAll { it.objectType.equals("Index") }
            if (indexPairs.size()>0) {
                sb.append("<h3>Indexes</h3>")
                sb.append("""<table cellspacing="0" class="simple-table" style="width:100%">""")
                if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                    sb.append("<tr><td>Change</td><td>Index Name</td><td>Index Definition</td><td>Source Definition</td></tr>")
                } else {
                    sb.append("<tr><td>Index Name</td><td>Index Definition</td></tr>")
                }
                
                def columnsAsText = { columns, included ->
                    String result = null
                    if (columns!=null) {
                        def filtered = columns.findAll { it.included == included }
                        if (filtered.size()>0) {
                            result = filtered.collect { it.getColumnName()+ (it.isAsc() ? " asc" : " desc") }.join (", ")
                        }
                    }
                    return result
                }
        
                def indexDefinition = { index ->
                    String result=""
                    if (index.primaryKey) {
                        result="PRIMARY KEY"
                    }
                    if (!index.primaryKey) {
                        result+=" "+(index.unique ? "UNIQUE":"")
                    }

                    result +=" "+index.type.toString().toUpperCase();
                    result +=  " (" + columnsAsText(index?.getColumns(),false) + ")"
                    String included = columnsAsText(index?.getColumns(),true)
                    if (included!=null) {
                           result += " INCLUDE ("+included+")"
                    }
                    if (index.disabled) {
                            result += " DISABLED"
                    }
                    return result
                }
                
                indexPairs.each { p -> 
                    def index = (p.changeType == SyncPair.ChangeType.DELETED) ? p.source : p.target
                    sb.append("<tr>")
                    if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                        sb.append("""<td style="background-color:${colors[p.changeType.toString()]}">${p.changeType}</td>""")
                    }
                    if (p.changeType==SyncPair.ChangeType.CHANGED && !p.getSourceName().equals(p.getTargetName())) {
                        sb.append("<td>").append(p.getTargetName()).append(" (renamed from ").append(p.getSourceName()).append(")").append("</td>")
                    } else {
                        sb.append("<td>").append(p.pairName).append("</td>")
                    }
                    sb.append("""<td>${indexDefinition(index)}</td>""")
                    if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                        if (p.changeType == SyncPair.ChangeType.CHANGED) {
                            sb.append("<td>").append(indexDefinition(p.source)).append("</td>")    
                        } else { 
                            sb.append("<td></td>")
                        }
                    }
                    sb.append("</tr>")
                }
                sb.append("</table>")
            }
            
            // Handle constraints
            def constraintPairs = pair.children.findAll { it.objectType.equals("Constraint") }
            if (constraintPairs.size()>0) {
                sb.append("<h3>Constraints</h3>")
                sb.append("""<table cellspacing="0" class="simple-table" style="width:100%">""")
                if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                    sb.append("<tr><td>Change</td><td>Constraint Name</td><td>Constraint Definition</td><td>Source Definition</td></tr>")
                } else {
                    sb.append("<tr><td>Constraint Name</td><td>Constraint Definition</td></tr>")
                }

                def constraintDefinition = { constraint ->
                    String result=escapeHtml(constraint.definition)
                    if (constraint.disabled) {
                        result+=" DISABLED"
                    }
                    return result
                }
                
                constraintPairs.each { p -> 
                    def constraint = (p.changeType == SyncPair.ChangeType.DELETED) ? p.source : p.target
                    sb.append("<tr>")
                    if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                        sb.append("""<td style="background-color:${colors[p.changeType.toString()]}">${p.changeType}</td>""")
                    }
                    sb.append("<td>").append(p.pairName).append("</td>")
                    sb.append("""<td>${constraintDefinition(constraint)}</td>""")
                    if (pair.changeType==SyncPair.ChangeType.CHANGED) {
                        if (p.changeType == SyncPair.ChangeType.CHANGED) {
                            sb.append("<td>").append(constraintDefinition(p.source)).append("</td>")    
                        } else { 
                            sb.append("<td></td>")
                        }
                    }
                    sb.append("</tr>")
                }
                sb.append("</table>")
            }

            // Handling object source code
            def sourceAttr = pair.attributes.find { it.attributeName.equals("Source") }
            if (sourceAttr!=null) {
                sb.append("<h3>Source code");
                if (sourceAttr.changeType == SyncAttributePair.AttributeChangeType.CHANGED) {
                    sb.append(" (changed)");
                }
                sb.append("</h3>");
                if (sourceAttr.changeType == SyncAttributePair.AttributeChangeType.CHANGED) {
                    sb.append("<table><tr>");
                    
                    sb.append("<td>");
                    sb.append(sourceAttr.attributeName);
                    sb.append("<span><!--hide:");
                    sb.append(pair.getObjectType());
                    sb.append(' ');
                    sb.append(sourceAttr.attributeName);
                    sb.append("-->&nbsp;<a href=\"\">(view changes)</a></span></td>");
                    
                    sb.append("<td><div style=\"display:none\">");
                    sb.append(sourceAttr.sourceValue==null ? "-not defined-" : escapeHtml(sourceAttr.sourceValue));
                    sb.append("</div></td>");
                    
                    sb.append("<td><div style=\"display:none\">");
                    sb.append(sourceAttr.targetValue==null ? "-not defined-" : escapeHtml(sourceAttr.targetValue));
                    sb.append("</div></td>");
                    
                    sb.append("</tr></table>");
                } else {
                    sb.append("<a target=\"_self\" href=\"javascript:void(toggle('sc");
                    sb.append(pair.id);
                    sb.append("'))\">(Show\\hide source code)</a>");
                    
                    sb.append("<div id=\"sc");
                    sb.append(pair.id);
                    sb.append("\" style=\"display:none\"><pre>");
                    sb.append(escapeHtml(sourceAttr.changeType == SyncAttributePair.AttributeChangeType.NEW ? sourceAttr.targetValue : sourceAttr.sourceValue));
                    sb.append("</pre></div>");
                }
            }
            
            def childItems = pair.getChildren().each { child ->
                if (showChangesOnly && child.getChangeType()==SyncPair.ChangeType.EQUALS && !child.isOrderChanged()) {
                    return
                }
                if (!child.objectType.equals("Column") && !child.objectType.equals("Index") 
                 && !child.objectType.equals("Constraint") && !child.objectType.equals("Parameter")) {
                    dumpItem(level+1, child)
                }
            }
        }
    }
}

htmlPreview = new PreviewGenerator(showChangesOnly).generatePreview(syncSession);
if (!showChangesOnly){ // save for diff only
    syncSession.setParameter("html", htmlPreview); // TODO subject to change
}