package io.dbmaster.sync;

import java.util.ArrayList
import java.util.Iterator
import java.util.List
import java.util.Map.Entry

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.branegy.dbmaster.sync.api.*
import com.branegy.dbmaster.sync.impl.*
import com.branegy.dbmaster.sync.api.SyncPair.ChangeType
import com.branegy.dbmaster.database.api.ModelService
import com.branegy.scripting.DbMaster
import com.branegy.service.core.QueryRequest
import com.branegy.dbmaster.sync.api.SyncAttributePair.SyncAttributeComparator;
import com.branegy.dbmaster.sync.api.SyncSession.SearchTarget
import com.branegy.dbmaster.sync.api.SyncService
import com.branegy.dbmaster.model.*

class ModelNamer implements Namer {
    @Override
    public String getName(Object o) {
        if (o instanceof DatabaseObject<?>) {
            return ((DatabaseObject<?>)o).getName();
        } else {
            throw new IllegalArgumentException("Unexpected object class "+o);
        }
    }

    @Override
    public String getType(Object o) {
        if (o instanceof Model)             {  return "Model"
        } else if (o instanceof Table)      {  return "Table"
        } else if (o instanceof View)       {  return "View"
        } else if (o instanceof Procedure)  {  return "Procedure"
        } else if (o instanceof Function)   {  return "Function"
        } else if (o instanceof Parameter)  {  return "Parameter"
        } else if (o instanceof Column)     {  return "Column"
        } else if (o instanceof Index)      {  return "Index"
        } else if (o instanceof Constraint) {  return "Constraint"
        } else if (o instanceof ForeignKey) {  return "ForeignKey"
        } else if (o instanceof Trigger)    {  return "Trigger"
        } else {
            throw new IllegalArgumentException("Unexpected object class "+o);
        }
    }
}

class ModelComparer extends BeanComparer {
    Model    source
    Model    target
    SyncAttributeComparator<?> sourceComparator;
    
    public ModelComparer(boolean ignoreWhitespaces) {
        sourceComparator = ignoreWhitespaces?SyncAttributeComparators.IGNORE_WHITESPACES:SyncAttributeComparators.DEFAULT;
    }
    
    private cleanDefault (String value) {
        if (value==null || value.length()<2 || !(value.startsWith("(")  && value.endsWith(")"))) {
            return value
        } else {
            return cleanDefault(value[1..value.length()-2])
        }
    }
    
    @Override
    public void syncPair(SyncPair pair, SyncSession session) {
        String objectType = pair.getObjectType();
        Namer namer = session.getNamer();
        String exludeObjects = session.getParameter("exclude_objects");
        if (objectType.equals("Model")) {
            Model sourceModel = (Model)pair.getSource();
            Model targetModel = (Model)pair.getTarget();
            pair.getChildren().addAll(mergeCollections(pair, sourceModel.getTables(), targetModel.getTables(), namer));
            pair.getChildren().addAll(mergeCollections(pair, sourceModel.getViews(),   targetModel.getViews(), namer));
            pair.getChildren().addAll(mergeCollections(pair, sourceModel.getProcedures(), targetModel.getProcedures(), namer));
            pair.getChildren().addAll(mergeCollections(pair, sourceModel.getFunctions(), targetModel.getFunctions(), namer));
        } else if (objectType.equals("Table")) {
            Table sourceTable = (Table)pair.getSource();
            Table targetTable = (Table)pair.getTarget();

            List<SyncPair> childPairs = pair.getChildren()
            
            childPairs.addAll(mergeLists(pair, sourceTable?.getColumns(), targetTable?.getColumns(), namer));
            childPairs.addAll(mergeCollections(pair, sourceTable?.getIndexes(), targetTable?.getIndexes(), namer));

            if (exludeObjects==null || !exludeObjects.contains("Constraints")) {
                childPairs.addAll(mergeCollections(pair, sourceTable?.getConstraints(), targetTable?.getConstraints(), namer));
            }
            
            if (exludeObjects==null || !exludeObjects.contains("Triggers")) {
                childPairs.addAll(mergeCollections(pair, sourceTable?.getTriggers(), targetTable?.getTriggers(), namer));
            }
        } else if (objectType.equals("View")) {
            View sourceView = (View)pair.getSource();
            View targetView = (View)pair.getTarget();

            List<SyncPair> childPairs = pair.getChildren()
            childPairs.addAll(mergeLists(pair, sourceView?.getColumns(),targetView?.getColumns(), namer));
            def attributes = pair.getAttributes()
            attributes.add(new SyncAttributePair("Source", sourceView?.getSource(),
                                                           targetView?.getSource(),
                                                           sourceComparator))
        } else if (objectType.equals("Function")) {
            Function sourceFunc = (Function)pair.getSource();
            Function targetFunc = (Function)pair.getTarget();

            List<SyncPair> childPairs = pair.getChildren()
            
            childPairs.addAll(mergeLists(pair, sourceFunc?.getParameters(),
                                               targetFunc?.getParameters(), namer));
            def attributes = pair.getAttributes()
            attributes.add(new SyncAttributePair("Source", sourceFunc?.getSource(),
                                                           targetFunc?.getSource(),
                                                           sourceComparator))
            attributes.add(new SyncAttributePair(Function.TYPE, sourceFunc?.getType(),
                                                                targetFunc?.getType()))
            attributes.add(new SyncAttributePair(Function.EXTRA_INFO, sourceFunc?.getExtraInfo(),
                                                                      targetFunc?.getExtraInfo()))
        } else if (objectType.equals("Procedure")) {
            Procedure sourceProc = (Procedure)pair.getSource();
            Procedure targetProc = (Procedure)pair.getTarget();

            List<SyncPair> childPairs = pair.getChildren()
            
            childPairs.addAll(mergeLists(pair, sourceProc?.getParameters(),
                                               targetProc?.getParameters(), namer));
            def attributes = pair.getAttributes()
            attributes.add(new SyncAttributePair("Source", sourceProc?.getSource(),
                                                           targetProc?.getSource(),
                                                           sourceComparator))
        } else if (objectType.equals("Column")) {
            Column sourceColumn = (Column)pair.getSource();
            Column targetColumn = (Column)pair.getTarget();

            def attributes = pair.getAttributes()
            attributes.add(new SyncAttributePair("Type", sourceColumn?.getPrettyType(),
                                                         targetColumn?.getPrettyType()))
            attributes.add(new SyncAttributePair("Nullable", sourceColumn?.isNullable(),
                                                             targetColumn?.isNullable()))

            attributes.add(new SyncAttributePair("Default Value", cleanDefault(sourceColumn?.getDefaultValue()),
                                                                  cleanDefault(targetColumn?.getDefaultValue())))
            
            attributes.add(new SyncAttributePair("Extra", sourceColumn?.getExtraDefinition(),
                                                          targetColumn?.getExtraDefinition()))
            
            // TODO check if null & null
            def sourceAttr = sourceColumn?.getCustomData("is_identity")
            def targetAttr = targetColumn?.getCustomData("is_identity")
            if (sourceAttr!=null && targetAttr!=null) 
            attributes.add(new SyncAttributePair("Identity", sourceAttr, targetAttr))
        } else if (objectType.equals("Parameter")) {
            Parameter sourceParameter = (Parameter)pair.getSource();
            Parameter targetParameter = (Parameter)pair.getTarget();
            def attributes = pair.getAttributes()
            attributes.add(new SyncAttributePair("Type", sourceParameter?.getPrettyType(),
                                                         targetParameter?.getPrettyType()))
/* ?? TODO Find out if it make sense to have nullable and default value
attributes.add(new SyncAttributePair("Nullable", sourceParameter?.isNullable(),
                                                             targetParameter?.isNullable()))
            attributes.add(new SyncAttributePair("Default Value", sourceParameter?.getDefaultValue(),
targetParameter?.getDefaultValue()))
            attributes.add(new SyncAttributePair("Kind",  sourceParameter?.getParamType()?.toString(),
                                                          targetParameter?.getParamType()?.toString()))
*/
        } else if (objectType.equals("Index")) {
            Index sourceIndex = (Index)pair.getSource();
            Index targetIndex = (Index)pair.getTarget();
        
            def attributes = pair.getAttributes()
            def columnsAsText = { columns, included ->
                String result = null
                if (columns!=null) {
                    def filtered = columns.findAll { it.included == included }
                    if ( included ) {
                       // For filtered columns order doesn't matter
                       filtered.sort { it.columnName  }
                    }
                    if (filtered.size()>0) {
                        result = filtered.collect { it.getColumnName()+ (it.isAsc() ? " asc" : " desc") }
                                         .join (",")
                    }
                }
                return result
            }

            attributes.add(new SyncAttributePair("Columns",
                                                 columnsAsText(sourceIndex?.getColumns(),false),
                                                 columnsAsText(targetIndex?.getColumns(),false)))
                                                            
            attributes.add(new SyncAttributePair("Included Columns",
                                                 columnsAsText(sourceIndex?.getColumns(),true),
                                                 columnsAsText(targetIndex?.getColumns(),true)))
                                                 
            attributes.add(new SyncAttributePair("Unique", sourceIndex?.isUnique(),
                                                           targetIndex?.isUnique()))

            attributes.add(new SyncAttributePair("Disabled", sourceIndex?.isDisabled(),
                                                             targetIndex?.isDisabled()))
        } else if (objectType.equals("Constraint")) {
            Constraint sourceConstraint = (Constraint)pair.getSource();
            Constraint targetConstraint = (Constraint)pair.getTarget();

            def attributes = pair.getAttributes()
            attributes.add(new SyncAttributePair("Definition", sourceConstraint?.getDefinition(),
                                                               targetConstraint?.getDefinition()))
            attributes.add(new SyncAttributePair("Disabled", sourceConstraint?.isDisabled(),
                                                             targetConstraint?.isDisabled()))

        
        } else if (objectType.equals("ForeignKey")) {
            // TODO Compare FKs
        } else if (objectType.equals("Trigger")) {
            Trigger sourceTrigger = (Trigger)pair.getSource();
            Trigger targetTrigger = (Trigger)pair.getTarget();
            
            def attributes = pair.getAttributes()
            
            attributes.add(new SyncAttributePair("Disabled",   sourceTrigger?.isDisabled(),
                                                               targetTrigger?.isDisabled()));
            attributes.add(new SyncAttributePair("Source",     sourceTrigger?.getSource(),
                                                               targetTrigger?.getSource(),
                                                               sourceComparator));
          
        } else {
            throw new SyncException("Unexpected object type "+ objectType);
        }
        
        Collection<String> keys = session.getParameter("customFieldMap").get(objectType);
        if (keys != null){
           if (pair.getSource() == null && pair.getTarget() != null){
               for (String key:keys){
                   pair.getAttributes().add(new SyncAttributePair(key,null, pair.getTarget().getCustomData(key)));
               }
           } else if (pair.getSource() != null && pair.getTarget() == null){
               for (String key:keys){
                   pair.getAttributes().add(new SyncAttributePair(key,pair.getSource().getCustomData(key),null));
               }
           } else {
               for (String key:keys){
                   pair.getAttributes().add(new SyncAttributePair(key,pair.getSource().getCustomData(key),
                       pair.getTarget().getCustomData(key)));
               }
           }
        }
    }
}

class ModelSyncSession extends SyncSession {
    DbMaster dbm
    Model sourceModel
    // modelService used in applyChanges
    ModelService modelService

    public ModelSyncSession(DbMaster dbm, boolean ignoreWhitespaces) {
        super(new ModelComparer(ignoreWhitespaces))
        setNamer(new ModelNamer())
        this.dbm = dbm
    }

    public void applyChanges() {
        try {
            this.modelService = dbm.getService(ModelService.class)

            importChanges(getSyncResult(), sourceModel)
            
            getAllParameters().remove("customFieldMap");
            
            SyncService syncService = dbm.getService(SyncService.class)
            syncService.saveSession(this, "Model Import")
            
            modelService.saveModel(sourceModel, null)
        } finally {
            dbm.closeResources()
        }
    }

    public void importChanges(SyncPair pair, DatabaseObject parentObject) {
        String objectType = pair.getObjectType();
        
        Collection<String> keys = getParameter("customFieldMap").get(objectType);
        if (keys!=null){
            DatabaseObject source = pair.getSource();
            if (pair.getChangeType() == ChangeType.CHANGED){
                for (SyncAttributePair attr : pair.getAttributes()) {
                    if (attr.getChangeType() != SyncAttributePair.AttributeChangeType.EQUALS &&
                            keys.contains(attr.getAttributeName())) {
                        source.setCustomData( attr.getAttributeName(), attr.getTargetValue()  )
                    }
                }
            }
        }
        
        if (objectType ==~ /Model/) {
            pair.getChildren().each { importChanges(it, sourceModel) }
        } else if (objectType.equals("Table")) {
            Table   sourceTable = (Table)pair.getSource();
            Table   targetTable = (Table)pair.getTarget();
            
            switch (pair.getChangeType()) {
                case ChangeType.NEW:
                    sourceModel.addTable(targetTable)
                    if (targetTable.isPersisted()) {
                        modelService.saveModelObject(targetTable)
                    } else {
                        modelService.createModelObject(targetTable)
                    }
                    break;
                case ChangeType.CHANGED:
                    //for (SyncAttributePair attr : pair.getAttributes()) {
                    //    if (attr.getChangeType() != SyncAttributePair.AttributeChangeType.EQUALS) {
                    //        sourceDB.setCustomData( attr.getAttributeName(), attr.getTargetValue()  )
                    //    }
                    //}
                    //inventorySrv.updateDatabase(sourceDB);
                    applyChangesForLists(sourceTable.getColumns(),targetTable.getColumns(), pair, "Column" );
                    pair.getChildren().each { importChanges(it, sourceTable) }
                    modelService.saveModelObject(sourceTable) 
                    break;
                case ChangeType.DELETED:
                    parentObject.removeTable(sourceTable)
                    modelService.deleteModelObject(sourceTable.getId())
                    break;
                case ChangeType.COPIED:
                    throw new RuntimeException("Not implemented change type ${pair.getChangeType()}")
                case ChangeType.EQUALS:
                    break;
                default:
                    throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
            }
        } else if (objectType.equals("View")) {
                View   sourceView = (View)pair.getSource();
                View   targetView = (View)pair.getTarget();
                
                switch (pair.getChangeType()) {
                    case ChangeType.NEW:
                        sourceModel.addView(targetView)
                        if (targetView.isPersisted()) {
                            modelService.saveModelObject(targetView)
                        } else {
                            modelService.createModelObject(targetView)
                        }
                        break;
                    case ChangeType.CHANGED:
                        sourceView.setSource( targetView.getSource() )
                        applyChangesForLists(sourceView.getColumns(),targetView.getColumns(), pair, "Column");
                        pair.getChildren().each { importChanges(it, sourceView) }
                        modelService.saveModelObject(sourceView)
                        break;
                    case ChangeType.DELETED:
                        parentObject.removeView(sourceView)
                        modelService.deleteModelObject(sourceView.getId())
                        break;
                    case ChangeType.COPIED:
                        throw new RuntimeException("Not implemented change type ${pair.getChangeType()}")
                    case ChangeType.EQUALS:
                        break;
                    default:
                        throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
                }          
        } else if (objectType.equals("Procedure")) {
                Procedure   sourceProcedure = (Procedure)pair.getSource();
                Procedure   targetProcedure = (Procedure)pair.getTarget();
                switch (pair.getChangeType()) {
                    case ChangeType.NEW:
                        sourceModel.addProcedure(targetProcedure)
                    
                        if (targetProcedure.isPersisted()) {
                            modelService.saveModelObject(targetProcedure)
                        } else {
                            modelService.createModelObject(targetProcedure)
                        }
                        break;
                    case ChangeType.CHANGED:
                        sourceProcedure.setSource( targetProcedure.getSource() )
                        applyChangesForLists(sourceProcedure.getParameters(),targetProcedure.getParameters(), 
                            pair, "Parameter");
                        pair.getChildren().each { importChanges(it, sourceProcedure) }
                        modelService.saveModelObject(sourceProcedure)
                        break;
                    case ChangeType.DELETED:
                        parentObject.removeProcedure(sourceProcedure)
                        modelService.deleteModelObject(sourceProcedure.getId())
                        break;
                    case ChangeType.COPIED:
                        throw new RuntimeException("Not implemented change type ${pair.getChangeType()}")
                    case ChangeType.EQUALS:
                        break;
                    default:
                        throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
                }
        } else if (objectType.equals("Function")) {
                Function   sourceFunction = (Function)pair.getSource();
                Function   targetFunction = (Function)pair.getTarget();
                switch (pair.getChangeType()) {
                    case ChangeType.NEW:
                        sourceModel.addFunction(targetFunction)
                    
                        if (targetFunction.isPersisted()) {
                            modelService.saveModelObject(targetFunction)
                        } else {
                            modelService.createModelObject(targetFunction)
                        }
                        break;
                    case ChangeType.CHANGED:
                        sourceFunction.setSource( targetFunction.getSource() )
                        sourceFunction.setType( targetFunction.getType() )
                        sourceFunction.setExtraInfo( targetFunction.getExtraInfo() )
                        applyChangesForLists(sourceFunction.getParameters(),targetFunction.getParameters(),
                            pair, "Parameter");
                        pair.getChildren().each { importChanges(it, sourceFunction) }
                        modelService.saveModelObject(sourceFunction)
                        break;
                    case ChangeType.DELETED:
                        parentObject.removeFunction(sourceFunction)
                        modelService.deleteModelObject(sourceFunction.getId())
                        break;
                    case ChangeType.COPIED:
                        throw new RuntimeException("Not implemented change type ${pair.getChangeType()}")
                    case ChangeType.EQUALS:
                        break;
                    default:
                        throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
                }
        } else if (objectType.equals("Column")) {
                Column   sourceColumn = (Column)pair.getSource()
                Column   targetColumn = (Column)pair.getTarget()
                switch (pair.getChangeType()) {
                    case ChangeType.NEW:
                        break;
                    case ChangeType.COPIED:
                    case ChangeType.CHANGED:
                        sourceColumn.setName(targetColumn.getName())
                        sourceColumn.setType(targetColumn.getType())
                        sourceColumn.setNullable(targetColumn.isNullable())
                        sourceColumn.setSize(targetColumn.getSize())
                        sourceColumn.setScale(targetColumn.getScale())
                        sourceColumn.setPrecesion(targetColumn.getPrecesion())
                        sourceColumn.setDefaultValue(targetColumn.getDefaultValue())
                        sourceColumn.setExtraDefinition(targetColumn.getExtraDefinition())
                        sourceColumn.setCustomData("Identity", targetColumn.getCustomData("Identity"));
                    case ChangeType.EQUALS:
                        break;
                    case ChangeType.DELETED:
                        break;                    
                    default:
                        throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
                }
        } else if (objectType.equals("Parameter")) {
                Parameter sourceParameter = (Parameter)pair.getSource();
                Parameter targetParameter = (Parameter)pair.getTarget();
                switch (pair.getChangeType()) {
                    case ChangeType.NEW:
                        break;
                    case ChangeType.COPIED:
                    case ChangeType.CHANGED:
                        sourceParameter.setName(targetParameter.getName());
                        sourceParameter.setType(targetParameter.getType());
                        sourceParameter.setNullable(targetParameter.isNullable());
                        sourceParameter.setSize(targetParameter.getSize());
                        sourceParameter.setScale(targetParameter.getScale());
                        sourceParameter.setPrecesion(targetParameter.getPrecesion());
                        sourceParameter.setDefaultValue(targetParameter.getDefaultValue());
                        sourceParameter.setExtraDefinition(targetParameter.getExtraDefinition());
                        sourceParameter.setParamType(targetParameter.getParamType());
                    case ChangeType.EQUALS:
                        break;
                    case ChangeType.DELETED:
                        break;
                    default:
                        throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
                }
        } else if (objectType.equals("Index")) {
                Index   sourceIndex = (Index)pair.getSource();
                Index   targetIndex = (Index)pair.getTarget();
                switch (pair.getChangeType()) {
                    case ChangeType.NEW:
                        parentObject.addIndex(targetIndex)
                        break;
                    case ChangeType.COPIED:
                    case ChangeType.CHANGED:
                        sourceIndex.setName(targetIndex.getName())
                        sourceIndex.setType(targetIndex.getType())

                        sourceIndex.setPrimaryKey(targetIndex.isPrimaryKey())
                        sourceIndex.setUnique(targetIndex.isUnique())
                        sourceIndex.setIgnoreDuplicates(targetIndex.isIgnoreDuplicates())
                        sourceIndex.setDisabled(targetIndex.isDisabled())
                        sourceIndex.setFillFactor(targetIndex.getFillFactor())
                        sourceIndex.setIndexSize(targetIndex.getIndexSize())
                        sourceIndex.setFragmentation(targetIndex.getFragmentation())
                        sourceIndex.setColumns(targetIndex.getColumns())
                        break;
                    case ChangeType.DELETED:
                        parentObject.removeIndex(sourceIndex)
                        // TODO - no method deleteIndex 
                        // modelService.deleteIndex(sourceColumn.getId())
                        break;
                    case ChangeType.EQUALS:
                        break;
                    default:
                        throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
                }
        } else if (objectType.equals("Constraint")) {
                Constraint sourceConstraint = (Constraint)pair.getSource();
                Constraint targetConstraint = (Constraint)pair.getTarget();
                switch (pair.getChangeType()) {
                    case ChangeType.NEW:
                        parentObject.addConstraint(targetConstraint)
                        break;
                    case ChangeType.COPIED:
                    case ChangeType.CHANGED:
                        sourceConstraint.setName(targetConstraint.getName());
                        sourceConstraint.setColumnName(targetConstraint.getColumnName())
                        sourceConstraint.setDefinition(targetConstraint.getDefinition())
                        sourceConstraint.setDisabled(targetConstraint.isDisabled())
                        break;
                    case ChangeType.DELETED:
                        parentObject.removeConstraint(sourceConstraint)
                        //?? modelService.deleteConstraint(sourceConstraint.getId())
                        break;
                    case ChangeType.EQUALS:
                        break;
                    default:
                        throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
                }
        } else if (objectType.equals("ForeignKey")) {
                ForeignKey sourceFK = (ForeignKey)pair.getSource();
                ForeignKey targetFK = (ForeignKey)pair.getTarget();
                switch (pair.getChangeType()) {
                    case ChangeType.NEW:
                        parentObject.addForeignKey(targetFK)
                        break;
                    case ChangeType.COPIED:
                    case ChangeType.CHANGED:
                        sourceFK.setName(targetFK.getName());
                        sourceFK.setDisabled(targetFK.getDisabled());

                        sourceFK.setDeleteAction(targetFK.getDeleteAction());
                        sourceFK.setUpdateAction(targetFK.getUpdateAction());
                        sourceFK.setTargetTable(targetFK.getTargetTable());
                        sourceFK.setColumns(targetFK.getColumns());
                        
                        break;
                    case ChangeType.DELETED:
                        parentObject.removeForeignKey(sourceFK)
                        break;
                    case ChangeType.EQUALS:
                        break;
                    default:
                        throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
                }
        } else {
            throw new SyncException("Unexpected object type "+ objectType);
        }
    }
}

def p_ignoreWhitespaces = false;
if (binding.variables["ignoreWhitespaces"]) {
    p_ignoreWhitespaces = ignoreWhitespaces;
}

modelService = dbm.getService(ModelService.class)
sync_session = new ModelSyncSession(dbm, p_ignoreWhitespaces)

sync_session.setParameter("customFieldMap", binding.variables.get("customFieldMap") == null
    ? Collections.emptyMap()
    : binding.variables.get("customFieldMap"))

if (source instanceof Model) {
    sync_session.sourceModel = source
}


sync_session.syncObjects(source, target)
sync_session.setParameter("title", "Model Synchronization")
sync_session.setParameter("longText", "Procedure.Source;View.Source;Function.Source;Trigger.Source")
// TODO (implement) sync_session.setParameter("exclude_objects", p_exclude_objects==null ? "" : p_exclude_objects.join(","))