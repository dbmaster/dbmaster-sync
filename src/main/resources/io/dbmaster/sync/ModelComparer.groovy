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
import com.branegy.dbmaster.sync.api.SyncSession.SearchTarget
import com.branegy.dbmaster.sync.api.SyncService
import com.branegy.dbmaster.model.*


public class ModelComparer extends BeanComparer {
    Model    source
    Model    target

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

            childPairs.addAll(mergeCollections(pair, sourceTable?.getColumns(), targetTable?.getColumns(), namer));
            childPairs.addAll(mergeCollections(pair, sourceTable?.getIndexes(), targetTable?.getIndexes(), namer));

            if (exludeObjects!=null && !exludeObjects.contains("Constraints")) {
                childPairs.addAll(mergeCollections(pair, sourceTable?.getConstraints(), targetTable?.getConstraints(), namer));
            }

            childPairs.addAll(mergeCollections(pair, sourceTable?.getForeignKeys(), targetTable?.getForeignKeys(), namer));
        } else if (objectType.equals("View")) {
            View sourceView = (View)pair.getSource();
            View targetView = (View)pair.getTarget();

            List<SyncPair> childPairs = pair.getChildren()
            childPairs.addAll(mergeCollections(pair, sourceView?.getColumns(),targetView?.getColumns(), namer));
            def attributes = pair.getAttributes()
            attributes.add(new SyncAttributePair("Source", sourceView?.getSource(),
                                                           targetView?.getSource()))
        } else if (objectType.equals("Function")) {
            Function sourceFunc = (Function)pair.getSource();
            Function targetFunc = (Function)pair.getTarget();

            List<SyncPair> childPairs = pair.getChildren()
            
            childPairs.addAll(mergeCollections(pair, sourceFunc?.getParameters(), 
                                                     targetFunc?.getParameters(), namer));
            def attributes = pair.getAttributes()
            attributes.add(new SyncAttributePair("Source", sourceFunc?.getSource(), 
                                                           targetFunc?.getSource()))
            attributes.add(new SyncAttributePair(Function.TYPE, sourceFunc?.getType(), 
                                                                targetFunc?.getType()))
            attributes.add(new SyncAttributePair(Function.EXTRA_INFO, sourceFunc?.getExtraInfo(), 
                                                                      targetFunc?.getExtraInfo()))
        } else if (objectType.equals("Procedure")) {
            Procedure sourceProc = (Procedure)pair.getSource();
            Procedure targetProc = (Procedure)pair.getTarget();

            List<SyncPair> childPairs = pair.getChildren()

            childPairs.addAll(mergeCollections(pair, sourceProc?.getParameters(),
                                                     targetProc?.getParameters(), namer));
            def attributes = pair.getAttributes()
            attributes.add(new SyncAttributePair("Source", sourceProc?.getSource(),
                                                           targetProc?.getSource()))
        } else if (objectType.equals("Column")) {
            Column sourceColumn = (Column)pair.getSource();
            Column targetColumn = (Column)pair.getTarget();

            def attributes = pair.getAttributes()
            attributes.add(new SyncAttributePair("Type", sourceColumn?.getPrettyType(),
                                                         targetColumn?.getPrettyType()))
            attributes.add(new SyncAttributePair("Nullable", sourceColumn?.isNullable(),
                                                             targetColumn?.isNullable()))
            attributes.add(new SyncAttributePair("Default Value", sourceColumn?.getDefaultValue(),
                                                                  targetColumn?.getDefaultValue()))
            attributes.add(new SyncAttributePair("Extra", sourceColumn?.getExtraDefinition(),
                                                          targetColumn?.getExtraDefinition()))
            attributes.add(new SyncAttributePair("Order Index", sourceColumn?.getCollectionIndex(),
                                                          targetColumn?.getCollectionIndex()))

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
            attributes.add(new SyncAttributePair("Index", sourceParameter?.getCollectionIndex(),
                                                          targetParameter?.getCollectionIndex()))
        } else if (objectType.equals("Index")) {
            Index sourceIndex = (Index)pair.getSource();
            Index targetIndex = (Index)pair.getTarget();

            def attributes = pair.getAttributes()
            def columnsAsText = { columns, included ->
                String result = null
                if (columns!=null) {
                    def filtered = columns.findAll { it.included == included }
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
        } else {
            throw new SyncException("Unexpected object type "+ objectType);
        }
    }
}
