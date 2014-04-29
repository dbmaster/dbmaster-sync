/*
 *  File Version:  $Id: ModelSyncSession.groovy 119 2013-04-25 16:49:28Z schristin $
 */
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

class ModelSyncSession extends SyncSession {
    DbMaster dbm
    Model sourceModel
    // modelService used in applyChanges
    ModelService modelService

    public ModelSyncSession(DbMaster dbm) {
        super(new ModelComparer())
        setNamer(new ModelNamer())
        this.dbm = dbm
    }

    public void applyChanges() {
        try {
            this.modelService = dbm.getService(ModelService.class)

            importChanges(getSyncResult(), sourceModel)
            SyncService syncService = dbm.getService(SyncService.class)
            syncService.saveSession(this, "Model Import")

            modelService.saveModel(sourceModel, null)
        } finally {
            dbm.closeResources()
        }
    }

    public void importChanges(SyncPair pair, DatabaseObject parentObject) {
        String objectType = pair.getObjectType();
        if (objectType ==~ /Model/) {
            pair.getChildren().each { importChanges(it, sourceModel) }
        } else if (objectType.equals("Table")) {
            Table   sourceTable = (Table)pair.getSource();
            Table   targetTable = (Table)pair.getTarget();

            switch (pair.getChangeType()) {
                case ChangeType.NEW:
                    sourceModel.addTable(targetTable)
                    targetTable.setModel(sourceModel)
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
                        targetView.setModel(sourceModel)

                        if (targetView.isPersisted()) {
                            modelService.saveModelObject(targetView)
                        } else {
                            modelService.createModelObject(targetView)
                        }
                        break;
                    case ChangeType.CHANGED:
                        sourceView.setSource( targetView.getSource() )
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
                        targetProcedure.setModel(sourceModel)

                        if (targetProcedure.isPersisted()) {
                            modelService.saveModelObject(targetProcedure)
                        } else {
                            modelService.createModelObject(targetProcedure)
                        }
                        break;
                    case ChangeType.CHANGED:
                        sourceProcedure.setSource( targetProcedure.getSource() )
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
                        targetFunction.setModel(sourceModel)
                    
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
                        parentObject.addColumn(targetColumn)
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
                        modelService.saveColumn(sourceColumn, null)
                        break;
                    case ChangeType.DELETED:
                        parentObject.removeColumn(sourceColumn)
                        modelService.deleteColumn(sourceColumn.getId())
                        break;
                    case ChangeType.EQUALS:
                        break;
                    default:
                        throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
                }
        } else if (objectType.equals("Parameter")) {
                Parameter sourceParameter = (Parameter)pair.getSource();
                Parameter targetParameter = (Parameter)pair.getTarget();
                switch (pair.getChangeType()) {
                    case ChangeType.NEW:
                        parentObject.addParameter(targetParameter)
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
                        break;
                    case ChangeType.DELETED:
                        parentObject.removeParameter(sourceParameter)
                        modelService.deleteParameter(sourceParameter.getId())
                        break;
                    case ChangeType.EQUALS:
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
                        //modelService.deleteConstraint(sourceConstraint.getId())
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
