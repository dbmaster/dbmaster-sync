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


public class ModelNamer implements Namer {
        @Override
        public String getName(Object o) {
            if (o instanceof Model) {                        return ((Model)o).getName();
            } else if (o instanceof Table) {                 return ((Table)o).getName();
            } else if (o instanceof View)  {                 return ((View)o).getName();
            } else if (o instanceof Procedure) {             return ((Procedure)o).getName();
            } else if (o instanceof Function)  {             return ((Function)o).getName();
            } else if (o instanceof Parameter) {             return ((Parameter)o).name;
            } else if (o instanceof Column) {                return ((Column)o).name;
            } else if (o instanceof Index) {                 return ((Index)o).getName();
            } else if (o instanceof Constraint) {            return ((Constraint)o).getName();
            } else if (o instanceof ForeignKey) {            return ((ForeignKey)o).getName();
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
            } else {
                throw new IllegalArgumentException("Unexpected object class "+o);
            }
        }
}
