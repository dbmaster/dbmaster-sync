/* 
 *  File Version:  $Id: database-importer.groovy 2249 2013-10-28 04:33:20Z schristin $
 */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.branegy.dbmaster.model.NamedObject;
import com.branegy.dbmaster.sync.api.*
import com.branegy.dbmaster.sync.impl.*
import com.branegy.dbmaster.sync.api.SyncPair.ChangeType;
import com.branegy.dbmaster.connection.ConnectionProvider;
import com.branegy.dbmaster.connection.Connector;
import com.branegy.dbmaster.connection.Dialect;
import com.branegy.dbmaster.core.Permission.Role;
import com.branegy.dbmaster.core.Project;
import com.branegy.dbmaster.model.DatabaseInfo;
import com.branegy.inventory.api.InventoryService;
import com.branegy.inventory.model.Database;
import com.branegy.service.connection.api.ConnectionService;
import com.branegy.service.connection.model.DatabaseConnection;
import com.branegy.service.core.AbstractService;
import com.branegy.service.core.CheckedAccess;
import com.branegy.service.core.exception.EntityNotFoundApiException
import com.google.inject.persist.Transactional;
import com.branegy.scripting.DbMaster
import com.branegy.service.core.QueryRequest
import com.branegy.dbmaster.sync.api.SyncSession.SearchTarget
import com.branegy.dbmaster.sync.api.SyncService;

class InventoryNamer implements Namer {
        @Override
        public String getName(Object o) {
            if (o instanceof RootObject) {                 return "Inventory";
            } else if (o instanceof DatabaseConnection) {  return ((DatabaseConnection)o).getName();
            } else if (o instanceof Database) {            return ((Database)o).getDatabaseName();
            } else if (o instanceof DatabaseInfo) {        return ((DatabaseInfo)o).getName();
            } else if (o instanceof NamedObject) {         return ((NamedObject)o).name;
            } else {
                throw new IllegalArgumentException("Unexpected object class "+o);
            }
        }

        @Override
        public String getType(Object o) {
            if (o instanceof RootObject) {                 return "Inventory";
            } else if (o instanceof DatabaseConnection) {  return "Server";
            } else if (o instanceof Database) {            return "Database";
            } else if (o instanceof DatabaseInfo) {        return "Database";
            } else if (o instanceof NamedObject) {         return ((NamedObject)o).type;
            } else {
                throw new IllegalArgumentException("Unexpected object class "+o);
            }
        }
}

class InventoryComparer extends BeanComparer {
    def connections
    def inventoryDBs
    
    @Override
    public void syncPair(SyncPair pair, SyncSession session) {
        String objectType = pair.getObjectType();
        Namer namer = session.getNamer();
        if (objectType.equals("Inventory")) {
            connections = session.connectionSrv.getConnectionList().collectEntries{[it.name, it]}

            connections.entrySet().removeAll { it.getValue().getDriver() == "ldap" }

            inventoryDBs = session.inventorySrv
                            .getDatabaseList(new QueryRequest("Deleted=no"))
                            .groupBy { it.getServerName() } 
            
            def invConnections = []
            def invConnectionNames = []
            invConnectionNames.addAll(inventoryDBs.keySet())
            invConnectionNames.addAll(connections.keySet())
            invConnectionNames.unique().each { serverName->
                def conn = connections[serverName]
                if (conn==null) {  
                    conn = new DatabaseConnection()
                    conn.setName(serverName)
                }
                invConnections.add(conn)                 
            }
            pair.getChildren().addAll(mergeCollections(pair, invConnections, connections.values(), namer));
        } else if (objectType.equals("Server")) {
            DatabaseConnection sourceServer = (DatabaseConnection)pair.getSource();
            DatabaseConnection targetServer = (DatabaseConnection)pair.getTarget();
            Dialect dialect = null
            List<SyncPair> childPairs = pair.getChildren()
            def sourceDatabases = inventoryDBs.get(sourceServer.getName())
            try {
                def targetDatabases = null;
                if (targetServer!=null) {
                    Connector connector = ConnectionProvider.getConnector(targetServer)
                    dialect = connector.connect()
                    targetDatabases = dialect.getDatabases()
                }
                childPairs.addAll(mergeCollections(pair, sourceDatabases,targetDatabases, namer));
            } catch (Exception e) {
                // assumption: this exception is related to connectivity
                session.logMessage("error", e.getMessage());
                def targetDatabases = sourceDatabases.collect { db ->  
                    def dbInfo = new DatabaseInfo(db.getDatabaseName(), "Not Accessible", false)
	            dbInfo.setCustomData("State", "Not Accessible")
                    return dbInfo
                }
                childPairs.addAll(mergeCollections(pair, sourceDatabases, targetDatabases, namer));
                e.printStackTrace()
            } finally{
                if (dialect!=null){
                    dialect.close();
                }
            }
//            Collection<NamedObject> sJobs = source.extraCollections.get("jobs");
//            Collection<NamedObject> tJobs = target.extraCollections.get("jobs");
//            children.addAll(mergeCollections(sJobs, tJobs, namer));
//
//            Collection<NamedObject> sUsers = source.extraCollections.get("users");
//            Collection<NamedObject> tUsers = target.extraCollections.get("users");
//            children.addAll(mergeCollections(sUsers, tUsers, namer));
        } else if (objectType.equals("Database")) {
            Database sourceDB = (Database)pair.getSource();
            DatabaseInfo targetDB = (DatabaseInfo)pair.getTarget();
            if (targetDB!=null) {
                targetDB.getCustomMap().each { key, targetValue ->
                        def sourceValue = sourceDB==null ? null : sourceDB.getCustomData(key)
                        pair.getAttributes().add(new SyncAttributePair(key, sourceValue, targetValue))
                }
            }
        } else if (objectType.equals("JOB")) {
            // all ok - nothing to compare for know - later will add attributes
        } else if (objectType.equals("USER")) {
            NamedObject source = (NamedObject)pair.getSource();
            NamedObject target = (NamedObject)pair.getTarget();
            List<SyncAttributePair> attrs = mergeAttributes(source.data, target.data);
            pair.getAttributes().addAll(attrs);
            for (SyncAttributePair attr : attrs) {
                if (attr.getChangeType()!=AttributeChangeType.EQUALS) {
                    pair.setChangeType(ChangeType.CHANGED);
                }
            }
        } else {
            throw new SyncException("Unexpected object type "+ objectType);
        }
    }
}

class InventorySyncSession extends SyncSession {
    DbMaster dbm
    InventoryService inventorySrv
    ConnectionService connectionSrv

    public InventorySyncSession(DbMaster dbm) {
        super(new InventoryComparer());
        setNamer(new InventoryNamer());
        this.dbm = dbm
        inventorySrv = dbm.getService(InventoryService.class)
        connectionSrv = dbm.getService(ConnectionService.class)
    }

    public void applyChanges() {
        try {        
            importChanges(getSyncResult());
            SyncService syncService = dbm.getService    (SyncService.class);
            syncService.saveSession(this, "Database Import");
        } finally {
            dbm.closeResources()
        } 
    }

    public void importChanges(SyncPair pair) {
        String objectType = pair.getObjectType();
        if (objectType ==~ /Inventory|Server/) {
            pair.getChildren().each { importChanges(it) }
        } else if (objectType.equals("Database")) {
            Database     sourceDB = (Database)pair.getSource();
            DatabaseInfo targetDB = (DatabaseInfo)pair.getTarget();
            
            switch (pair.getChangeType()) {
                case ChangeType.NEW:
                    String serverName = pair.getParentPair().getSourceName()
                    def db = null
                    try {
                        db = inventorySrv.findDatabaseByServerNameDbName(serverName, targetDB.getName())                        
                    } catch (EntityNotFoundApiException e) {
                        db = new Database()
                        db.setDatabaseName(targetDB.getName())
                        db.setServerName(serverName)
                    }
                    db.setCustomData(Database.DELETED, false);
                    for (SyncAttributePair attr : pair.getAttributes()) {
                        if (attr.getChangeType() != SyncAttributePair.AttributeChangeType.EQUALS) {
                            db.setCustomData( attr.getAttributeName(), attr.getTargetValue()  )
                        }
                    }
                    if (db.isPersisted()) {
                        inventorySrv.updateDatabase(db)
                    } else {
                        inventorySrv.createDatabase(db)
                    }
                    break;
                case ChangeType.CHANGED:
                    for (SyncAttributePair attr : pair.getAttributes()) {
                        if (attr.getChangeType() != SyncAttributePair.AttributeChangeType.EQUALS) {
                            sourceDB.setCustomData( attr.getAttributeName(), attr.getTargetValue()  )
                        }
                    }
                    inventorySrv.updateDatabase(sourceDB);
                    break;
                case ChangeType.DELETED:
                    inventorySrv.deleteDatabase(sourceDB.getId());
                    break;                    
                case ChangeType.COPIED:
                    throw new RuntimeException("Not implemented change type ${pair.getChangeType()}")
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

sync_session = new InventorySyncSession(dbm)

def inventory = new RootObject("Inventory", "Inventory")
sync_session.syncObjects(inventory, inventory)
sync_session.setParameter("title", "Database Synchronization")