import com.branegy.dbmaster.sync.api.*
import com.branegy.dbmaster.sync.impl.*
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.regex.Pattern
import java.util.concurrent.Future
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException

import org.slf4j.Logger

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
import com.branegy.inventory.model.Job;
import com.branegy.persistence.custom.BaseCustomEntity
import com.branegy.service.connection.api.ConnectionService;
import com.branegy.service.connection.model.DatabaseConnection;
import com.branegy.service.core.AbstractService;
import com.branegy.service.core.CheckedAccess;
import com.branegy.service.core.exception.EntityNotFoundApiException
import com.google.inject.persist.Transactional;
import com.branegy.scripting.DbMaster
import com.branegy.service.core.QueryRequest
import com.branegy.dbmaster.sync.api.SyncSession.SearchTarget
import com.branegy.dbmaster.sync.api.SyncService
import com.branegy.service.core.search.CustomCriterion
import com.branegy.service.core.search.CustomCriterion.Operator
import com.branegy.dbmaster.sync.api.SyncAttributePair.AttributeChangeType

class InventoryNamer implements Namer {
        @Override
        public String getName(Object o) {
            if (o instanceof RootObject) {                 return "Inventory";
            } else if (o instanceof DatabaseConnection) {  return ((DatabaseConnection)o).getName();
            } else if (o instanceof Database) {            return ((Database)o).getDatabaseName();
            } else if (o instanceof Job)      {            return ((Job)o).getJobName();
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
            } else if (o instanceof Job) {                 return "Job";
            } else if (o instanceof Database) {            return "Database";
            } else if (o instanceof DatabaseInfo) {        return "Database";
            } else if (o instanceof NamedObject) {         return ((NamedObject)o).type;
            } else {
                throw new IllegalArgumentException("Unexpected object class "+o);
            }
        }
}

class InventoryComparer extends BeanComparer {
    def inventoryDBs
    def inventoryJobs
    Logger logger;
    
    static class ConnectionResult{
        List<Database> databases;
        List<Job> jobs;
        boolean caseSensitive;
        
        public ConnectionResult(List<Database> databases, List<Job> jobs,  boolean caseSensitive) {
            this.databases = databases;
            this.jobs = jobs;
            this.caseSensitive = caseSensitive;
        }
    }
    
    Map<String,Future<ConnectionResult>> connectionResults;
    final ExecutorService executor = new ThreadPoolExecutor(20, Integer.MAX_VALUE,
                                      20L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>());
    
    public InventoryComparer(Logger logger){
        this.logger = logger;
    }
    
    @Override
    public void syncPair(SyncPair pair, SyncSession session) {
        this.logger = logger;
        String objectType = pair.getObjectType();
        Namer namer = session.getNamer();
        if (objectType.equals("Inventory")) {
            def connections = session.connectionSrv.getConnectionList().collectEntries{[it.name, it]}

            connections.entrySet().removeAll { it.getValue().getDriver() == "ldap" }

            inventoryDBs = session.inventorySrv
                            .getDatabaseList(new QueryRequest("Deleted=no"))
                            .groupBy { it.getServerName() } 

            inventoryJobs = session.inventorySrv
                            .getJobList(new QueryRequest("Deleted=no"))
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
            
            connectionResults = connections.values().collectEntries{[it.name,
                executor.submit(new Callable<ConnectionResult>(){
                    public ConnectionResult call()  throws Exception{
                        DatabaseConnection connection = it;
                        Connector connector = ConnectionProvider.getConnector(connection);
                        def dialect = null;
                        try {
                            dialect = connector.connect();
                            def databases = dialect.getDatabases();
                            def jobs = dialect.getJobs();
                            return new ConnectionResult(databases, jobs, dialect.isCaseSensitive());
                        } finally {
                            try {
                                dialect?.close();
                            } catch (Exception e) {
                                logger.debug("Connection close",e);
                            }
                        }
                    }
                })    
            ]};
            pair.getChildren().addAll(mergeCollections(pair, invConnections, connections.values(), namer));
        } else if (objectType.equals("Server")) {
            DatabaseConnection sourceServer = (DatabaseConnection)pair.getSource();
            DatabaseConnection targetServer = (DatabaseConnection)pair.getTarget();
            Dialect dialect = null
            List<SyncPair> childPairs = pair.getChildren()
            def sourceDatabases = inventoryDBs.get(sourceServer.getName())
            def targetDatabases = null;

            def sourceJobs = inventoryJobs.get(sourceServer.getName())
            def targetJobs = null

            if (targetServer!=null) {
                try {
                    ConnectionResult result = connectionResults.get(targetServer.getName()).get();
                    targetDatabases = new ArrayList(result.databases);
                    pair.setCaseSensitive(result.isCaseSensitive())

                    targetJobs = new ArrayList(result.jobs)
                    targetJobs.each{ 
                        it.setServerName(targetServer.getName()) 
                        it.setCustomData(Database.DELETED, false)
        		    }
                    if (targetServer.getCustomData(DatabaseConnection.SYNC_EXCLUDE_JOBS)!=null) {
                        try {
                            def patterns = targetServer.getCustomData(DatabaseConnection.SYNC_EXCLUDE_JOBS).split(",").collect{
                                Pattern.compile(it.trim())
                            };
                            targetJobs = targetJobs.findAll{
                                def jobName = it.getJobName();
                                def enabled = !patterns.any{
                                    it.matcher(jobName)
                                }
                                if (!enabled) {
                                    session.logger.debug("Job {} is ignored",it.getJobName());
                                }
                                return enabled;
                            };
                        } catch (Exception e) {
                            session.logger.error("Invalid SYNC_EXCLUDE_JOBS regexp {} for connection {}",
                                targetServer.getCustomData(DatabaseConnection.SYNC_EXCLUDE_JOBS),
                                targetServer.getName(),
                                e);
                            logger.warn("Invalid SYNC_EXCLUDE_JOBS regexp {} for connection {}", 
                                targetServer.getCustomData(DatabaseConnection.SYNC_EXCLUDE_JOBS),
                                targetServer.getName());
                        }
                    }
                } catch (ExecutionException e) {
                    // assumption: this exception is related to connectivity
                    pair.setCaseSensitive(true); // make safe megreCollection for equals databases
                    session.logger.error(e.getCause().getMessage())
                    targetDatabases = sourceDatabases;
                    
                    /*targetDatabases = sourceDatabases.collect { db ->  
                        def dbInfo = new DatabaseInfo(db.getDatabaseName(), "Not Accessible", false)
                        dbInfo.setCustomData("State", "Not Accessible")
                        return dbInfo
                    }*/
                    logger.warn("Can not load databases", e.getCause());
                } finally {
                    if (dialect!=null){
                        dialect.close();
                    }
                }
                childPairs.addAll(mergeCollections(pair, sourceDatabases, targetDatabases, namer))
                childPairs.addAll(mergeCollections(pair, sourceJobs, targetJobs, namer))
            }
            
// TODO: Add users
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
        } else if (objectType.equals("Job")) {
            Job source = (Job)pair.getSource()
            Job target = (Job)pair.getTarget()
            List<SyncAttributePair> attrs = mergeAttributes(source?.getCustomMap(), target?.getCustomMap())
            pair.getAttributes().addAll(attrs)
            if (pair.getChangeType()==ChangeType.EQUALS) { 
                for (SyncAttributePair attr : attrs) {
                    if (attr.getChangeType()!=AttributeChangeType.EQUALS) {
                        pair.setChangeType(ChangeType.CHANGED)
                    }
                }
            }
        } else if (objectType.equals("USER")) {
            NamedObject source = (NamedObject)pair.getSource();
            NamedObject target = (NamedObject)pair.getTarget();
            List<SyncAttributePair> attrs = mergeAttributes(source.getCustomMap(), target.getCustomMap());
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
    Map<String, Database> connectionDbMap;
    
    public InventorySyncSession(DbMaster dbm, Logger logger) {
        super(new InventoryComparer(logger));
        setNamer(new InventoryNamer());
        this.dbm = dbm
        inventorySrv = dbm.getService(InventoryService.class)
        connectionSrv = dbm.getService(ConnectionService.class)
    }

    public void applyChanges() {
        try {        
            connectionDbMap = [:]
            QueryRequest q;
            // TODO: fix delimeter char %
            q = new QueryRequest();
            q.getCriteria().add(new CustomCriterion("\"Deleted\"", Operator.EQ, "no"));
            inventorySrv.getDatabaseList(q).each{ db ->
                connectionDbMap.put( db.getConnectionName()+"%"+db.getDatabaseName(), db);
            }
            q = new QueryRequest();
            q.getCriteria().add(new CustomCriterion("\"Deleted\"", Operator.EQ, "yes"));
            inventorySrv.getDatabaseList(q).each{ db ->
                connectionDbMap.put( db.getConnectionName()+"%"+db.getDatabaseName(), db);
            }
            
            importChanges(getSyncResult());
            SyncService syncService = dbm.getService    (SyncService.class);
            syncService.saveSession(this, "Database Import");
        } finally {
            dbm.closeResources()
        } 
    }
    
    private void setLastSyncDate(SyncPair pair) {
        if (pair.getTarget() instanceof BaseCustomEntity) {
            pair.getTarget().setCustomData("Last Sync Date", lastSyncDate);
        }
        for (SyncPair child:pair.getChildren()) {
            setLastSyncDate(child);
        }
    }

    public void importChanges(SyncPair pair) {
        String objectType = pair.getObjectType();
        if (objectType.equals("Inventory")) {
            setLastSyncDate(pair)
            pair.getChildren().each {
                importChanges(it)
            }
        } else if (objectType.equals("Server")) {
            pair.getChildren().each { importChanges(it) }
        } else if (objectType.equals("Database")) {
            Database     sourceDB = (Database)pair.getSource();
            DatabaseInfo targetDB = (DatabaseInfo)pair.getTarget();
            
            switch (pair.getChangeType()) {
                case ChangeType.NEW:
                    String serverName = pair.getParentPair().getSourceName()
                    def db = connectionDbMap.get(serverName+"%"+targetDB.getName());
                    if (db == null){
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
        } else if (objectType.equals("Job")) {
            Job sourceJob = (Job)pair.getSource();
            Job targetJob = (Job)pair.getTarget();
            
            switch (pair.getChangeType()) {
                case ChangeType.NEW:
                    String serverName = pair.getParentPair().getSourceName()
                    //def db = connectionDbMap.get(serverName+"%"+targetDB.getName());
                    //if (db == null) {
                    //    db = new Database()
                    //    db.setDatabaseName(targetDB.getName())
                    targetJob.setServerName(serverName)
                    targetJob.setCustomData(Database.DELETED, false);
                    //}
                    // TODO(Restore operation)db.setCustomData(Database.DELETED, false);
                    //for (SyncAttributePair attr : pair.getAttributes()) {
                    //    if (attr.getChangeType() != SyncAttributePair.AttributeChangeType.EQUALS) {
                    //        db.setCustomData( attr.getAttributeName(), attr.getTargetValue()  )
                    //    }
                    //}
                    //if (sourceJob.isPersisted()) {
                    //    inventorySrv.updateJob(sourceJob)
                    //} else {
                        inventorySrv.createJob(targetJob)
                    //}
                    break;
                case ChangeType.CHANGED:
                    for (SyncAttributePair attr : pair.getAttributes()) {
                        if (attr.getChangeType() != SyncAttributePair.AttributeChangeType.EQUALS) {
                            sourceJob.setCustomData( attr.getAttributeName(), attr.getTargetValue()  )
                        }
                    }
                    sourceJob.setCustomData(Database.DELETED, false);
                    inventorySrv.saveJob(sourceJob);
                    break;
                case ChangeType.DELETED:
                    inventorySrv.deleteJob(sourceJob.getId());
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

sync_session = new InventorySyncSession(dbm, logger)

def inventory = new RootObject("Inventory", "Inventory")
sync_session.syncObjects(inventory, inventory)
sync_session.setParameter("title", "Database Synchronization")