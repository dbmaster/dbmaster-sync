import com.branegy.dbmaster.sync.api.*
import com.branegy.dbmaster.sync.impl.*
import java.util.ArrayList
import java.util.Iterator
import java.util.List
import java.util.Map.Entry
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.regex.Pattern
import java.util.concurrent.Future
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.sql.*
import org.slf4j.Logger

import com.branegy.dbmaster.model.NamedObject
import com.branegy.dbmaster.sync.api.*
import com.branegy.dbmaster.sync.impl.*
import com.branegy.dbmaster.sync.api.SyncPair.ChangeType
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.JdbcDialect
import com.branegy.dbmaster.core.Permission.Role
import com.branegy.dbmaster.core.Project
import com.branegy.dbmaster.model.DatabaseInfo
import com.branegy.inventory.api.InventoryService
import com.branegy.inventory.model.Database
import com.branegy.inventory.model.Job
import com.branegy.inventory.model.SecurityObject
import com.branegy.persistence.custom.BaseCustomEntity
import com.branegy.service.connection.api.ConnectionService
import com.branegy.service.connection.model.DatabaseConnection
import com.branegy.service.core.AbstractService
import com.branegy.service.core.CheckedAccess
import com.branegy.service.core.exception.EntityNotFoundApiException
import com.google.inject.persist.Transactional
import com.branegy.scripting.DbMaster
import com.branegy.service.core.QueryRequest
import com.branegy.dbmaster.sync.api.SyncSession.SearchTarget
import com.branegy.dbmaster.sync.api.SyncService
import com.branegy.service.core.search.CustomCriterion
import com.branegy.service.core.search.CustomCriterion.Operator

import groovy.util.slurpersupport.NodeChild

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
        } else if (o instanceof NodeChild) {       
            NodeChild nc = (NodeChild)o;
            String type = nc.name();
            if (type == "Step") {
                return /*nc.step_id + ". "+ */nc.step_name;
            } else if (type == "Schedule") {
                return nc.name;
            } else if (type == "File") {
                return nc.name;
            } else if (type == "ServerPrincipal") {
                return nc.name;
            }
            throw new IllegalArgumentException("Unexpected object NodeChild "+type);
        } else if (o instanceof SecurityObject) {      return ((SecurityObject)o).getCustomData("Name");
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
        } else if (o instanceof SecurityObject) {      return "ServerPrincipal";  // TODO Check if database is empty
        } else if (o instanceof NamedObject) {         return ((NamedObject)o).type;
        } else if (o instanceof NodeChild) {           return ((NodeChild)o).name();
        } else {
            throw new IllegalArgumentException("Unexpected object class "+o);
        }
    }
}

class ConnectionResult {
    List<Database> databases
    List<Job> jobs
    boolean caseSensitive
    String serverInfo
    
    public ConnectionResult(List<Database> databases, List<Job> jobs,  boolean caseSensitive, String serverInfo) {
        this.databases = databases
        this.jobs = jobs
        this.caseSensitive = caseSensitive
        this.serverInfo = serverInfo
    }
}

class InventoryComparer extends BeanComparer {
    Map<String,Database> inventoryDBs
    Map<String,Job> inventoryJobs
    Map<String,SecurityObject> inventoryServerPrincipals
    Logger logger
    
    Map<String,Future<Object>> connectionResults;
    final ExecutorService executor = new ThreadPoolExecutor(20, Integer.MAX_VALUE,
                                      20L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>());
    
    public InventoryComparer(Logger logger) {
        this.logger = logger
    }
    
    @Override
    public void syncPair(SyncPair pair, SyncSession session) {
        this.logger = logger
        String objectType = pair.getObjectType()
        Namer namer = session.getNamer()
        if (objectType.equals("Inventory")) {
            boolean selectedOnly = session.connections != null;
            Set<String> disabledConnections = [] as Set;
            Map<String,DatabaseConnection> connections = (selectedOnly
                    ?session.connections
                    :session.connectionSrv.getConnectionSlice(new QueryRequest()))
                            .findAll{ 
                                if (it.disabled) {
                                    disabledConnections << it.name;
                                    return false;
                                }
                                return true;
                            }
                            .collectEntries{[it.name, it]};
                            
            connections.entrySet().removeAll { it.getValue().getDriver() == "ldap" }

            inventoryDBs = session.inventorySrv
                            .getDatabaseList(new QueryRequest("Deleted=no"))
                            .findAll{!disabledConnections.contains(it.connectionName)}
                            .groupBy { it.getServerName() } 

            inventoryJobs = session.inventorySrv
                            .getJobList(new QueryRequest("Deleted=no"))
                            .findAll{!disabledConnections.contains(it.serverName)}
                            .groupBy { it.getServerName() }
           
            inventoryServerPrincipals = session.inventorySrv
                            .getSecurityObjectList(new QueryRequest("Source=SqlServer && DatabaseName="))
                            .groupBy { it.getServerName() }
           
            def invConnections = []
            def invConnectionNames = [] as Set;
            invConnectionNames.addAll(inventoryDBs.keySet())
            invConnectionNames.addAll(connections.keySet())
            invConnectionNames.each { serverName->
                if (!selectedOnly || connections.containsKey(serverName)) {
                    def conn = null; // connections[serverName]
                    if (conn==null) {
                        conn = new DatabaseConnection()
                        conn.setName(serverName)
                    }
                    invConnections.add(conn)
                }
            }
            
            connectionResults = connections.values().collectEntries{[it.name,
                executor.submit(new Callable<Object>() {
                    public Object call()  throws Exception {
                        DatabaseConnection connection = it
                        String name = it.name
                        JdbcDialect dialect = null;
                        try {
                            dialect = ConnectionProvider.get().getDialect(connection)
                            def databases = dialect.getDatabases()
                            def jobs = dialect.getJobs()
                            def serverInfo = getServerInfo(dialect)
                            return new ConnectionResult(databases, jobs, dialect.isCaseSensitive(), serverInfo)
                        } catch (Exception e) {
                            logger.error("Cannot log sql server info {}", e);
                            String errorMsg = e.getMessage()
                            session.logger.error(errorMsg)
                            return e
                        } finally {
                            dialect?.close();
                        }
                    }
                })    
            ]}
            pair.getChildren().addAll(mergeCollections(pair, invConnections, connections.values(), namer))
        } else if (objectType.equals("Server")) {
            DatabaseConnection sourceServer = (DatabaseConnection)pair.getSource()
            DatabaseConnection targetServer = (DatabaseConnection)pair.getTarget()
            List<SyncPair> childPairs = pair.getChildren()
            def sourceDatabases = inventoryDBs.get(sourceServer.getName())
            def targetDatabases = null

            def sourceJobs = inventoryJobs.get(sourceServer.getName())
            def targetJobs = null

            if (targetServer!=null) {
                try {
                    def result = connectionResults.get(targetServer.getName()).get()
                    if (result instanceof Exception) {
                        pair.setError(result.getMessage())
                    } else {
                        targetDatabases = new ArrayList(result.databases)
                        pair.setCaseSensitive(result.isCaseSensitive())
                        def slurper = new XmlSlurper()

                        def remoteServerInfo = slurper.parseText(result.serverInfo)
                        sourceServer.setCustomData("ServerInfo", result.serverInfo)

                        def serverInfo = targetServer.getCustomData("ServerInfo")
                        if (serverInfo == null) {  
                            serverInfo = "<ServerInfo></ServerInfo>"
                        }
                        def targetServerInfo = new XmlSlurper().parseText(serverInfo)

                        def remoteAttrs = new HashMap<String, Object>(100)
                        remoteServerInfo.Configuration.Config.each { config -> remoteAttrs.put("Config."+config.name.text(), config.value.text()) }
                        remoteServerInfo.Properties.Properties.children().each { p ->  remoteAttrs.put("Property."+ p.name(), p.text()) }
                        remoteServerInfo.SysInfo.SysInfo.children().each { p ->  remoteAttrs.put("SysInfo."+ p.name(), p.text()) }
                        remoteAttrs.put("Version", remoteServerInfo.Version.text())

                        def localAttrs = new HashMap<String, Object>(100)
                        targetServerInfo.Configuration.Config.each { config -> localAttrs.put("Config."+config.name.text(), config.value.text()) }
                        targetServerInfo.Properties.Properties.children().each { p -> localAttrs.put("Property."+ p.name(), p.text()) }
                        targetServerInfo.SysInfo.SysInfo.children().each { p ->  localAttrs.put("SysInfo."+ p.name(), p.text()) }
                        localAttrs.put("Version", targetServerInfo.Version.text())

                        def sourcePrincipals = inventoryServerPrincipals.get(sourceServer.getName())
                        def targetPrincipals = remoteServerInfo.ServerPrincipals.children().list(); 

                        pair.getChildren().addAll(mergeCollections(pair, sourcePrincipals, targetPrincipals, namer))

                        List<SyncAttributePair> attrs = mergeAttributes(localAttrs, remoteAttrs, true)
                        pair.getAttributes().addAll(attrs)

                        targetJobs = new ArrayList(result.jobs)
                        targetJobs.each { 
                            it.setServerName(targetServer.getName()) 
                            it.setCustomData(Database.DELETED, false)
                        }
                        if (targetServer.getCustomData(DatabaseConnection.SYNC_EXCLUDE_JOBS)!=null) {
                            try {
                                def patterns = targetServer.getCustomData(DatabaseConnection.SYNC_EXCLUDE_JOBS).split(",").collect{
                                    Pattern.compile(it.trim())
                                }
                                targetJobs = targetJobs.findAll {
                                    def jobName = it.getJobName()
                                    def enabled = !patterns.any {  it.matcher(jobName) }
                                    if (!enabled) {
                                        session.logger.debug("Job {} is ignored",it.getJobName())
                                    }
                                    return enabled
                                }
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
                            childPairs.addAll(mergeCollections(pair, sourceDatabases, targetDatabases, namer))
                            childPairs.addAll(mergeCollections(pair, sourceJobs, targetJobs, namer))
                        }
                    } catch (ExecutionException e) {
                        // assumption: this exception is related to connectivity
                        //pair.setCaseSensitive(true) // make safe megreCollection for equals databases
                        //targetDatabases = new ArrayList()
                        //sourceDatabases = new ArrayList()
                        //sourceJobs = new ArrayList()
                        //targetJobs = new ArrayList()
                        logger.warn("Can not get sql server info", e.getCause())
                    }
                }           
        } else if (objectType.equals("Database")) {
            Database sourceDB = (Database)pair.getSource();
            DatabaseInfo targetDB = (DatabaseInfo)pair.getTarget();
           
            def slurper = new XmlSlurper()
            
            def sourceAttrs = new HashMap<String, Object>(100)
            def sourceFiles = new ArrayList(100);
            if (sourceDB!=null) {
                String sourceDbDef = sourceDB.getCustomData("DatabaseDefinition");
                if (sourceDbDef!=null) {
                    def sourceDbInfo = slurper.parseText(sourceDbDef)
                    sourceDbInfo.Properties.children().each { config ->
                        sourceAttrs.put("Property." + config.name(), config.text());
                    }
                    sourceFiles = sourceDbInfo.Files.children().list();
                }
            }
            def targetAttrs = new HashMap<String, Object>(100)
            def targetFiles = new ArrayList(100);
            if (targetDB!=null) {
                String targetDbDef = targetDB.getCustomData("DatabaseDefinition");
                if (targetDbDef!=null) {
                    def targetDbInfo = slurper.parseText(targetDbDef)
                    targetDbInfo.Properties.children().each { config ->
                        targetAttrs.put("Property." + config.name(), config.text());
                    }
                    targetFiles = targetDbInfo.Files.children().list();
                }
            }
            pair.getAttributes().addAll(mergeAttributes(sourceAttrs, targetAttrs, true));
            pair.getChildren().addAll(mergeCollections(pair, sourceFiles, targetFiles, namer))
            
            if (targetDB!=null) {
                targetDB.getCustomMap().each { key, targetValue ->
                    if (!["Create Date","DatabaseDefinition"].contains(key)) {
                        pair.getAttributes().add(new SyncAttributePair(key, sourceDB?.getCustomData(key), targetValue))
                    }
                }
            }
        } else if (objectType.equals("Job")) {
            Job source = (Job)pair.getSource()
            Job target = (Job)pair.getTarget()
            def slurper = new XmlSlurper()

            def localAttrs = Collections.emptyMap()
            def localSteps = Collections.emptyList()
            def localSchedules = Collections.emptyList()
            if (source!=null) {
                String jobDefinision = source.getCustomData("JobDefinition");
                if (jobDefinision!=null) {
                    def localDef = slurper.parseText(jobDefinision)
                    localAttrs  = new HashMap<String, Object>(5)
                    localDef.children().each { attribute ->
                        if (["Steps","Schedules","date_modified","date_created"].contains(attribute.name())) {
                            return;
                        }
                        localAttrs.put(attribute.name(),attribute.text())
                    }
                    localSteps = localDef.Steps.children().list();
                    localSchedules = localDef.Schedules.children().list();
                }
            }
            def remoteAttrs = Collections.emptyMap()
            def remoteSteps = Collections.emptyList()
            def remoteSchedules = Collections.emptyList()
            if (target != null) {
                String jobDefinision = target.getCustomData("JobDefinition");
                if (jobDefinision!=null) {
                    def remoteDef = slurper.parseText(jobDefinision)
                    remoteAttrs  = new HashMap<String, Object>(5)
                    remoteDef.children().each { attribute ->
                        if (["Steps","Schedules","date_modified","date_created"].contains(attribute.name())) {
                            return;
                        }
                        remoteAttrs.put(attribute.name(),attribute.text())
                    }
                    remoteSteps = remoteDef.Steps.children().list();
                    remoteSchedules = remoteDef.Schedules.children().list();
                }
            }
            
            pair.getAttributes().addAll(mergeAttributes(localAttrs, remoteAttrs, true))
            pair.getChildren().addAll(mergeLists(pair, localSteps, remoteSteps, namer))
            pair.getChildren().addAll(mergeCollections(pair, localSchedules, remoteSchedules, namer))
            
            if (target!=null) {
                target.getCustomMap().each { key, targetValue ->
                    if (["Description", "Category", "Owner", "Enabled"].contains(key)) {
                        def sourceValue = source==null ? null : source.getCustomData(key)
                        pair.getAttributes().add(new SyncAttributePair(key, sourceValue, targetValue))
                    }
                }
            }
        } else if (objectType.equals("ServerPrincipal")) {
            SecurityObject source = (SecurityObject)pair.getSource();
            NodeChild      target = (NodeChild)pair.getTarget();

            pair.getAttributes().add(new SyncAttributePair(SecurityObject.TYPE, source?.getCustomData(SecurityObject.TYPE), target?.type_desc?.text()))
            pair.getAttributes().add(new SyncAttributePair("Roles", source?.getCustomData("Roles") ?: "", target?.ServerRoles?.text()))
            pair.getAttributes().add(new SyncAttributePair(SecurityObject.ENABLED, source?.getCustomData(SecurityObject.ENABLED), target?.enabled?.text()))
            pair.getAttributes().add(new SyncAttributePair(SecurityObject.ID, source?.getCustomData(SecurityObject.ID), target?.name?.text()))

            // Map<String,String> sourceAttrs = new LinkedHashMap<>();
            // source?.children().each{p -> if (!p.name().equals("ServerPermissions")) sourceAttrs.put(p.name(), p.text())};
            
            // Map<String,String> targetAttrs = new LinkedHashMap<>();
            // target?.children().each{p -> if (!p.name().equals("ServerPermissions")) targetAttrs.put(p.name(), p.text())};            
            // pair.getAttributes().addAll(mergeAttributes(sourceAttrs, targetAttrs));
        } else if (objectType.equals("File") || objectType.equals("Step")) {
            
            NodeChild source = (NodeChild)pair.getSource();
            NodeChild target = (NodeChild)pair.getTarget();
            
            Map<String,String> sourceAttrs = new LinkedHashMap<>();
            source?.children().each{p -> sourceAttrs.put(p.name(),p.text())};
            
            Map<String,String> targetAttrs = new LinkedHashMap<>();
            target?.children().each{p -> targetAttrs.put(p.name(),p.text())};
            
            pair.getAttributes().addAll(mergeAttributes(sourceAttrs, targetAttrs));
        } else if (objectType.equals("Schedule")) {
            
            NodeChild source = (NodeChild)pair.getSource();
            NodeChild target = (NodeChild)pair.getTarget();
            
            Map<String,String> sourceAttrs = Collections.emptyMap();
            if (source != null) {
                sourceAttrs = new LinkedHashMap<>(4);
                sourceAttrs.put("Enabled", source.enabled.text());
                sourceAttrs.put("Summary", SQLServerDialect.getScheduleSummary(source));
            }
            
            Map<String,String> targetAttrs = Collections.emptyMap();
            if (target != null) {
                targetAttrs = new LinkedHashMap<>(4);
                targetAttrs.put("Enabled", target.enabled.text());
                targetAttrs.put("Summary", SQLServerDialect.getScheduleSummary(target));
            }
            pair.getAttributes().addAll(mergeAttributes(sourceAttrs, targetAttrs));
        } else {
            throw new SyncException("Unexpected object type "+ objectType);
        }
    }

    private String getServerInfo(JdbcDialect dialect) {
        
String query = """
select
(
select (
    select 
        name,
        value
    from 
        sys.configurations
    order by 
        name
    for xml path('Config'), type
) as Configuration,
(
    select
        serverproperty('BuildClrVersion') as BuildClrVersion,
        serverproperty('Collation') as Collation,
        serverproperty('CollationID') as CollationID,
        serverproperty('ComparisonStyle') as ComparisonStyle,
        serverproperty('ComputerNamePhysicalNetBIOS') as ComputerNamePhysicalNetBIOS,
        serverproperty('Edition') as Edition,
        serverproperty('EditionID') as EditionID,
        serverproperty('EngineEdition') as EngineEdition,
        serverproperty('HadrManagerStatus') as HadrManagerStatus,
        serverproperty('InstanceDefaultDataPath') as InstanceDefaultDataPath,
        serverproperty('InstanceDefaultLogPath') as InstanceDefaultLogPath,
        serverproperty('InstanceName') as InstanceName,
        serverproperty('IsAdvancedAnalyticsInstalled') as IsAdvancedAnalyticsInstalled,
        serverproperty('IsClustered') as IsClustered,
        serverproperty('IsFullTextInstalled') as IsFullTextInstalled,
        serverproperty('IsHadrEnabled') as IsHadrEnabled,
        serverproperty('IsIntegratedSecurityOnly') as IsIntegratedSecurityOnly,
        serverproperty('IsLocalDB') as IsLocalDB,
        serverproperty('IsPolybaseInstalled') as IsPolybaseInstalled,
        serverproperty('IsSingleUser') as IsSingleUser,
        serverproperty('IsXTPSupported') as IsXTPSupported,
        serverproperty('LCID') as LCID,
        serverproperty('LicenseType') as LicenseType,
        serverproperty('MachineName') as MachineName,
        serverproperty('NumLicenses') as NumLicenses,
        -- serverproperTY('ProcessID') as ProcessID,
        serverproperty('ProductBuild') as ProductBuild,
        serverproperty('ProductBuildType') as ProductBuildType,
        serverproperty('ProductLevel') as ProductLevel,
        serverproperty('ProductMajorVersion') as ProductMajorVersion,
        serverproperty('ProductMinorVersion') as ProductMinorVersion,
        serverproperty('ProductUpdateLevel') as ProductUpdateLevel,
        serverproperty('ProductUpdateReference') as ProductUpdateReference,
        serverproperty('ProductVersion') as ProductVersion,
        serverproperty('ResourceLastUpdateDateTime') as ResourceLastUpdateDateTime,
        serverproperty('ResourceVersion') as ResourceVersion,
        serverproperty('ServerName') as ServerName,
        serverproperty('SqlCharSet') as SqlCharSet,
        serverproperty('SqlCharSetName') as SqlCharSetName,
        serverproperty('SqlSortOrder') as SqlSortOrder,
        serverproperty('SqlSortOrderName') as SqlSortOrderName,
        serverproperty('FilestreamShareName') as FilestreamShareName,
        serverproperty('FilestreamConfiguredLevel') as FilestreamConfiguredLevel,
        serverproperty('FilestreamEffectiveLevel') as FilestreamEffectiveLevel
    for xml path('Properties'), type
)
as Properties,
@@VERSION as Version,
(
    select 
        cpu_count, 
        hyperthread_ratio,
        -- physical_memory_kb,
        -- virtual_memory_kb,
        max_workers_count,
        scheduler_count 
    from 
        sys.dm_os_sys_info
    for xml path('SysInfo'), type
) as SysInfo,
(
    select 
    -- SecurityObject.Source = 'SqlServer' ( constant )
    -- SecurityObject.ServerName = 'ConnectionName' from DBMaster
    -- SecurityObject.DatabaseName = null
    p.name, -- SecurityObject.Name
    -- p.sid, -- SecutityObject.Id
    p.type, -- ignore
    p.type_desc, -- SecurityObject.Type
    ~p.is_disabled as enabled, -- SecurityObject.Type
    p.create_date, 
    p.modify_date,
    stuff((
            select N', '+ rp.name from sys.server_role_members rm
            inner join sys.server_principals rp on rp.principal_id = rm.role_principal_id
            where p.principal_id=rm.member_principal_id
            for xml path(''), type
    ).value('text()[1]','nvarchar(max)'),1,2,N'') as ServerRoles, 
    (
        select 
            sp.class,
            sp.class_desc,
            sp.grantor_principal_id,
            sp.permission_name,
            sp.state_desc,
	    ep.name as EndpointName
        from 
            sys.server_permissions sp
   	    left join sys.endpoints ep on sp.major_id = ep.endpoint_id and sp.class=105
        where p.principal_id=sp.grantee_principal_id
        for xml path('ServerPermission'), type
    ) as ServerPermissions
    from 
        sys.server_principals p
    where 
        p.name not in ('sysadmin','securityadmin','serveradmin','setupadmin','processadmin','diskadmin','dbcreator','bulkadmin') and p.type<>'C'
    for xml path ('ServerPrincipal'), type
) as ServerPrincipals
for xml path('ServerInfo')
) as ServerInfo
"""
// TODO Add exec msdb.dbo.sp_get_sqlagent_properties


        def conn = dialect.getConnection()
        DatabaseMetaData meta = conn.getMetaData()
        String serverInfoXML = "<ServerInfo><Version>SQL 2000 is not supported</Version><Configuration></Configuration><Properties></Properties><SysInfo></SysInfo></ServerInfo>"
        if (meta.getDatabaseMajorVersion() > 8) {  // TODO Add warning that SQL 200 is not supported
           Statement statement = null;
           ResultSet rs = null;
           try {
               statement = conn.createStatement()
               rs = statement.executeQuery(query)
               if (rs.next()) {
                  serverInfoXML = rs.getString("ServerInfo")
               }
           } finally {
               com.branegy.util.IOUtils.closeQuietly(rs);
               com.branegy.util.IOUtils.closeQuietly(statement);
           }
        }
        return serverInfoXML
    }
}

class InventorySyncSession extends SyncSession {
    DbMaster dbm
    InventoryService inventorySrv
    ConnectionService connectionSrv
    Collection<DatabaseConnection> connections;
    Map<String, Database> connectionDbMap;
    
    public InventorySyncSession(DbMaster dbm, Logger logger, Collection<DatabaseConnection> connections) {
        super(new InventoryComparer(logger));
        setNamer(new InventoryNamer());
        this.dbm = dbm
        inventorySrv = dbm.getService(InventoryService.class)
        connectionSrv = dbm.getService(ConnectionService.class)
        this.connections = connections;
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
    
    public void importChanges(SyncPair pair) {
        String objectType = pair.getObjectType();
        if (objectType.equals("Inventory")) {
            pair.getChildren().each {
                importChanges(it)
            }
        } else if (objectType.equals("Server")) {
            DatabaseConnection source = (DatabaseConnection)pair.getSource();
            DatabaseConnection target = (DatabaseConnection)pair.getTarget();
            switch (pair.getChangeType()) {
                case ChangeType.COPIED:
                    throw new RuntimeException("Not implemented change type ${pair.getChangeType()}")
                case ChangeType.NEW:
                case ChangeType.CHANGED:
                case ChangeType.EQUALS:
                    if (pair.isSelected() && pair.getErrorStatus()==SyncPair.ErrorType.NONE) {
                        target.setCustomData("ServerInfo", source.getCustomData("ServerInfo"))
                        target.setCustomData("Last Sync Date", lastSyncDate);
                        connectionSrv.updateConnection(target)
                    }
                    break;
                case ChangeType.DELETED:
                    /*if (pair.isSelected()) {
                        connectionSrv.deleteConnection(source);
                    }*/
                    break;
            }
            pair.getChildren().each { importChanges(it) }
        } else if (objectType.equals("Database")) {
            Database     sourceDB = (Database)pair.getSource();
            DatabaseInfo targetDB = (DatabaseInfo)pair.getTarget();
            switch (pair.getChangeType()) {
                case ChangeType.NEW:
                    String serverName = pair.getParentPair().getSourceName()
                    def db = connectionDbMap.get(serverName + "%" + targetDB.getName())
                    if (db == null){
                        db = new Database()
                        db.setDatabaseName(targetDB.getName())
                        db.setConnectionName(serverName)
                    }
                    for (SyncAttributePair attr : pair.getAttributes()) {
                        if (attr.getAttributeName().startsWith("Property.")) {
                            continue;
                        }
                        if (attr.getChangeType() != SyncAttributePair.AttributeChangeType.EQUALS) {
                            db.setCustomData( attr.getAttributeName(), attr.getTargetValue()  )
                        }
                    }
                    db.setCustomData(Database.DELETED, false);
                    db.setCustomData("DatabaseDefinition", targetDB.getCustomData("DatabaseDefinition"));
                    db.setCustomData("Last Sync Date", lastSyncDate);
                    
                    if (db.isPersisted()) {
                        inventorySrv.updateDatabase(db)
                    } else {
                        inventorySrv.createDatabase(db)
                    }
                    break;
                case ChangeType.CHANGED:
                    for (SyncAttributePair attr : pair.getAttributes()) {
                        if (attr.getAttributeName().startsWith("Property.")) {
                            continue;
                        }
                        if (attr.getChangeType() != SyncAttributePair.AttributeChangeType.EQUALS) {
                            sourceDB.setCustomData( attr.getAttributeName(), attr.getTargetValue()  )
                        }
                    }
                    sourceDB.setCustomData("DatabaseDefinition", targetDB.getCustomData("DatabaseDefinition"));
                    sourceDB.setCustomData("Last Sync Date", lastSyncDate);
                    inventorySrv.updateDatabase(sourceDB);
                    break;
                case ChangeType.DELETED:
                    inventorySrv.deleteDatabase(sourceDB.getId());
                    break;                    
                case ChangeType.COPIED:
                    throw new RuntimeException("Not implemented change type ${pair.getChangeType()}")
                case ChangeType.EQUALS: 
                    sourceDB.setCustomData("Last Sync Date", lastSyncDate);
                    inventorySrv.updateDatabase(sourceDB);
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
                    targetJob.setCustomData("Last Sync Date", lastSyncDate);
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
                    sourceJob.setCustomData("JobDefinition",  targetJob.getCustomData("JobDefinition"))
                    sourceJob.setCustomData("Last Sync Date", lastSyncDate);
                    sourceJob.setCustomData(Database.DELETED, false)
                    inventorySrv.saveJob(sourceJob)
                    break;
                case ChangeType.DELETED:
                    inventorySrv.deleteJob(sourceJob.getId());
                    break;                    
                case ChangeType.COPIED:
                    throw new RuntimeException("Not implemented change type ${pair.getChangeType()}")
                case ChangeType.EQUALS: 
                    sourceJob.setCustomData("Last Sync Date", lastSyncDate);
                    inventorySrv.saveJob(sourceJob)
                    break;
                default:
                    throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
            }
        } else if (objectType.equals("ServerPrincipal")) {
            SecurityObject  sourceSP = (SecurityObject)pair.getSource();
            NodeChild       targetSP = (NodeChild)pair.getTarget();
            switch (pair.getChangeType()) {
                case ChangeType.NEW:
                case ChangeType.CHANGED:
                    String serverName = pair.getParentPair().getSourceName()
                    def so = sourceSP
                    if (so == null) {
                        so = new SecurityObject()
                        so.setServerName(serverName)
                        so.setSource("SqlServer")
		    }
                    so.setSecurityObjectId(targetSP.name.text())
                    so.setCustomData(SecurityObject.TYPE,targetSP.type_desc.text())
                    so.setCustomData(SecurityObject.NAME,targetSP.name.text())
                    so.setCustomData(SecurityObject.ENABLED,targetSP.enabled.text())
                    so.setCustomData("Roles",targetSP?.ServerRoles.text())

                    so.setDeleted(false)
                    so.setCustomData("Definition", groovy.xml.XmlUtil.serialize(targetSP))
                    so.setCustomData("Last Sync Date", lastSyncDate)
                    
                    if (so.isPersisted()) {
                        inventorySrv.saveSecurityObject(so)
                    } else {
                        inventorySrv.createSecurityObject(so)
                    }
                    break;
                case ChangeType.DELETED:
                    inventorySrv.deleteSecurityObject(sourceSP.getId());
                    break;                    
                case ChangeType.COPIED:
                    throw new RuntimeException("Not implemented change type ${pair.getChangeType()}")
                case ChangeType.EQUALS: 
                    sourceSP.setCustomData("Last Sync Date", lastSyncDate);
                    inventorySrv.saveSecurityObject(sourceSP);
                    break;
                default:
                    throw new RuntimeException("Unexpected change type ${pair.getChangeType()}")
            }
        } else {
            throw new SyncException("Unexpected object type "+ objectType);
        }
    }    
}

sync_session = new InventorySyncSession(dbm, logger, connections)

def inventory = new RootObject("Inventory", "Inventory")
sync_session.syncObjects(inventory, inventory)
sync_session.setParameter("title", "Database Synchronization")