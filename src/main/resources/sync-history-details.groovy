import com.branegy.dbmaster.sync.api.*
import io.dbmaster.sync.*

logger.info("Load session")

SyncService syncService = dbm.getService(SyncService.class)

def session = syncService.findSessionById(Long.parseLong(p_session_id), false)

logger.info("Start generation")

println "<h1>Synchronization session: ${p_title}</h1>"

if (session.getParameter("html")==null) {
    session = syncService.findSessionById(Long.parseLong(p_session_id), true)
    println syncService.generateSyncSessionPreviewHtml(session, false)
} else {
    println session.getParameter("html")
    logger.info("Use html value")
}

logger.info("Done")
