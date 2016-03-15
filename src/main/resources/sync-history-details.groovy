import com.branegy.dbmaster.sync.api.*
import io.dbmaster.sync.*

logger.info("Loading session ${p_session_id}")

SyncService syncService = dbm.getService(SyncService.class)

def session = syncService.findSessionById(p_session_id, false)

logger.info("Start generation")

println "<h1>Synchronization session: ${p_title}</h1>"

if (session.getParameter("html")==null) {
    session = syncService.findSessionById(p_session_id, true)
    println syncService.generateSyncSessionPreviewHtml(session, p_show_changes_only)
} else {
    println session.getParameter("html")
    logger.info("Use html value")
}

logger.info("Done")
