import com.branegy.dbmaster.sync.api.*
import io.dbmaster.sync.*

logger.info("Loading session ${p_session_id}")

SyncService syncService = dbm.getService(SyncService.class)

def session = syncService.findSessionById(p_session_id, false)

println "<h1>Synchronization session: ${p_title}</h1>"

def sessionType = session.getParameter("type")
def preview     = null

if (sessionType.equals("Database Import")) {
  preview = "/db-sync-summary.groovy"
}
if (session.getParameter("html")==null) {
    logger.info("Starting generation")
    session = syncService.findSessionById(p_session_id, true)
    println syncService.generateSyncSessionPreviewHtml(preview, session, p_show_changes_only)
} else {
    logger.info("Using saved pre-generated results")
    println session.getParameter("html")
}

logger.info("Done")