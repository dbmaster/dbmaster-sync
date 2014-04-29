/*
 *  File Version:  $Id: sync-history-details.groovy 145 2013-05-22 18:10:44Z schristin $
 */

import com.branegy.dbmaster.sync.api.*
import io.dbmaster.sync.*

logger.info("Load session")

SyncService syncService = dbm.getService(SyncService.class)

def session = syncService.findSessionById(Integer.parseInt(p_session_id), false)

logger.info("Start generation")

println "<h1>Synchronization session: ${p_title}</h1>"

if (session.getParameter("html")==null) {
    session = syncService.findSessionById(Integer.parseInt(p_session_id), true)
    pg = new io.dbmaster.sync.PreviewGenerator()
    println "${pg.generatePreview(session)}"
} else {
    println session.getParameter("html")
    logger.info("Use html value")
}

logger.info("Done")
