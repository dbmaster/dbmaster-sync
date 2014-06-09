import com.branegy.dbmaster.sync.api.*;
import com.branegy.service.core.QueryRequest;
import com.branegy.service.base.api.UserService;

logger.info("Load sessions")

SyncService syncService = dbm.getService(SyncService.class);
UserService userService = dbm.getService(UserService.class);
def sessions = syncService.getSessions(new QueryRequest(), false)

logger.info("Start generation")

def linkHelper = tool_linker.all().set("id","sync-history-details")

println """<table class="simple-table" cellspacing="0">
            <tr><th>Session</th><th>Type</th><th>Date</th><th>User</th></tr>"""

logger.info("Started report generation")

int i = 0;

for (SyncSession session: sessions) {
    println "<tr>"

    def title = session.getParameter("title") ?: "no title"

    linkHelper.set("p_session_id",  (session.getParameter("id")?:"no id")).set("p_title", title)
    println "<td>"+linkHelper.render(title)+"</td>"

    println "<td>${session.getParameter("type")}</td>"
    // TODO Get rid of format - locale should be changed
    println "<td>${session.getParameter("date").format('MM/dd/yyyy HH:mm a')}</td>"

    try {
        def user  = userService.findUserByName(session.getParameter("author"))
        println "<td>${user.getFirstName()} ${user.getLastName()}</td>"
    } catch (Exception e) {
        println "<td>${session.getParameter("author")}</td>"
    }
    println "</tr>"
}
println "</table>"
logger.info("Done")
