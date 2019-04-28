
import com.xenomachina.argparser.ArgParser
import org.apache.commons.codec.digest.DigestUtils
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.impl.DSL.*
import org.jooq.impl.SQLDataType
import spark.Request
import spark.Response
import spark.Spark
import spark.Spark.halt
import java.sql.DriverManager
import java.time.LocalDateTime
import java.util.*
import java.time.ZoneId
import java.time.Instant
import org.jooq.impl.DefaultConfiguration
import org.jooq.impl.DefaultExecuteListenerProvider

fun main(args : Array<String>) {
    ArgParser(args).parseInto(::LectureRoomArgParser).run {
        createInitialTablesAndViews()
        when(program) {
            "server" -> {
                //create view if not exists
                val user = table("\"User\"").`as`("u")
                val group= table("\"UserGroup\"").`as`("g")
                val lecture = table("\"Lecture\"").`as`("l")

                Spark.port(8080)
                Spark.get("/meeting") { request, response ->  getMeetingRoom(request, response)}
            }
            "adduser" -> {
                val loginName = if(username.isEmpty()) {
                    println("Please insert a username")
                    Scanner(System.`in`).nextLine()
                } else username
                val pw = if(password.isEmpty()) {
                    println("Please insert a password")
                    Scanner(System.`in`).nextLine()
                } else password

                val roomNumber = if(room.isEmpty()) {
                    println("Please insert a room")
                    Scanner(System.`in`).nextLine()
                } else room
                println("$loginName : $pw : *******")
                JOOQ.query {
                    val inserted = it.insertInto(table("\"MeetingRoomUser\""))
                        .columns(
                            field("loginname"),
                            field("room"),
                            field("password")
                        )
                        .values(
                            loginName,
                            roomNumber,
                            DigestUtils.sha512Hex(pw)
                        )
                        .execute()
                    println("Result: $inserted")
                }
            }
            else -> System.exit(-1)
        }
    }
}

fun createInitialTablesAndViews() {
    JOOQ.query {
        it.createViewIfNotExists("MeetingRoom").`as`(
            select(
                field("start"),
                field("\"end\""),
                field("room"),
                field("uid")
            ).from(
                "\"Lecture\" l " +
                        "JOIN \"User\" u ON u.id = l.user_id " +
                        "JOIN \"UserGroup\" g ON u.group_id = g.id"
            )
                .where(field("userHash").isNotNull.and(field("userHash").notEqual("")))
        ).execute()
    }

    JOOQ.query {
        it.createTableIfNotExists("MeetingRoomUser")
            .column("id", SQLDataType.INTEGER.identity(true))
            .column("loginname", SQLDataType.VARCHAR.length(10))
            .column("room", SQLDataType.VARCHAR.length(50))
            .column("password", SQLDataType.VARCHAR.length(128))
            .constraint(
                constraint("PK_MeetingRoomUser").primaryKey("id")
            )
            .execute()
    }
}

fun getMeetingRoom(req : Request, resp : Response) : Any {
    val auth = req.getBasicAuth()
    if(auth == null || auth.username.isEmpty() || auth.password.isEmpty()) {
        resp.header("WWW-Authenticate", "Basic realm=\"Bitte Anmelden\"")
        return halt(401, "Unauthorized")
    }
    val user = JOOQ.query {
        it.select()
            .from(table("\"MeetingRoomUser\""))
            .where(field("loginname").eq(auth.username))
            .limit(1)
            .fetchSingle()
    }
    val pwHash = DigestUtils.sha512Hex(auth.password)
    if(user == null || user.get("password") != pwHash) {
        resp.header("WWW-Authenticate", "Basic realm=\"Bitte Anmelden\"")
        return halt(401, "Unauthorized")
    }

    val nowDate = LocalDateTime.of(2019, 4, 29, 15, 30)
    val startAtDay = nowDate.withHour(0).withMinute(0)
    val endOfDay = nowDate.withHour(23).withMinute(59)
    val records = JOOQ.query {
        val records = it.select()
            .from("\"MeetingRoom\"")
            .where(
                field("start").between(startAtDay.millis() / 1000, endOfDay.millis() / 1000).and(
                    field("room").eq(user.get("room"))
                )
            )
            .orderBy(field("start").asc())
            .fetch()
            .distinct()
        records
    }
    println(records?.size)
    val nowRecord = records?.find {
        val start = it.get<Long>("start") ?: 0
        val end = it.get<Long>("end") ?: 0
        nowDate.millis() / 1000 in start..end
    }
    val nextRecord = records?.find {
        val start = it.get<Long>("start") ?: 0
        start > nowDate.millis() / 1000

    }
    resp.type("application/json")
    val nowLecture = nowRecord?.let {
        val range = it.get<Long>("start").times(1000).toLocalDateTime()..it.get<Long>("end").times(1000).toLocalDateTime()
        range.toClockPattern() + "- ${it.get("uid")}"
    }
    val nextLecture = nextRecord?.let {
        val range = it.get<Long>("start").times(1000).toLocalDateTime()..it.get<Long>("end").times(1000).toLocalDateTime()
        range.toClockPattern() + "- ${it.get("uid")}"
    }
    return "{ \"now\" : \"$nowLecture\", \"next\" : \"$nextLecture\"}"
}

data class Authentication(
    val username : String,
    val password : String
)
fun Request.getBasicAuth() : Authentication? {
    val auth = this.headers("Authorization") ?: return null
    return if(auth.startsWith("Basic")) {
        val encoded = String(Base64.getDecoder().decode(auth.replace("Basic", "").trim().toByteArray()))
        val username = encoded.split(":")[0]
        val password = encoded.split(":")[1]
        return Authentication(username, password)
    } else null
}

object JOOQ {
    val userName = "dbservice"
    val password = "dbservice"
    val url = "jdbc:postgresql://localhost:5555/dbservice"
    fun <T> query(lambda : (DSLContext) -> T) : T? {
        try {
            DriverManager.getConnection(url, userName, password).use { conn ->
                val configuration = DefaultConfiguration().set(conn).set(SQLDialect.POSTGRES_10)
                configuration.set(DefaultExecuteListenerProvider(PrettyPrinter()))

                // Create a DSLContext from the above configuration
                val context = DSL.using(configuration)
                return lambda(context)
            }
        } catch (e: Exception) {
            throw e
        }
    }
}

fun LocalDateTime.millis() : Long {
    return this.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
}

fun Long.toLocalDateTime() : LocalDateTime {
    return Instant.ofEpochMilli(this).atZone(ZoneId.systemDefault()).toLocalDateTime()
}

inline fun <reified T> Record.get(col: String) : T {
    return this.get(col, T::class.java)
}

fun ClosedRange<LocalDateTime>.toClockPattern() : String {
    return "%02d:%02d-%02d:%02d".format(this.start.hour, this.start.minute, this.endInclusive.hour, this.endInclusive.minute)
}