import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default

class LectureRoomArgParser(parser: ArgParser) {
    val program by parser.positional("program")
    val username by parser.storing("-u", "--username", help = "name of the user").default("")
    val password by parser.storing("-p", "--password", help = "password of the user").default("")
    val room by parser.storing("-r", "--room", help = "room of the user").default("")
}