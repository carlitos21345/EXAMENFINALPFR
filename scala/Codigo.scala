import cats.effect.{IO, IOApp}
import doobie.*
import doobie.implicits.*
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*

case class Campana(
                    ID: Int,
                    Candidato: String,
                    Partido_Politico: String,
                    Evento: String,
                    Fecha_Evento: String,
                    Ubicacion: String,
                    Asistentes_Estimados: Int,
                    Campaña_Activa: String
                  )

given CsvRowDecoder[Campana, String] = deriveCsvRowDecoder[Campana]

object Codigo extends IOApp.Simple:

  val rutaArchivo = Path("src/main/resources/data/politica.csv")

  val xa = Transactor.fromDriverManager[IO](
    driver = "oracle.jdbc.OracleDriver",
    url = "jdbc:oracle:thin:@//localhost:1521/orcl",
    user = "system",
    password = "ismael2006",
    logHandler = None
  )

  def insertar(c: Campana): ConnectionIO[Int] =
    sql"""
      INSERT INTO CAMPANAS_POLITICAS (
        ID, CANDIDATO, PARTIDO_POLITICO, EVENTO,
        FECHA_EVENTO, UBICACION, ASISTENTES_ESTIMADOS, CAMPANA_ACTIVA
      )
      VALUES (
        ${c.ID}, ${c.Candidato}, ${c.Partido_Politico}, ${c.Evento},
        ${c.Fecha_Evento}, ${c.Ubicacion}, ${c.Asistentes_Estimados}, ${c.Campaña_Activa}
      )
    """.update.run

  val run: IO[Unit] =
    Files[IO].readAll(rutaArchivo)
      .through(fs2.text.utf8.decode)
      .through(decodeUsingHeaders[Campana](','))
      .evalMap { dato =>
        insertar(dato).transact(xa)
      }
      .compile
      .drain
      .flatMap(_ => IO.println("Proceso Exitoso."))