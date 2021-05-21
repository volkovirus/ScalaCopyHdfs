object ScalaCopyHdfs extends App {

  import org.apache.hadoop.conf._
  import org.apache.hadoop.fs._
  import java.net.URI

  val config = new Configuration()
  val host = "hdfs://localhost:9000"
  val fileSystem = FileSystem.get(new URI(host), config)

  val pathStageFolder = new Path(host + "/stage/")
  val pathOdsFolder = new Path(host + "/ods/")

  // Регулярное выражение для работы только с каталогами, подходящими под шаблон
  val regexFolders = """^(date=)\d{4}(-)\d{2}(-)\d{2}$""".r

  // Получаем все вложенные каталоги для каталога Stage
  val allStageSubFolders = fileSystem.listStatus(pathStageFolder).filter(_.isDirectory).filter(pathFolder => regexFolders.findFirstIn(pathFolder.getPath.getName.toLowerCase).isDefined).toList

  allStageSubFolders.foreach(subFolder => {
    // Создаеть путь к целевому подкатологу копирования (Ods)
    val pathOdsSubFolder = new Path(pathOdsFolder + "/" + subFolder.getPath.getName.toLowerCase())

    // Если целевого субкаталога нет, то создаём его
    if (!fileSystem.exists(pathOdsSubFolder)) {
      fileSystem.mkdirs(pathOdsSubFolder)
    }

    // Регулярное выражение для работы только с файлами, подходящими под шаблон (так как могут быть файлы в процессе загрузки и т.д.)
    val regexFiles = """^(part-)\d{4}(.csv)$""".r

    // Получаем список файлов для перемещения из подкаталога (Stage)
    val filesStageSubFolder = fileSystem.listStatus(subFolder.getPath).filter(_.isFile).filter(pathStageFile => regexFiles.findFirstIn(pathStageFile.getPath.getName.toLowerCase).isDefined).toList

    // Проходим по каждому файлу и применяем функцию перемещения данных
    filesStageSubFolder.foreach(pathStageFile => replaceDataFromFile(pathStageFile.getPath,pathOdsSubFolder))
  })

  fileSystem.close()




 def replaceDataFromFile(pathFileIn: Path, pathFolderOut: Path):Unit = {
   // Создаём путь до целевого файла
   val pathFileOut = new Path(pathFolderOut + "/part-0000.csv")

   // Признак существования целевого файла (Ods)
   val isOutFileExists = fileSystem.exists(pathFileOut)

   // Целевой файл либо создаём, либо дописываем в него
   val out = if (fileSystem.exists(pathFileOut)) {
     fileSystem.append(pathFileOut)
   } else {
     fileSystem.create(pathFileOut)
   }

   val in = fileSystem.open(pathFileIn)
   var b = new Array[Byte](1024)
   var numBytes = in.read(b)

   //Если целевой файл part-0000.csv существовал и в исходном файле есть данные, то делаем перенос строки
   if (numBytes > 0 && isOutFileExists) out.writeChars("\n")

   // Далее пишем данные в целевой файл part-0000.csv
   while (numBytes > 0) {
     out.write(b, 0, numBytes)
     numBytes = in.read(b)
   }

   in.close()
   out.close()

   fileSystem.deleteOnExit(pathFileIn)
 }

}
