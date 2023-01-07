package com.finaly.spark

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import spray.json._

import scala.io.StdIn
object lastapi {
  def main(args: Array[String]): Unit = {




    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher
    case class DataObject(date: String, word: String, latLng: String, freq: Int)

    object DataObjectJsonProtocol extends DefaultJsonProtocol {
      implicit val dataObjectFormat = jsonFormat4(DataObject)
    }

    import DataObjectJsonProtocol._

    val dataobj = Array(
      DataObject("Sep-14-2013", "http", "-77.029;38.8991", 4),
      DataObject("Sep-14-2013", "States", "-77.029;38.8991", 4),
      DataObject("Sep-14-2013", "mleEmY", "-77.029;38.8991", 4),
      DataObject("Sep-14-2013", "Friday", "-77.029;38.8991", 4),
      DataObject("Sep-14-2013", "United", "-77.029;38.8991", 4),
      DataObject( "Sep-13-2013", "Flatiron","-105.21330888;40.015358",2),
      DataObject( "Sep-13-2013" ,"boulder","-105.21330888;40.015358",2),
      DataObject("Sep-13-2013","Subaru" , "-105.21330888;40.015358",2),
      DataObject( "Sep-13-2013","http", "-105.21330888;40.015358",2),
      DataObject("Sep-13-2013","flood ", "-105.21330888;40.015358",2)
    )




    val jsonString = dataobj.toJson.compactPrint
    //let dataobj =[{ "data": "Sep-14-2013" ,"word": "http" ,"cord": "-77.029;38.8991","feq":4 },{"data": "Sep-14-2013" ,"word": "States" ,"cord": "-77.029;38.8991","feq":4 }
    // ,{"data": "Sep-14-2013" ,"word": "mleEmY" ,"cord": "-77.029;38.8991","feq":4 },{"data": "Sep-14-2013" ,"word": "Friday" ,"cord": "-77.029;38.8991","feq":4},{"data": "Sep-14-2013" ,"word": "United" ,"cord": "-77.029;38.8991","feq":4}]
    val route =
    path("data") {
      get {

        complete(HttpEntity(ContentTypes.`application/json`, s"""{"data": "$jsonString"}""" ))
      }
    }

    val bindingFuture = Http().newServerAt("localhost", 9000).bind(route)

    println(s"Server now online. Please navigate to http://localhost:9000/data\n")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done

  }
}
