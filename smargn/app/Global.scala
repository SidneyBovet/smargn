import controllers.Spark
import filters.CORSFilter
import play.api._
import play.api.mvc.Results._
import play.api.mvc._

import scala.concurrent.Future

/**
 * Created by Valentin on 26/03/15.
 */
object Global extends WithFilters(new CORSFilter) with GlobalSettings {

  override def onError(request: RequestHeader, ex: Throwable): Future[Result] = {
    Spark.stop()
    Future.successful(InternalServerError(views.html.errorPage(ex)))
  }

  override def onStop(app: Application) {
    Spark.stop()
  }

  override def onHandlerNotFound(request: RequestHeader): Future[Result] = {
    Future.successful(NotFound(views.html.notFoundPage(request.path)))
  }
}
