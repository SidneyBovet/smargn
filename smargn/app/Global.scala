import filters.CORSFilter
import play.api._
import play.api.mvc.Results._
import play.api.mvc._

import scala.concurrent.Future

/*
 * Contributors:
 *  - Valentin Rutz
 */

/**
 * Created by Valentin on 26/03/15.
 */
object Global extends WithFilters(new CORSFilter) with GlobalSettings {

  override def onError(request: RequestHeader, ex: Throwable): Future[Result] = {
    Future.successful(InternalServerError(views.html.errorPage(ex)))
  }

  override def onStop(app: Application) {
  }

  override def onHandlerNotFound(request: RequestHeader): Future[Result] = {
    Future.successful(NotFound(views.html.notFoundPage(request.path)))
  }
}
