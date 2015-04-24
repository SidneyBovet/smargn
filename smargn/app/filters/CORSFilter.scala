package filters

import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global

/**
 *
 */
class CORSFilter extends EssentialFilter {
  /**
   *
   * @param next
   * @return
   */
  def apply(next: EssentialAction) = {
    new EssentialAction {
      /**
       *
       * @param requestHeader
       * @return
       */
      def apply(requestHeader: RequestHeader) = {
        next(requestHeader).map { result =>
          result.withHeaders("Access-Control-Allow-Origin" -> "*",
            "Access-Control-Expose-Headers" -> "WWW-Authenticate, Server-Authorization",
            "Access-Control-Allow-Methods" -> "POST, GET, OPTIONS, PUT, DELETE",
            "Access-Control-Allow-Headers" -> "x-requested-with,content-type,Cache-Control,Pragma,Date")
        }
      }
    }
  }
}