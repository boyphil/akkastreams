package akkastreams.flows

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

class WishList[TWish, TNotification](wishes: Set[TWish],
                                     grantsWish:(TWish, TNotification)=>Boolean
                                    ) extends GraphStage[FlowShape[TNotification, Map[TWish, Seq[TNotification]]]] {
  val in = Inlet[TWish]("WishList.in")
  val out = Outlet[Map[TWish, Seq[TNotification]]]("WishList.out")

  override def createLogic(inheritedAttributes:Attributes): GraphStageLogic =
    new GraphStageLogic(shape){
      type W = TWish
      type N = TNotification
      var acc = Map.empty[W, Seq[N]]

      setHandler(in, new InHandler {
        override def onPush():Unit = {
          val n = grab(in)

          val grantedWishes: Set[(W, N)] = wishes
            .filter(grantsWish(_, n))
            .map(r => (r -> n))

          acc = merge(acc, grantedWishes)

          if(acc.keySet == wishes) push(out, acc)
          else (pull(in))
        }
      })

      setHandler(out, new OutHandler{
        override def onPull(): Unit = {
          pull(in)
        }
      })

      def merge(m: Map[W, Seq[N]], s: Set[(W, N)]): Map[W, Seq[N]] = {
        s.foldLeft(m) {
          case(a, (r, n)) => {
            val union: Seq[N] = a.getOrElse(r, Seq.empty[N]) :+ n
            a + (r -> union)
          }
        }
      }
    }
}
