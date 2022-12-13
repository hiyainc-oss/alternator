package com.hiya.alternator

import akka.actor.typed.ActorRef
import com.hiya.alternator.alpakka.internal.ShutdownExts

package object alpakka {
  implicit def shutdownExts[T](actorRef: ActorRef[T])(implicit T: ShutdownExts.Support[T]): ShutdownExts[T] = T(actorRef)

}
