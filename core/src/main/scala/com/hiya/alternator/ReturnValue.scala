package com.hiya.alternator

/** Which state of the item `updateAndReturn` reports back: the state immediately before the update (`Old`, maps to
  * `ReturnValues=ALL_OLD`) or immediately after it (`New`, maps to `ReturnValues=ALL_NEW`). Required with no default on
  * `updateAndReturn` so every call site states its choice explicitly — unlike `put`/`delete`, `update` can produce a
  * "new" value that isn't already known locally (the result of server-side arithmetic/list-append/set union), so the
  * choice actually matters here.
  */
sealed trait ReturnValue
object ReturnValue {
  case object Old extends ReturnValue
  case object New extends ReturnValue
}
