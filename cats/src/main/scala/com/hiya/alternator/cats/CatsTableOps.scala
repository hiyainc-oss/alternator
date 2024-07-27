package com.hiya.alternator.cats

import fs2._
import com.hiya.alternator.crud
import com.hiya.alternator.crud.TableWithRangeOps

trait CatsTableOps[S[_], V, PK] extends crud.TableOps[V, PK, S, Stream[S, *]]

trait CatsTableOpsWithRange[S[_], V, PK, RK]
  extends TableWithRangeOps[V, PK, RK, S, Stream[S, *]]
  with CatsTableOps[S, V, (PK, RK)]
