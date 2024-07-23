package com.hiya.alternator.alpakka

import com.hiya.alternator.Table

package object stream {
  implicit def toStreamOps[V, PK](table: Table[V, PK]): AlpakkaStreamOps[V, PK] =
    new AlpakkaStreamOps[V, PK](table)

}
