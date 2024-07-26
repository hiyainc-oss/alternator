package com.hiya

import cats.free.Free

package object alternator {
  type DynamoDB[A] = Free[internal.DynamoDBA, A]
  type DynamoDBSource[A] = Free[internal.DynamoDBSourceA, A]
}
