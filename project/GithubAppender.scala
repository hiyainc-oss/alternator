package sbt.internal.util

import sbt.{ConsoleOut => _, _}
import sbt.protocol.testing.TestItemEvent
import sbt.testing.Status
import sbt.util.Level
import xsbti.Severity

class GithubAppender(baseDir: File)
  extends ConsoleAppender(
    s"github",
    ConsoleAppender.Properties.from(ConsoleOut.globalProxy, false, false),
    _ => None
  ) {
  private val levelMapping = Map(
    Severity.Warn -> "warning", // https://help.github.com/en/actions/reference/workflow-commands-for-github-actions#setting-a-warning-message
    Severity.Error -> "error", // https://help.github.com/en/actions/reference/workflow-commands-for-github-actions#setting-an-error-message
    Severity.Info -> "debug" // there is no info level
  )

  override private[sbt] def appendObjectEvent[T](level: Level.Value, obj: => ObjectEvent[T]): Unit = {
    obj.message match {
      case problem: xsbti.Problem =>
        for {
          absoluteFilePath <- problem.position().sourceFile().asScala
          relativeFile <- absoluteFilePath.relativeTo(baseDir)
          line = problem.position().line().asScala.getOrElse(1)
        } {
          val ghActionsCmd = levelMapping(problem.severity())
          out.println(s"::$ghActionsCmd file=$relativeFile,line=$line::${problem.message().takeWhile(_ != '\n')}")
        }

      case testEvent: TestItemEvent =>
        if (testEvent.result.exists(r => Set[TestResult](TestResult.Failed, TestResult.Error).contains(r))) {
          testEvent.detail.foreach(detail =>
            if (Set(Status.Error, Status.Failure).contains(detail.status)) {
              out.println(s"::error::Failing test case: ${detail.fullyQualifiedName}")
            }
          )
        }

      case _ =>
    }
  }

  override def appendLog(level: Level.Value, message: => String): Unit = {}
}
